#
# Copyright 2021 VMware, Inc.
# Portions Copyright 2023 Timescale, Inc.
# SPDX-License-Identifier: PostgreSQL
#

import base64
import collections
import ctypes
import http.server
import json
import logging
import os
import platform
import secrets
import sys
import threading
import time
import traceback
import types
import urllib.parse
from numbers import Number

import psycopg2
import pytest

import pq3

from .conftest import BLOCKING_TIMEOUT

# The client tests need libpq to have been compiled with OAuth support; skip
# them otherwise.
pytestmark = pytest.mark.skipif(
    os.getenv("with_oauth") == "none",
    reason="OAuth client tests require --with-oauth support",
)

if platform.system() == "Darwin":
    libpq = ctypes.cdll.LoadLibrary("libpq.5.dylib")
else:
    libpq = ctypes.cdll.LoadLibrary("libpq.so.5")


def finish_handshake(conn):
    """
    Sends the AuthenticationOK message and the standard opening salvo of server
    messages, then asserts that the client immediately sends a Terminate message
    to close the connection cleanly.
    """
    pq3.send(conn, pq3.types.AuthnRequest, type=pq3.authn.OK)
    pq3.send(conn, pq3.types.ParameterStatus, name=b"client_encoding", value=b"UTF-8")
    pq3.send(conn, pq3.types.ParameterStatus, name=b"DateStyle", value=b"ISO, MDY")
    pq3.send(conn, pq3.types.ReadyForQuery, status=b"I")

    pkt = pq3.recv1(conn)
    assert pkt.type == pq3.types.Terminate


#
# OAUTHBEARER (see RFC 7628: https://tools.ietf.org/html/rfc7628)
#


def start_oauth_handshake(conn):
    """
    Negotiates an OAUTHBEARER SASL challenge. Returns the client's initial
    response data.
    """
    startup = pq3.recv1(conn, cls=pq3.Startup)
    assert startup.proto == pq3.protocol(3, 0)

    pq3.send(
        conn, pq3.types.AuthnRequest, type=pq3.authn.SASL, body=[b"OAUTHBEARER", b""]
    )

    pkt = pq3.recv1(conn)
    assert pkt.type == pq3.types.PasswordMessage

    initial = pq3.SASLInitialResponse.parse(pkt.payload)
    assert initial.name == b"OAUTHBEARER"

    return initial.data


def get_auth_value(initial):
    """
    Finds the auth value (e.g. "Bearer somedata..." in the client's initial SASL
    response.
    """
    kvpairs = initial.split(b"\x01")
    assert kvpairs[0] == b"n,,"  # no channel binding or authzid
    assert kvpairs[2] == b""  # ends with an empty kvpair
    assert kvpairs[3] == b""  # ...and there's nothing after it
    assert len(kvpairs) == 4

    key, value = kvpairs[1].split(b"=", 2)
    assert key == b"auth"

    return value


def xtest_oauth_success(conn):  # TODO
    initial = start_oauth_handshake(conn)

    auth = get_auth_value(initial)
    assert auth.startswith(b"Bearer ")

    # Accept the token. TODO actually validate
    pq3.send(conn, pq3.types.AuthnRequest, type=pq3.authn.SASLFinal)
    finish_handshake(conn)


class RawResponse(str):
    """
    Returned by registered endpoint callbacks to take full control of the
    response. Usually, return values are converted to JSON; a RawResponse body
    will be passed to the client as-is, allowing endpoint implementations to
    issue invalid JSON.
    """

    pass


class OpenIDProvider(threading.Thread):
    """
    A thread that runs a mock OpenID provider server.
    """

    def __init__(self, *, port):
        super().__init__()

        self.exception = None

        addr = ("", port)
        self.server = self._Server(addr, self._Handler)

        # TODO: allow HTTPS only, somehow
        oauth = self._OAuthState()
        oauth.host = f"localhost:{port}"
        oauth.issuer = f"http://localhost:{port}"

        # The following endpoints are required to be advertised by providers,
        # even though our chosen client implementation does not actually make
        # use of them.
        oauth.register_endpoint(
            "authorization_endpoint", "POST", "/authorize", self._authorization_handler
        )
        oauth.register_endpoint("jwks_uri", "GET", "/keys", self._jwks_handler)

        self.server.oauth = oauth

    def run(self):
        try:
            # XXX socketserver.serve_forever() has a serious architectural
            # issue: its select loop wakes up every `poll_interval` seconds to
            # see if the server is shutting down. The default, 500 ms, only lets
            # us run two tests every second. But the faster we go, the more CPU
            # we burn unnecessarily...
            self.server.serve_forever(poll_interval=0.01)
        except Exception as e:
            self.exception = e

    def stop(self, timeout=BLOCKING_TIMEOUT):
        """
        Shuts down the server and joins its thread. Raises an exception if the
        thread could not be joined, or if it threw an exception itself. Must
        only be called once, after start().
        """
        self.server.shutdown()
        self.join(timeout)

        if self.is_alive():
            raise TimeoutError("client thread did not handshake within the timeout")
        elif self.exception:
            e = self.exception
            raise e

    class _OAuthState(object):
        def __init__(self):
            self.endpoint_paths = {}
            self._endpoints = {}

            # Provide a standard discovery document by default; tests can
            # override it.
            self.register_endpoint(
                None,
                "GET",
                "/.well-known/openid-configuration",
                self._default_discovery_handler,
            )

        def register_endpoint(self, name, method, path, func):
            if method not in self._endpoints:
                self._endpoints[method] = {}

            self._endpoints[method][path] = func

            if name is not None:
                self.endpoint_paths[name] = path

        def endpoint(self, method, path):
            if method not in self._endpoints:
                return None

            return self._endpoints[method].get(path)

        def _default_discovery_handler(self, headers, params):
            doc = {
                "issuer": self.issuer,
                "response_types_supported": ["token"],
                "subject_types_supported": ["public"],
                "id_token_signing_alg_values_supported": ["RS256"],
                "grant_types_supported": [
                    "urn:ietf:params:oauth:grant-type:device_code"
                ],
            }

            for name, path in self.endpoint_paths.items():
                doc[name] = self.issuer + path

            return 200, doc

    class _Server(http.server.HTTPServer):
        def handle_error(self, request, addr):
            self.shutdown_request(request)
            raise

    @staticmethod
    def _jwks_handler(headers, params):
        return 200, {"keys": []}

    @staticmethod
    def _authorization_handler(headers, params):
        # We don't actually want this to be called during these tests -- we
        # should be using the device authorization endpoint instead.
        assert (
            False
        ), "authorization handler called instead of device authorization handler"

    class _Handler(http.server.BaseHTTPRequestHandler):
        timeout = BLOCKING_TIMEOUT

        def _handle(self, *, params=None, handler=None):
            oauth = self.server.oauth
            assert self.headers["Host"] == oauth.host

            if handler is None:
                handler = oauth.endpoint(self.command, self.path)
                assert (
                    handler is not None
                ), f"no registered endpoint for {self.command} {self.path}"

            result = handler(self.headers, params)

            if len(result) == 2:
                headers = {"Content-Type": "application/json"}
                code, resp = result
            else:
                code, headers, resp = result

            self.send_response(code)
            for h, v in headers.items():
                self.send_header(h, v)
            self.end_headers()

            if resp is not None:
                if not isinstance(resp, RawResponse):
                    resp = json.dumps(resp)
                resp = resp.encode("utf-8")
                self.wfile.write(resp)

            self.close_connection = True

        def do_GET(self):
            self._handle()

        def _request_body(self):
            length = self.headers["Content-Length"]

            # Handle only an explicit content-length.
            assert length is not None
            length = int(length)

            return self.rfile.read(length).decode("utf-8")

        def do_POST(self):
            assert self.headers["Content-Type"] == "application/x-www-form-urlencoded"

            body = self._request_body()
            params = urllib.parse.parse_qs(body)

            self._handle(params=params)


@pytest.fixture
def openid_provider(unused_tcp_port_factory):
    """
    A fixture that returns the OAuth state of a running OpenID provider server. The
    server will be stopped when the fixture is torn down.
    """
    thread = OpenIDProvider(port=unused_tcp_port_factory())
    thread.start()

    try:
        yield thread.server.oauth
    finally:
        thread.stop()


#
# PQAuthDataHook implementation, matching libpq.h
#


PQAUTHDATA_PROMPT_OAUTH_DEVICE = 0
PQAUTHDATA_OAUTH_BEARER_TOKEN = 1

PGRES_POLLING_FAILED = 0
PGRES_POLLING_READING = 1
PGRES_POLLING_WRITING = 2
PGRES_POLLING_OK = 3


class PQPromptOAuthDevice(ctypes.Structure):
    _fields_ = [
        ("verification_uri", ctypes.c_char_p),
        ("user_code", ctypes.c_char_p),
    ]


class PQOAuthBearerRequest(ctypes.Structure):
    pass


PQOAuthBearerRequest._fields_ = [
    ("openid_configuration", ctypes.c_char_p),
    ("scope", ctypes.c_char_p),
    (
        "async_",
        ctypes.CFUNCTYPE(
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.POINTER(PQOAuthBearerRequest),
            ctypes.POINTER(ctypes.c_int),
        ),
    ),
    (
        "cleanup",
        ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(PQOAuthBearerRequest)),
    ),
    ("token", ctypes.c_char_p),
    ("user", ctypes.c_void_p),
]


@pytest.fixture
def auth_data_cb():
    """
    Tracks calls to the libpq authdata hook. The yielded object contains a calls
    member that records the data sent to the hook. If a test needs to perform
    custom actions during a call, it can set the yielded object's impl callback;
    beware that the callback takes place on a different thread.

    This is done differently from the other callback implementations on purpose.
    For the others, we can declare test-specific callbacks and have them perform
    direct assertions on the data they receive. But that won't work for a C
    callback, because there's no way for us to bubble up the assertion through
    libpq. Instead, this mock-style approach is taken, where we just record the
    calls and let the test examine them later.
    """

    class _Call:
        pass

    class _cb(object):
        def __init__(self):
            self.calls = []

    cb = _cb()
    cb.impl = None

    # The callback will occur on a different thread, so protect the cb object.
    cb_lock = threading.Lock()

    @ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_byte, ctypes.c_void_p, ctypes.c_void_p)
    def auth_data_cb(typ, pgconn, data):
        handle_by_default = 0  # does an implementation have to be provided?

        if typ == PQAUTHDATA_PROMPT_OAUTH_DEVICE:
            cls = PQPromptOAuthDevice
            handle_by_default = 1
        elif typ == PQAUTHDATA_OAUTH_BEARER_TOKEN:
            cls = PQOAuthBearerRequest
        else:
            return 0

        call = _Call()
        call.type = typ

        # The lifetime of the underlying data being pointed to doesn't
        # necessarily match the lifetime of the Python object, so we can't
        # reference a Structure's fields after returning. Explicitly copy the
        # contents over, field by field.
        data = ctypes.cast(data, ctypes.POINTER(cls))
        for name, _ in cls._fields_:
            setattr(call, name, getattr(data.contents, name))

        with cb_lock:
            cb.calls.append(call)

        if cb.impl:
            # Pass control back to the test.
            try:
                return cb.impl(typ, pgconn, data.contents)
            except Exception:
                # This can't escape into the C stack, but we can fail the flow
                # and hope the traceback gives us enough detail.
                logging.error(
                    "Exception during authdata hook callback:\n"
                    + traceback.format_exc()
                )
                return -1

        return handle_by_default

    libpq.PQsetAuthDataHook(auth_data_cb)
    try:
        yield cb
    finally:
        # The callback is about to go out of scope, so make sure libpq is
        # disconnected from it. (We wouldn't want to accidentally influence
        # later tests anyway.)
        libpq.PQsetAuthDataHook(None)


@pytest.mark.parametrize("secret", [None, "", "hunter2"])
@pytest.mark.parametrize("scope", [None, "", "openid email"])
@pytest.mark.parametrize("retries", [0, 1])
@pytest.mark.parametrize(
    "asynchronous",
    [
        pytest.param(False, id="synchronous"),
        pytest.param(True, id="asynchronous"),
    ],
)
def test_oauth_with_explicit_issuer(
    accept, openid_provider, asynchronous, retries, scope, secret, auth_data_cb
):
    client_id = secrets.token_hex()

    sock, client = accept(
        oauth_issuer=openid_provider.issuer,
        oauth_client_id=client_id,
        oauth_client_secret=secret,
        oauth_scope=scope,
        async_=asynchronous,
    )

    device_code = secrets.token_hex()
    user_code = f"{secrets.token_hex(2)}-{secrets.token_hex(2)}"
    verification_url = "https://example.com/device"

    access_token = secrets.token_urlsafe()

    def check_client_authn(headers, params):
        if not secret:
            assert params["client_id"] == [client_id]
            return

        # Require the client to use Basic authn; request-body credentials are
        # NOT RECOMMENDED (RFC 6749, Sec. 2.3.1).
        assert "Authorization" in headers

        method, creds = headers["Authorization"].split()
        assert method == "Basic"

        expected = f"{client_id}:{secret}"
        assert base64.b64decode(creds) == expected.encode("ascii")

    # Set up our provider callbacks.
    # NOTE that these callbacks will be called on a background thread. Don't do
    # any unprotected state mutation here.

    def authorization_endpoint(headers, params):
        check_client_authn(headers, params)

        if scope:
            assert params["scope"] == [scope]
        else:
            assert "scope" not in params

        resp = {
            "device_code": device_code,
            "user_code": user_code,
            "interval": 0,
            "verification_uri": verification_url,
            "expires_in": 5,
        }

        return 200, resp

    openid_provider.register_endpoint(
        "device_authorization_endpoint", "POST", "/device", authorization_endpoint
    )

    attempts = 0
    retry_lock = threading.Lock()

    def token_endpoint(headers, params):
        check_client_authn(headers, params)

        assert params["grant_type"] == ["urn:ietf:params:oauth:grant-type:device_code"]
        assert params["device_code"] == [device_code]

        now = time.monotonic()

        with retry_lock:
            nonlocal attempts

            # If the test wants to force the client to retry, return an
            # authorization_pending response and decrement the retry count.
            if attempts < retries:
                attempts += 1
                return 400, {"error": "authorization_pending"}

        # Successfully finish the request by sending the access bearer token.
        resp = {
            "access_token": access_token,
            "token_type": "bearer",
        }

        return 200, resp

    openid_provider.register_endpoint(
        "token_endpoint", "POST", "/token", token_endpoint
    )

    with sock:
        with pq3.wrap(sock, debug_stream=sys.stdout) as conn:
            # Initiate a handshake, which should result in the above endpoints
            # being called.
            initial = start_oauth_handshake(conn)

            # Validate and accept the token.
            auth = get_auth_value(initial)
            assert auth == f"Bearer {access_token}".encode("ascii")

            pq3.send(conn, pq3.types.AuthnRequest, type=pq3.authn.SASLFinal)
            finish_handshake(conn)

    if retries:
        # Finally, make sure that the client prompted the user once with the
        # expected authorization URL and user code.
        assert len(auth_data_cb.calls) == 2

        # First call should have been for a custom flow, which we ignored.
        assert auth_data_cb.calls[0].type == PQAUTHDATA_OAUTH_BEARER_TOKEN

        # Second call is for our user prompt.
        call = auth_data_cb.calls[1]
        assert call.type == PQAUTHDATA_PROMPT_OAUTH_DEVICE
        assert call.verification_uri.decode() == verification_url
        assert call.user_code.decode() == user_code


def expect_disconnected_handshake(sock):
    """
    Helper for any tests that expect the client to disconnect immediately after
    being sent the OAUTHBEARER SASL method. Generally speaking, this requires
    the client to have an oauth_issuer set so that it doesn't try to go through
    discovery.
    """
    with sock:
        with pq3.wrap(sock, debug_stream=sys.stdout) as conn:
            # Initiate a handshake.
            startup = pq3.recv1(conn, cls=pq3.Startup)
            assert startup.proto == pq3.protocol(3, 0)

            pq3.send(
                conn,
                pq3.types.AuthnRequest,
                type=pq3.authn.SASL,
                body=[b"OAUTHBEARER", b""],
            )

            # The client should disconnect at this point.
            assert not conn.read()


def test_oauth_requires_client_id(accept, openid_provider):
    sock, client = accept(
        oauth_issuer=openid_provider.issuer,
        # Do not set a client ID; this should cause a client error after the
        # server asks for OAUTHBEARER and the client tries to contact the
        # issuer.
    )

    expect_disconnected_handshake(sock)

    expected_error = "no oauth_client_id is set"
    with pytest.raises(psycopg2.OperationalError, match=expected_error):
        client.check_completed()


@pytest.mark.slow
@pytest.mark.parametrize("error_code", ["authorization_pending", "slow_down"])
@pytest.mark.parametrize("retries", [1, 2])
def test_oauth_retry_interval(accept, openid_provider, retries, error_code):
    sock, client = accept(
        oauth_issuer=openid_provider.issuer,
        oauth_client_id="some-id",
    )

    expected_retry_interval = 1
    access_token = secrets.token_urlsafe()

    # Set up our provider callbacks.
    # NOTE that these callbacks will be called on a background thread. Don't do
    # any unprotected state mutation here.

    def authorization_endpoint(headers, params):
        resp = {
            "device_code": "my-device-code",
            "user_code": "my-user-code",
            "interval": expected_retry_interval,
            "verification_uri": "https://example.com",
            "expires_in": 5,
        }

        return 200, resp

    openid_provider.register_endpoint(
        "device_authorization_endpoint", "POST", "/device", authorization_endpoint
    )

    attempts = 0
    last_retry = None
    retry_lock = threading.Lock()

    def token_endpoint(headers, params):
        now = time.monotonic()

        with retry_lock:
            nonlocal attempts, last_retry, expected_retry_interval

            # Make sure the retry interval is being respected by the client.
            if last_retry is not None:
                interval = now - last_retry
                assert interval >= expected_retry_interval

            last_retry = now

            # If the test wants to force the client to retry, return the desired
            # error response and decrement the retry count.
            if attempts < retries:
                attempts += 1

                # A slow_down code requires the client to additionally increase
                # its interval by five seconds.
                if error_code == "slow_down":
                    expected_retry_interval += 5

                return 400, {"error": error_code}

        # Successfully finish the request by sending the access bearer token.
        resp = {
            "access_token": access_token,
            "token_type": "bearer",
        }

        return 200, resp

    openid_provider.register_endpoint(
        "token_endpoint", "POST", "/token", token_endpoint
    )

    with sock:
        with pq3.wrap(sock, debug_stream=sys.stdout) as conn:
            # Initiate a handshake, which should result in the above endpoints
            # being called.
            initial = start_oauth_handshake(conn)

            # Validate and accept the token.
            auth = get_auth_value(initial)
            assert auth == f"Bearer {access_token}".encode("ascii")

            pq3.send(conn, pq3.types.AuthnRequest, type=pq3.authn.SASLFinal)
            finish_handshake(conn)


@pytest.fixture
def self_pipe():
    """
    Yields a pipe fd pair.
    """

    class _Pipe:
        pass

    p = _Pipe()
    p.readfd, p.writefd = os.pipe()

    try:
        yield p
    finally:
        os.close(p.readfd)
        os.close(p.writefd)


@pytest.mark.parametrize("scope", [None, "", "openid email"])
@pytest.mark.parametrize(
    "retries",
    [
        -1,  # no async callback
        0,  # async callback immediately returns token
        1,  # async callback waits on altsock once
        2,  # async callback waits on altsock twice
    ],
)
@pytest.mark.parametrize(
    "asynchronous",
    [
        pytest.param(False, id="synchronous"),
        pytest.param(True, id="asynchronous"),
    ],
)
def test_user_defined_flow(
    accept, auth_data_cb, self_pipe, scope, retries, asynchronous
):
    issuer = "http://localhost"
    discovery_uri = issuer + "/.well-known/openid-configuration"
    access_token = secrets.token_urlsafe()

    sock, _ = accept(
        oauth_issuer=issuer,
        oauth_client_id="some-id",
        oauth_scope=scope,
        async_=asynchronous,
    )

    # Track callbacks.
    attempts = 0
    wakeup_called = False
    cleanup_calls = 0
    lock = threading.Lock()

    def wakeup():
        """Writes a byte to the wakeup pipe."""
        nonlocal wakeup_called
        with lock:
            wakeup_called = True
            os.write(self_pipe.writefd, b"\0")

    def get_token(pgconn, request, p_altsock):
        """
        Async token callback. While attempts < retries, libpq will be instructed
        to wait on the self_pipe. When attempts == retries, the token will be
        set.

        Note that assertions and exceptions raised here are allowed but not very
        helpful, since they can't bubble through the libpq stack to be collected
        by the test suite. Try not to rely too heavily on them.
        """
        # Make sure libpq passed our user data through.
        assert request.user == 42

        with lock:
            nonlocal attempts, wakeup_called

            if attempts:
                # If we've already started the timer, we shouldn't get a
                # call back before it trips.
                assert wakeup_called, "authdata hook was called before the timer"

                # Drain the wakeup byte.
                os.read(self_pipe.readfd, 1)

            if attempts < retries:
                attempts += 1

                # Wake up the client in a little bit of time.
                wakeup_called = False
                threading.Timer(0.1, wakeup).start()

                # Tell libpq to wait on the other end of the wakeup pipe.
                p_altsock[0] = self_pipe.readfd
                return PGRES_POLLING_READING

        # Done!
        request.token = access_token.encode()
        return PGRES_POLLING_OK

    @ctypes.CFUNCTYPE(
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.POINTER(PQOAuthBearerRequest),
        ctypes.POINTER(ctypes.c_int),
    )
    def get_token_wrapper(pgconn, p_request, p_altsock):
        """
        Translation layer between C and Python for the async callback.
        Assertions and exceptions will be swallowed at the boundary, so make
        sure they don't escape here.
        """
        try:
            return get_token(pgconn, p_request.contents, p_altsock)
        except Exception:
            logging.error("Exception during async callback:\n" + traceback.format_exc())
            return PGRES_POLLING_FAILED

    @ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(PQOAuthBearerRequest))
    def cleanup(pgconn, p_request):
        """
        Should be called exactly once per connection.
        """
        nonlocal cleanup_calls
        with lock:
            cleanup_calls += 1

    def bearer_hook(typ, pgconn, request):
        """
        Implementation of the PQAuthDataHook, which either sets up an async
        callback or returns the token directly, depending on the value of
        retries.

        As above, try not to rely too much on assertions/exceptions here.
        """
        assert typ == PQAUTHDATA_OAUTH_BEARER_TOKEN
        request.cleanup = cleanup

        if retries < 0:
            # Special case: return a token immediately without a callback.
            request.token = access_token.encode()
            return 1

        # Tell libpq to call us back.
        request.async_ = get_token_wrapper
        request.user = ctypes.c_void_p(42)  # will be checked in the callback
        return 1

    auth_data_cb.impl = bearer_hook

    # Now drive the server side.
    with sock:
        with pq3.wrap(sock, debug_stream=sys.stdout) as conn:
            # Initiate a handshake, which should result in our custom callback
            # being invoked to fetch the token.
            initial = start_oauth_handshake(conn)

            # Validate and accept the token.
            auth = get_auth_value(initial)
            assert auth == f"Bearer {access_token}".encode("ascii")

            pq3.send(conn, pq3.types.AuthnRequest, type=pq3.authn.SASLFinal)
            finish_handshake(conn)

    # Check the data provided to the hook.
    assert len(auth_data_cb.calls) == 1

    call = auth_data_cb.calls[0]
    assert call.type == PQAUTHDATA_OAUTH_BEARER_TOKEN
    assert call.openid_configuration.decode() == discovery_uri
    assert call.scope == (None if scope is None else scope.encode())

    # Make sure we cleaned up after ourselves.
    assert cleanup_calls == 1


def alt_patterns(*patterns):
    """
    Just combines multiple alternative regexes into one. It's not very efficient
    but IMO it's easier to read and maintain.
    """
    pat = ""

    for p in patterns:
        if pat:
            pat += "|"
        pat += f"({p})"

    return pat


@pytest.mark.parametrize(
    "failure_mode, error_pattern",
    [
        pytest.param(
            (
                400,
                {
                    "error": "invalid_client",
                    "error_description": "client authentication failed",
                },
            ),
            r"failed to obtain device authorization: client authentication failed \(invalid_client\)",
            id="authentication failure with description",
        ),
        pytest.param(
            (400, {"error": "invalid_request"}),
            r"failed to obtain device authorization: \(invalid_request\)",
            id="invalid request without description",
        ),
        pytest.param(
            (400, {}),
            alt_patterns(
                r'failed to parse token error response: field "error" is missing',
                r"failed to obtain device authorization: \(iddawc error I_ERROR_PARAM\)",
            ),
            id="broken error response",
        ),
        pytest.param(
            (200, RawResponse(r'{ "interval": 3.5.8 }')),
            alt_patterns(
                r"failed to parse device authorization: Token .* is invalid",
                r"failed to obtain device authorization: \(iddawc error I_ERROR\)",
            ),
            id="non-numeric interval",
        ),
        pytest.param(
            (200, RawResponse(r'{ "interval": 08 }')),
            alt_patterns(
                r"failed to parse device authorization: Token .* is invalid",
                r"failed to obtain device authorization: \(iddawc error I_ERROR\)",
            ),
            id="invalid numeric interval",
        ),
    ],
)
def test_oauth_device_authorization_failures(
    accept, openid_provider, failure_mode, error_pattern
):
    client_id = secrets.token_hex()

    sock, client = accept(
        oauth_issuer=openid_provider.issuer,
        oauth_client_id=client_id,
    )

    # Set up our provider callbacks.
    # NOTE that these callbacks will be called on a background thread. Don't do
    # any unprotected state mutation here.

    def authorization_endpoint(headers, params):
        return failure_mode

    openid_provider.register_endpoint(
        "device_authorization_endpoint", "POST", "/device", authorization_endpoint
    )

    def token_endpoint(headers, params):
        assert False, "token endpoint was invoked unexpectedly"

    openid_provider.register_endpoint(
        "token_endpoint", "POST", "/token", token_endpoint
    )

    expect_disconnected_handshake(sock)

    # Now make sure the client correctly failed.
    with pytest.raises(psycopg2.OperationalError, match=error_pattern):
        client.check_completed()


Missing = object()  # sentinel for test_oauth_device_authorization_bad_json()


@pytest.mark.parametrize(
    "bad_value",
    [
        pytest.param({"device_code": 3}, id="object"),
        pytest.param([1, 2, 3], id="array"),
        pytest.param("some string", id="string"),
        pytest.param(4, id="numeric"),
        pytest.param(False, id="boolean"),
        pytest.param(None, id="null"),
        pytest.param(Missing, id="missing"),
    ],
)
@pytest.mark.parametrize(
    "field_name,ok_type,required",
    [
        ("device_code", str, True),
        ("user_code", str, True),
        ("verification_uri", str, True),
        ("interval", int, False),
    ],
)
def test_oauth_device_authorization_bad_json_schema(
    accept, openid_provider, field_name, ok_type, required, bad_value
):
    # To make the test matrix easy, just skip the tests that aren't actually
    # interesting (field of the correct type, missing optional field).
    if bad_value is Missing and not required:
        pytest.skip("not interesting: optional field")
    elif type(bad_value) == ok_type:  # not isinstance(), because bool is an int
        pytest.skip("not interesting: correct type")

    sock, client = accept(
        oauth_issuer=openid_provider.issuer,
        oauth_client_id=secrets.token_hex(),
    )

    # Set up our provider callbacks.
    # NOTE that these callbacks will be called on a background thread. Don't do
    # any unprotected state mutation here.

    def authorization_endpoint(headers, params):
        # Begin with an acceptable base response...
        resp = {
            "device_code": "my-device-code",
            "user_code": "my-user-code",
            "interval": 0,
            "verification_uri": "https://example.com",
            "expires_in": 5,
        }

        # ...then tweak it so the client fails.
        if bad_value is Missing:
            del resp[field_name]
        else:
            resp[field_name] = bad_value

        return 200, resp

    openid_provider.register_endpoint(
        "device_authorization_endpoint", "POST", "/device", authorization_endpoint
    )

    def token_endpoint(headers, params):
        assert False, "token endpoint was invoked unexpectedly"

    openid_provider.register_endpoint(
        "token_endpoint", "POST", "/token", token_endpoint
    )

    expect_disconnected_handshake(sock)

    # Now make sure the client correctly failed.
    if bad_value is Missing:
        error_pattern = f'field "{field_name}" is missing'
    elif ok_type == str:
        error_pattern = f'field "{field_name}" must be a string'
    elif ok_type == int:
        error_pattern = f'field "{field_name}" must be a number'
    else:
        assert False, "update error_pattern for new failure mode"

    # XXX iddawc doesn't really check for problems in the device authorization
    # response, leading to this patchwork:
    if field_name == "verification_uri":
        error_pattern = alt_patterns(
            error_pattern,
            "issuer did not provide a verification URI",
        )
    elif field_name == "user_code":
        error_pattern = alt_patterns(
            error_pattern,
            "issuer did not provide a user code",
        )
    else:
        error_pattern = alt_patterns(
            error_pattern,
            r"failed to obtain access token: \(iddawc error I_ERROR_PARAM\)",
        )

    with pytest.raises(psycopg2.OperationalError, match=error_pattern):
        client.check_completed()


@pytest.mark.parametrize(
    "failure_mode, error_pattern",
    [
        pytest.param(
            (
                400,
                {
                    "error": "expired_token",
                    "error_description": "the device code has expired",
                },
            ),
            r"failed to obtain access token: the device code has expired \(expired_token\)",
            id="expired token with description",
        ),
        pytest.param(
            (400, {"error": "access_denied"}),
            r"failed to obtain access token: \(access_denied\)",
            id="access denied without description",
        ),
        pytest.param(
            (400, {}),
            alt_patterns(
                r'failed to parse token error response: field "error" is missing',
                r"failed to obtain access token: \(iddawc error I_ERROR_PARAM\)",
            ),
            id="empty error response",
        ),
        pytest.param(
            (200, {}, {}),
            alt_patterns(
                r"failed to parse access token response: no content type was provided",
                r"failed to obtain access token: \(iddawc error I_ERROR\)",
            ),
            id="missing content type",
        ),
        pytest.param(
            (200, {"Content-Type": "text/plain"}, {}),
            alt_patterns(
                r"failed to parse access token response: unexpected content type",
                r"failed to obtain access token: \(iddawc error I_ERROR\)",
            ),
            id="wrong content type",
        ),
    ],
)
@pytest.mark.parametrize("retries", [0, 1])
def test_oauth_token_failures(
    accept, openid_provider, retries, failure_mode, error_pattern
):
    client_id = secrets.token_hex()

    sock, client = accept(
        oauth_issuer=openid_provider.issuer,
        oauth_client_id=client_id,
    )

    device_code = secrets.token_hex()
    user_code = f"{secrets.token_hex(2)}-{secrets.token_hex(2)}"

    # Set up our provider callbacks.
    # NOTE that these callbacks will be called on a background thread. Don't do
    # any unprotected state mutation here.

    def authorization_endpoint(headers, params):
        assert params["client_id"] == [client_id]

        resp = {
            "device_code": device_code,
            "user_code": user_code,
            "interval": 0,
            "verification_uri": "https://example.com/device",
            "expires_in": 5,
        }

        return 200, resp

    openid_provider.register_endpoint(
        "device_authorization_endpoint", "POST", "/device", authorization_endpoint
    )

    retry_lock = threading.Lock()
    final_sent = False

    def token_endpoint(headers, params):
        with retry_lock:
            nonlocal retries, final_sent

            # If the test wants to force the client to retry, return an
            # authorization_pending response and decrement the retry count.
            if retries > 0:
                retries -= 1
                return 400, {"error": "authorization_pending"}

            # We should only return our failure_mode response once; any further
            # requests indicate that the client isn't correctly bailing out.
            assert not final_sent, "client continued after token error"

            final_sent = True

        return failure_mode

    openid_provider.register_endpoint(
        "token_endpoint", "POST", "/token", token_endpoint
    )

    expect_disconnected_handshake(sock)

    # Now make sure the client correctly failed.
    with pytest.raises(psycopg2.OperationalError, match=error_pattern):
        client.check_completed()


@pytest.mark.parametrize(
    "bad_value",
    [
        pytest.param({"device_code": 3}, id="object"),
        pytest.param([1, 2, 3], id="array"),
        pytest.param("some string", id="string"),
        pytest.param(4, id="numeric"),
        pytest.param(False, id="boolean"),
        pytest.param(None, id="null"),
        pytest.param(Missing, id="missing"),
    ],
)
@pytest.mark.parametrize(
    "field_name,ok_type,required",
    [
        ("access_token", str, True),
        ("token_type", str, True),
    ],
)
def test_oauth_token_bad_json_schema(
    accept, openid_provider, field_name, ok_type, required, bad_value
):
    # To make the test matrix easy, just skip the tests that aren't actually
    # interesting (field of the correct type, missing optional field).
    if bad_value is Missing and not required:
        pytest.skip("not interesting: optional field")
    elif type(bad_value) == ok_type:  # not isinstance(), because bool is an int
        pytest.skip("not interesting: correct type")

    sock, client = accept(
        oauth_issuer=openid_provider.issuer,
        oauth_client_id=secrets.token_hex(),
    )

    # Set up our provider callbacks.
    # NOTE that these callbacks will be called on a background thread. Don't do
    # any unprotected state mutation here.

    def authorization_endpoint(headers, params):
        resp = {
            "device_code": "my-device-code",
            "user_code": "my-user-code",
            "interval": 0,
            "verification_uri": "https://example.com",
            "expires_in": 5,
        }

        return 200, resp

    openid_provider.register_endpoint(
        "device_authorization_endpoint", "POST", "/device", authorization_endpoint
    )

    def token_endpoint(headers, params):
        # Begin with an acceptable base response...
        resp = {
            "access_token": secrets.token_urlsafe(),
            "token_type": "bearer",
        }

        # ...then tweak it so the client fails.
        if bad_value is Missing:
            del resp[field_name]
        else:
            resp[field_name] = bad_value

        return 200, resp

    openid_provider.register_endpoint(
        "token_endpoint", "POST", "/token", token_endpoint
    )

    expect_disconnected_handshake(sock)

    # Now make sure the client correctly failed.
    error_pattern = "failed to parse access token response: "
    if bad_value is Missing:
        error_pattern += f'field "{field_name}" is missing'
    elif ok_type == str:
        error_pattern += f'field "{field_name}" must be a string'
    elif ok_type == int:
        error_pattern += f'field "{field_name}" must be a number'
    else:
        assert False, "update error_pattern for new failure mode"

    # XXX iddawc is fairly silent on the topic.
    error_pattern = alt_patterns(
        error_pattern,
        r"failed to obtain access token: \(iddawc error I_ERROR_PARAM\)",
    )

    with pytest.raises(psycopg2.OperationalError, match=error_pattern):
        client.check_completed()


@pytest.mark.parametrize("scope", [None, "openid email"])
@pytest.mark.parametrize(
    "base_response",
    [
        {"status": "invalid_token"},
        {"extra_object": {"key": "value"}, "status": "invalid_token"},
        {"extra_object": {"status": 1}, "status": "invalid_token"},
    ],
)
def test_oauth_discovery(accept, openid_provider, base_response, scope):
    sock, client = accept(oauth_client_id=secrets.token_hex())

    device_code = secrets.token_hex()
    user_code = f"{secrets.token_hex(2)}-{secrets.token_hex(2)}"
    verification_url = "https://example.com/device"

    access_token = secrets.token_urlsafe()

    # Set up our provider callbacks.
    # NOTE that these callbacks will be called on a background thread. Don't do
    # any unprotected state mutation here.

    def authorization_endpoint(headers, params):
        if scope:
            assert params["scope"] == [scope]
        else:
            assert "scope" not in params

        resp = {
            "device_code": device_code,
            "user_code": user_code,
            "interval": 0,
            "verification_uri": verification_url,
            "expires_in": 5,
        }

        return 200, resp

    openid_provider.register_endpoint(
        "device_authorization_endpoint", "POST", "/device", authorization_endpoint
    )

    def token_endpoint(headers, params):
        assert params["grant_type"] == ["urn:ietf:params:oauth:grant-type:device_code"]
        assert params["device_code"] == [device_code]

        # Successfully finish the request by sending the access bearer token.
        resp = {
            "access_token": access_token,
            "token_type": "bearer",
        }

        return 200, resp

    openid_provider.register_endpoint(
        "token_endpoint", "POST", "/token", token_endpoint
    )

    with sock:
        with pq3.wrap(sock, debug_stream=sys.stdout) as conn:
            initial = start_oauth_handshake(conn)

            # For discovery, the client should send an empty auth header. See
            # RFC 7628, Sec. 4.3.
            auth = get_auth_value(initial)
            assert auth == b""

            # We will fail the first SASL exchange. First return a link to the
            # discovery document, pointing to the test provider server.
            resp = dict(base_response)

            discovery_uri = f"{openid_provider.issuer}/.well-known/openid-configuration"
            resp["openid-configuration"] = discovery_uri

            if scope:
                resp["scope"] = scope

            resp = json.dumps(resp)

            pq3.send(
                conn,
                pq3.types.AuthnRequest,
                type=pq3.authn.SASLContinue,
                body=resp.encode("ascii"),
            )

            # Per RFC, the client is required to send a dummy ^A response.
            pkt = pq3.recv1(conn)
            assert pkt.type == pq3.types.PasswordMessage
            assert pkt.payload == b"\x01"

            # Now fail the SASL exchange.
            pq3.send(
                conn,
                pq3.types.ErrorResponse,
                fields=[
                    b"SFATAL",
                    b"C28000",
                    b"Mdoesn't matter",
                    b"",
                ],
            )

    # The client will connect to us a second time, using the parameters we sent
    # it.
    sock, _ = accept()

    with sock:
        with pq3.wrap(sock, debug_stream=sys.stdout) as conn:
            initial = start_oauth_handshake(conn)

            # Validate and accept the token.
            auth = get_auth_value(initial)
            assert auth == f"Bearer {access_token}".encode("ascii")

            pq3.send(conn, pq3.types.AuthnRequest, type=pq3.authn.SASLFinal)
            finish_handshake(conn)


@pytest.mark.parametrize(
    "response,expected_error",
    [
        pytest.param(
            "abcde",
            'Token "abcde" is invalid',
            id="bad JSON: invalid syntax",
        ),
        pytest.param(
            '"abcde"',
            "top-level element must be an object",
            id="bad JSON: top-level element is a string",
        ),
        pytest.param(
            "[]",
            "top-level element must be an object",
            id="bad JSON: top-level element is an array",
        ),
        pytest.param(
            "{}",
            "server sent error response without a status",
            id="bad JSON: no status member",
        ),
        pytest.param(
            '{ "status": null }',
            'field "status" must be a string',
            id="bad JSON: null status member",
        ),
        pytest.param(
            '{ "status": 0 }',
            'field "status" must be a string',
            id="bad JSON: int status member",
        ),
        pytest.param(
            '{ "status": [ "bad" ] }',
            'field "status" must be a string',
            id="bad JSON: array status member",
        ),
        pytest.param(
            '{ "status": { "bad": "bad" } }',
            'field "status" must be a string',
            id="bad JSON: object status member",
        ),
        pytest.param(
            '{ "nested": { "status": "bad" } }',
            "server sent error response without a status",
            id="bad JSON: nested status",
        ),
        pytest.param(
            '{ "status": "invalid_token" ',
            "The input string ended unexpectedly",
            id="bad JSON: unterminated object",
        ),
        pytest.param(
            '{ "status": "invalid_token" } { }',
            'Expected end of input, but found "{"',
            id="bad JSON: trailing data",
        ),
        pytest.param(
            '{ "status": "invalid_token", "openid-configuration": 1 }',
            'field "openid-configuration" must be a string',
            id="bad JSON: int openid-configuration member",
        ),
        pytest.param(
            '{ "status": "invalid_token", "openid-configuration": 1 }',
            'field "openid-configuration" must be a string',
            id="bad JSON: int openid-configuration member",
        ),
        pytest.param(
            '{ "status": "invalid_token", "scope": 1 }',
            'field "scope" must be a string',
            id="bad JSON: int scope member",
        ),
    ],
)
def test_oauth_discovery_server_error(accept, response, expected_error):
    sock, client = accept(oauth_client_id=secrets.token_hex())

    with sock:
        with pq3.wrap(sock, debug_stream=sys.stdout) as conn:
            initial = start_oauth_handshake(conn)

            # Fail the SASL exchange with an invalid JSON response.
            pq3.send(
                conn,
                pq3.types.AuthnRequest,
                type=pq3.authn.SASLContinue,
                body=response.encode("utf-8"),
            )

            # The client should disconnect, so the socket is closed here. (If
            # the client doesn't disconnect, it will report a different error
            # below and the test will fail.)

    with pytest.raises(psycopg2.OperationalError, match=expected_error):
        client.check_completed()


@pytest.mark.parametrize(
    "bad_response,expected_error",
    [
        pytest.param(
            (200, {"Content-Type": "text/plain"}, {}),
            r'failed to parse OpenID discovery document: unexpected content type "text/plain"',
            id="not JSON",
        ),
        pytest.param(
            (200, {}, {}),
            r"failed to parse OpenID discovery document: no content type was provided",
            id="no Content-Type",
        ),
        pytest.param(
            (204, {}, None),
            r"failed to fetch OpenID discovery document: unexpected response code 204",
            id="no content",
        ),
        pytest.param(
            (301, {"Location": "https://localhost/"}, None),
            r"failed to fetch OpenID discovery document: unexpected response code 301",
            id="redirection",
        ),
        pytest.param(
            (404, {}),
            r"failed to fetch OpenID discovery document: unexpected response code 404",
            id="not found",
        ),
        pytest.param(
            (200, RawResponse("blah\x00blah")),
            r"failed to parse OpenID discovery document: response contains embedded NULLs",
            id="NULL bytes in document",
        ),
        pytest.param(
            (200, 123),
            r"failed to parse OpenID discovery document: top-level element must be an object",
            id="scalar at top level",
        ),
        pytest.param(
            (200, []),
            r"failed to parse OpenID discovery document: top-level element must be an object",
            id="array at top level",
        ),
        pytest.param(
            (200, RawResponse("{")),
            r"failed to parse OpenID discovery document.* input string ended unexpectedly",
            id="unclosed object",
        ),
        pytest.param(
            (200, RawResponse(r'{ "hello": ] }')),
            r"failed to parse OpenID discovery document.* Expected JSON value",
            id="bad array",
        ),
        pytest.param(
            (200, {"issuer": 123}),
            r'failed to parse OpenID discovery document: field "issuer" must be a string',
            id="non-string issuer",
        ),
        pytest.param(
            (200, {"issuer": ["something"]}),
            r'failed to parse OpenID discovery document: field "issuer" must be a string',
            id="issuer array",
        ),
        pytest.param(
            (200, {"issuer": {}}),
            r'failed to parse OpenID discovery document: field "issuer" must be a string',
            id="issuer object",
        ),
        pytest.param(
            (200, {"grant_types_supported": 123}),
            r'failed to parse OpenID discovery document: field "grant_types_supported" must be an array of strings',
            id="scalar grant types field",
        ),
        pytest.param(
            (200, {"grant_types_supported": {}}),
            r'failed to parse OpenID discovery document: field "grant_types_supported" must be an array of strings',
            id="object grant types field",
        ),
        pytest.param(
            (200, {"grant_types_supported": [123]}),
            r'failed to parse OpenID discovery document: field "grant_types_supported" must be an array of strings',
            id="non-string grant types",
        ),
        pytest.param(
            (200, {"grant_types_supported": ["something", 123]}),
            r'failed to parse OpenID discovery document: field "grant_types_supported" must be an array of strings',
            id="non-string grant types later in the list",
        ),
        pytest.param(
            (200, {"grant_types_supported": ["something", {}]}),
            r'failed to parse OpenID discovery document: field "grant_types_supported" must be an array of strings',
            id="object grant types later in the list",
        ),
        pytest.param(
            (200, {"grant_types_supported": ["something", ["something"]]}),
            r'failed to parse OpenID discovery document: field "grant_types_supported" must be an array of strings',
            id="embedded array grant types later in the list",
        ),
        pytest.param(
            (
                200,
                {
                    "grant_types_supported": ["something"],
                    "token_endpoint": "https://example.com/",
                    "issuer": 123,
                },
            ),
            r'failed to parse OpenID discovery document: field "issuer" must be a string',
            id="non-string issuer after other valid fields",
        ),
        pytest.param(
            (
                200,
                {
                    "ignored": {"grant_types_supported": 123, "token_endpoint": 123},
                    "issuer": 123,
                },
            ),
            r'failed to parse OpenID discovery document: field "issuer" must be a string',
            id="non-string issuer after other ignored fields",
        ),
        pytest.param(
            (200, {"token_endpoint": "https://example.com/"}),
            r'failed to parse OpenID discovery document: field "issuer" is missing',
            id="missing issuer",
        ),
        pytest.param(
            (200, {"issuer": "https://example.com/"}),
            r'failed to parse OpenID discovery document: field "token_endpoint" is missing',
            id="missing token endpoint",
        ),
        pytest.param(
            (
                200,
                {
                    "issuer": "https://example.com",
                    "token_endpoint": "https://example.com/token",
                    "device_authorization_endpoint": "https://example.com/dev",
                },
            ),
            r'cannot run OAuth device authorization: issuer "https://example.com" does not support device code grants',
            id="missing device code grants",
        ),
        pytest.param(
            (
                200,
                {
                    "issuer": "https://example.com",
                    "token_endpoint": "https://example.com/token",
                    "grant_types_supported": [
                        "urn:ietf:params:oauth:grant-type:device_code"
                    ],
                },
            ),
            r'cannot run OAuth device authorization: issuer "https://example.com" does not provide a device authorization endpoint',
            id="missing device_authorization_endpoint",
        ),
        #
        # Exercise HTTP-level failures by breaking the protocol. Note that the
        # error messages here are implementation-dependent.
        #
        pytest.param(
            (1000, {}),
            r"failed to fetch OpenID discovery document: Unsupported protocol \(.*\)",
            id="invalid HTTP response code",
        ),
        pytest.param(
            (200, {"Content-Length": -1}, {}),
            r"failed to fetch OpenID discovery document: Weird server reply \(.*Content-Length.*\)",
            id="bad HTTP Content-Length",
        ),
    ],
)
def test_oauth_discovery_provider_failure(
    accept, openid_provider, bad_response, expected_error
):
    sock, client = accept(
        oauth_issuer=openid_provider.issuer,
        oauth_client_id=secrets.token_hex(),
    )

    def failing_discovery_handler(headers, params):
        return bad_response

    openid_provider.register_endpoint(
        None,
        "GET",
        "/.well-known/openid-configuration",
        failing_discovery_handler,
    )

    expect_disconnected_handshake(sock)

    # XXX iddawc doesn't differentiate...
    expected_error = alt_patterns(
        expected_error,
        r"failed to fetch OpenID discovery document \(iddawc error I_ERROR(_PARAM)?\)",
    )

    with pytest.raises(psycopg2.OperationalError, match=expected_error):
        client.check_completed()


@pytest.mark.parametrize(
    "sasl_err,resp_type,resp_payload,expected_error",
    [
        pytest.param(
            {"status": "invalid_request"},
            pq3.types.ErrorResponse,
            dict(
                fields=[b"SFATAL", b"C28000", b"Mexpected error message", b""],
            ),
            "expected error message",
            id="standard server error: invalid_request",
        ),
        pytest.param(
            {"status": "invalid_token"},
            pq3.types.ErrorResponse,
            dict(
                fields=[b"SFATAL", b"C28000", b"Mexpected error message", b""],
            ),
            "expected error message",
            id="standard server error: invalid_token without discovery URI",
        ),
        pytest.param(
            {"status": "invalid_request"},
            pq3.types.AuthnRequest,
            dict(type=pq3.authn.SASLContinue, body=b""),
            "server sent additional OAuth data",
            id="broken server: additional challenge after error",
        ),
        pytest.param(
            {"status": "invalid_request"},
            pq3.types.AuthnRequest,
            dict(type=pq3.authn.SASLFinal),
            "server sent additional OAuth data",
            id="broken server: SASL success after error",
        ),
        pytest.param(
            {"status": "invalid_request"},
            pq3.types.AuthnRequest,
            dict(type=pq3.authn.SASL, body=[b"OAUTHBEARER", b""]),
            "duplicate SASL authentication request",
            id="broken server: SASL reinitialization after error",
        ),
    ],
)
def test_oauth_server_error(accept, sasl_err, resp_type, resp_payload, expected_error):
    sock, client = accept()

    with sock:
        with pq3.wrap(sock, debug_stream=sys.stdout) as conn:
            start_oauth_handshake(conn)

            # Ignore the client data. Return an error "challenge".
            resp = json.dumps(sasl_err)
            resp = resp.encode("utf-8")

            pq3.send(
                conn, pq3.types.AuthnRequest, type=pq3.authn.SASLContinue, body=resp
            )

            # Per RFC, the client is required to send a dummy ^A response.
            pkt = pq3.recv1(conn)
            assert pkt.type == pq3.types.PasswordMessage
            assert pkt.payload == b"\x01"

            # Now fail the SASL exchange (in either a valid way, or an invalid
            # one, depending on the test).
            pq3.send(conn, resp_type, **resp_payload)

    with pytest.raises(psycopg2.OperationalError, match=expected_error):
        client.check_completed()
