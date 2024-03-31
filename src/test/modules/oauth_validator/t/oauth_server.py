#! /usr/bin/env python3

import http.server
import json
import os
import sys


class OAuthHandler(http.server.BaseHTTPRequestHandler):
    JsonObject = dict[str, object]  # TypeAlias is not available until 3.10

    def _check_issuer(self):
        """
        Switches the behavior of the provider depending on the issuer URI.
        """
        self._alt_issuer = self.path.startswith("/alternate/")
        if self._alt_issuer:
            self.path = self.path.removeprefix("/alternate")

    def do_GET(self):
        self._check_issuer()

        if self.path == "/.well-known/openid-configuration":
            resp = self.config()
        else:
            self.send_error(404, "Not Found")
            return

        self._send_json(resp)

    def do_POST(self):
        self._check_issuer()

        if self.path == "/authorize":
            resp = self.authorization()
        elif self.path == "/token":
            resp = self.token()
        else:
            self.send_error(404, "Not Found")
            return

        self._send_json(resp)

    def _send_json(self, js: JsonObject) -> None:
        """
        Sends the provided JSON dict as an application/json response.
        """

        resp = json.dumps(js).encode("ascii")

        self.send_response(200, "OK")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(resp)))
        self.end_headers()

        self.wfile.write(resp)

    def config(self) -> JsonObject:
        port = self.server.socket.getsockname()[1]
        issuer = f"http://localhost:{port}"
        if self._alt_issuer:
            issuer += "/alternate"

        return {
            "issuer": issuer,
            "token_endpoint": issuer + "/token",
            "device_authorization_endpoint": issuer + "/authorize",
            "response_types_supported": ["token"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
            "grant_types_supported": ["urn:ietf:params:oauth:grant-type:device_code"],
        }

    def authorization(self) -> JsonObject:
        uri = "https://example.com/"
        if self._alt_issuer:
            uri = "https://example.org/"

        return {
            "device_code": "postgres",
            "user_code": "postgresuser",
            "interval": 0,
            "verification_uri": uri,
            "expires-in": 5,
        }

    def token(self) -> JsonObject:
        token = "9243959234"
        if self._alt_issuer:
            token += "-alt"

        return {
            "access_token": token,
            "token_type": "bearer",
        }


def main():
    s = http.server.HTTPServer(("127.0.0.1", 0), OAuthHandler)

    # Give the parent the port number to contact (this is also the signal that
    # we're ready to receive requests).
    port = s.socket.getsockname()[1]
    print(port)

    stdout = sys.stdout.fileno()
    sys.stdout.close()
    os.close(stdout)

    s.serve_forever()  # we expect our parent to send a termination signal


if __name__ == "__main__":
    main()
