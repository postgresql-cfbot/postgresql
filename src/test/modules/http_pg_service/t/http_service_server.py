#! /usr/bin/env python3
#
# A mock http pg_service server, designed to be invoked from
# PgHttpService/Server.pm. This listens on an ephemeral port number (printed to stdout
# so that the Perl tests can contact it) and runs as a daemon until it is
# signaled.
#

import http.server
import os
import sys
import textwrap


class ServiceFileHandler(http.server.BaseHTTPRequestHandler):
    """
    Core implementation of the service file server. The API is
    inheritance-based, with an entry point at do_GET(). See the
    documentation for BaseHTTPRequestHandler.
    """

    JsonObject = dict[str, object]  # TypeAlias is not available until 3.10


    def do_GET(self):
        self._response_code = 200

        self._send_service_file()

    def _send_service_file(self) -> None:
        """
        Sends the provided JSON dict as an application/json response.
        self._response_code can be modified to send JSON error responses.
        """

        service_file_path = sys.argv[1] + "/" + self.path
        with open(service_file_path, "r") as file:
            service_file_content = file.read()

        resp = service_file_content.encode()
        self.log_message("sending string response: %s", resp)

        self.send_response(self._response_code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(resp)))
        self.end_headers()

        self.wfile.write(resp)


def main():
    """
    Starts the PgHttpService server on localhost. The ephemeral port in use will
    be printed to stdout.
    """

    s = http.server.HTTPServer(("127.0.0.1", 0), ServiceFileHandler)

    # Give the parent the port number to contact (this is also the signal that
    # we're ready to receive requests).
    port = s.socket.getsockname()[1]
    print(port)

    # stdout is closed to allow the parent to just "read to the end".
    stdout = sys.stdout.fileno()
    sys.stdout.close()
    os.close(stdout)

    s.serve_forever()  # we expect our parent to send a termination signal


if __name__ == "__main__":
    main()
