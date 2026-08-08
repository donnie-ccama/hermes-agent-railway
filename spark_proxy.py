"""Stream an OpenAI-compatible Spark endpoint through Tailscale userspace networking."""

from __future__ import annotations

import http.client
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urljoin


UPSTREAM = os.environ["SPARK_VLLM_UPSTREAM"].rstrip("/") + "/"
PROXY_HOST = os.environ.get("TAILSCALE_HTTP_PROXY_HOST", "127.0.0.1")
PROXY_PORT = int(os.environ.get("TAILSCALE_HTTP_PROXY_PORT", "1055"))
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade"}


class SparkProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _forward(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        headers = {key: value for key, value in self.headers.items() if key.lower() not in HOP_BY_HOP | {"host", "content-length"}}
        if body:
            headers["Content-Length"] = str(len(body))
        target = urljoin(UPSTREAM, self.path.lstrip("/"))
        connection = http.client.HTTPConnection(PROXY_HOST, PROXY_PORT, timeout=300)
        try:
            connection.request(self.command, target, body=body, headers=headers)
            response = connection.getresponse()
            self.send_response(response.status, response.reason)
            for key, value in response.getheaders():
                if key.lower() not in HOP_BY_HOP | {"content-length"}:
                    self.send_header(key, value)
            self.send_header("Connection", "close")
            self.end_headers()
            while chunk := response.read(64 * 1024):
                self.wfile.write(chunk)
                self.wfile.flush()
        finally:
            connection.close()

    do_GET = _forward
    do_POST = _forward
    do_DELETE = _forward

    def log_message(self, format: str, *args: object) -> None:
        print(f"[spark-proxy] {format % args}", flush=True)


ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("SPARK_VLLM_PROXY_PORT", "18000"))), SparkProxyHandler).serve_forever()
