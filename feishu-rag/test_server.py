# -*- coding: utf-8 -*-
"""Minimal test: verify port 9000 receives requests"""
from http.server import HTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        print(f"[TEST] GET {self.path}", flush=True)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        print(f"[TEST] POST {self.path} len={len(body)}", flush=True)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

if __name__ == "__main__":
    print("Test server on port 9000. Send request and check this window.", flush=True)
    HTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
