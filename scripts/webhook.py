"""Local demo receiver only. Never deploy this unauthenticated helper publicly."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

events = []
fail = False


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        global fail
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        if self.path == "/mode/fail":
            fail = True
        elif self.path == "/mode/success":
            fail = False
        elif self.path == "/callback":
            events.append(
                {"body": json.loads(body), "event_id": self.headers.get("X-Webhook-Event-ID")}
            )
        else:
            self.send_error(404)
            return
        self.send_response(503 if fail and self.path == "/callback" else 204)
        self.end_headers()

    def do_GET(self):
        data = json.dumps(events).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
