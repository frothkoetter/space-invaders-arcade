#!/usr/bin/env python3
"""
Serve Space Invaders static files on 127.0.0.1:8080 and proxy:
  POST/OPTIONS /space-invaders → NiFi ListenHTTP
  GET /api/ssb/top10player → Flink SSB materialized view (Basic auth from env)

Usage:
  python3 serve-with-nifi-proxy.py
  SSB_USER=you SSB_PASSWORD=secret python3 serve-with-nifi-proxy.py
  python3 serve-with-nifi-proxy.py --upstream http://other-host:9088/space-invaders

Default upstream (projtriton CDP inbound HTTP):
  http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import zlib
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

DEFAULT_UPSTREAM = (
    "http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders"
)
BIND_HOST = "127.0.0.1"
BIND_PORT = 8080
INGEST_PATH = "/space-invaders"
SSB_PROXY_PATH = "/api/ssb/top10player"
DEFAULT_SSB_UPSTREAM = (
    "https://trition-si-gateway.se-sandb.a465-9q4k.cloudera.site"
    "/trition-si/cdp-proxy-api/ssb-mve-api/api/v1/query/5198/top10player"
    "?key=14de1710-a15b-49ef-a088-83a9c43a631f"
)

ROOT = Path(__file__).resolve().parent
LEADERBOARD_FILE = ROOT / "top10-leaderboard.html"
LEADERBOARD_BLOB_FILE = ROOT / "_leaderboard_blob.txt"
LEADERBOARD_URL_PATHS = frozenset({
    "/top10-leaderboard.html",
    "/leaderboard.html",
    "/leaderboard",
})


def _leaderboard_html_bytes() -> bytes:
    if LEADERBOARD_FILE.is_file():
        return LEADERBOARD_FILE.read_bytes()
    if LEADERBOARD_BLOB_FILE.is_file():
        raw = LEADERBOARD_BLOB_FILE.read_text(encoding="utf-8").strip()
        return zlib.decompress(base64.b64decode(raw))
    raise FileNotFoundError(
        "top10-leaderboard.html missing. Open it in Cursor Context to sync, "
        "or ensure _leaderboard_blob.txt is beside serve-with-nifi-proxy.py"
    )


def materialize_leaderboard_if_missing() -> None:
    if LEADERBOARD_FILE.is_file():
        return
    try:
        LEADERBOARD_FILE.write_bytes(_leaderboard_html_bytes())
        print(f"Wrote missing {LEADERBOARD_FILE.name} from bundled copy")
    except FileNotFoundError:
        pass


def cors_preflight_response() -> tuple[int, bytes, list[tuple[str, str]]]:
    headers = [
        ("Access-Control-Allow-Origin", "*"),
        ("Access-Control-Allow-Methods", "POST, OPTIONS"),
        (
            "Access-Control-Allow-Headers",
            "Content-Type, Accept, Accept-Language, Content-Language",
        ),
        ("Access-Control-Max-Age", "86400"),
        ("Content-Length", "0"),
    ]
    return HTTPStatus.NO_CONTENT, b"", headers


def proxy_post(upstream_base: str, body: bytes, content_type: str | None) -> tuple[int, bytes, list[tuple[str, str]]]:
    target = upstream_base.rstrip("/")
    req = Request(target, data=body, method="POST")
    if content_type:
        req.add_header("Content-Type", content_type)
    else:
        req.add_header("Content-Type", "text/plain;charset=UTF-8")
    try:
        with urlopen(req, timeout=60) as resp:
            status = resp.status
            resp_body = resp.read()
    except HTTPError as e:
        status = e.code
        resp_body = e.read() or str(e).encode("utf-8", errors="replace")
    except URLError as e:
        status = HTTPStatus.BAD_GATEWAY
        resp_body = f"Upstream error: {e.reason}".encode("utf-8", errors="replace")

    headers = [
        ("Access-Control-Allow-Origin", "*"),
        ("Content-Type", "text/plain; charset=utf-8"),
        ("Content-Length", str(len(resp_body))),
    ]
    return status, resp_body, headers


def proxy_ssb_get(
    upstream_template: str,
    query: dict[str, list[str]],
    user: str | None,
    password: str | None,
) -> tuple[int, bytes, list[tuple[str, str]]]:
    parsed = urlparse(upstream_template)
    merged = parse_qs(parsed.query, keep_blank_values=True)
    for k, v in query.items():
        if k != "path":
            merged[k] = v
    if "limit" not in merged:
        merged.setdefault("limit", ["100"])
    target = urlunparse(parsed._replace(query=urlencode(merged, doseq=True)))
    req = Request(target, method="GET")
    req.add_header("Accept", "application/json")
    if user:
        token = base64.b64encode(f"{user}:{password or ''}".encode("utf-8")).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
    try:
        with urlopen(req, timeout=90) as resp:
            status = resp.status
            resp_body = resp.read()
    except HTTPError as e:
        status = e.code
        resp_body = e.read() or str(e).encode("utf-8", errors="replace")
    except URLError as e:
        status = HTTPStatus.BAD_GATEWAY
        resp_body = json.dumps({"error": str(e.reason)}).encode("utf-8")

    headers = [
        ("Access-Control-Allow-Origin", "*"),
        ("Content-Type", "application/json; charset=utf-8"),
        ("Content-Length", str(len(resp_body))),
    ]
    return status, resp_body, headers


class NifiProxyHandler(SimpleHTTPRequestHandler):
    upstream_base: str = DEFAULT_UPSTREAM
    ssb_upstream: str = DEFAULT_SSB_UPSTREAM
    ssb_user: str | None = None
    ssb_password: str | None = None

    def __init__(self, *args, directory: str | None = None, **kwargs):
        super().__init__(*args, directory=directory or str(ROOT), **kwargs)

    def log_message(self, format: str, *args) -> None:
        sys.stderr.write("%s - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))

    def end_headers(self) -> None:
        # Static files are same-origin; proxy responses add CORS explicitly.
        super().end_headers()

    def _serve_leaderboard(self) -> None:
        try:
            body = _leaderboard_html_bytes()
        except FileNotFoundError as e:
            msg = f"<pre>{e}</pre>".encode("utf-8")
            self.send_response(HTTPStatus.NOT_FOUND)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = urlparse(self.path).path.rstrip("/") or "/"
        if path in LEADERBOARD_URL_PATHS:
            self._serve_leaderboard()
            return
        if path == SSB_PROXY_PATH.rstrip("/"):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query, keep_blank_values=True)
            if not self.ssb_user:
                body = json.dumps(
                    {"error": "Set SSB_USER and SSB_PASSWORD environment variables"}
                ).encode("utf-8")
                self.send_response(HTTPStatus.UNAUTHORIZED)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            status, resp_body, extra = proxy_ssb_get(
                self.ssb_upstream,
                query,
                self.ssb_user,
                self.ssb_password,
            )
            self.send_response(status)
            for k, v in extra:
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(resp_body)
            return
        return super().do_GET()

    def do_OPTIONS(self) -> None:
        path = urlparse(self.path).path.rstrip("/")
        if path in (INGEST_PATH.rstrip("/"), SSB_PROXY_PATH.rstrip("/")):
            status, body, extra = cors_preflight_response()
            self.send_response(status)
            for k, v in extra:
                self.send_header(k, v)
            self.end_headers()
            if body:
                self.wfile.write(body)
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path.rstrip("/") != INGEST_PATH.rstrip("/"):
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        content_type = self.headers.get("Content-Type")
        status, resp_body, extra = proxy_post(self.upstream_base, body, content_type)
        self.send_response(status)
        for k, v in extra:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(resp_body)

    def translate_path(self, path: str) -> str:
        # Serve index.html for /
        parsed = urlparse(path)
        if parsed.path in ("", "/"):
            path = "/index.html"
        return super().translate_path(path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Static game + NiFi ingest proxy on :8080")
    parser.add_argument(
        "--upstream",
        default=os.environ.get("NIFI_UPSTREAM", DEFAULT_UPSTREAM),
        help="NiFi ListenHTTP URL (default: env NIFI_UPSTREAM or projtriton :9088)",
    )
    parser.add_argument("--host", default=BIND_HOST)
    parser.add_argument("--port", type=int, default=BIND_PORT)
    parser.add_argument(
        "--ssb-upstream",
        default=os.environ.get("SSB_UPSTREAM", DEFAULT_SSB_UPSTREAM),
        help="Flink SSB MVE top10player URL (without limit, or with base query)",
    )
    args = parser.parse_args()
    materialize_leaderboard_if_missing()

    handler = partial(NifiProxyHandler, directory=str(ROOT))
    NifiProxyHandler.upstream_base = args.upstream.rstrip("/")
    NifiProxyHandler.ssb_upstream = args.ssb_upstream
    NifiProxyHandler.ssb_user = os.environ.get("SSB_USER") or None
    NifiProxyHandler.ssb_password = os.environ.get("SSB_PASSWORD") or ""

    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"Serving {ROOT} at http://{args.host}:{args.port}/")
    print(f"Proxy POST {INGEST_PATH} → {NifiProxyHandler.upstream_base}")
    print(f"Proxy GET  {SSB_PROXY_PATH} → SSB (SSB_USER={'set' if NifiProxyHandler.ssb_user else 'MISSING'})")
    print("Game URL (proxy ingest, CORS mode OK):")
    print(
        f"  http://{args.host}:{args.port}/index.html?nifi=1&events=1"
        f"&nifi-url=http://{args.host}:{args.port}{INGEST_PATH}"
    )
    lb_ok = LEADERBOARD_FILE.is_file() or LEADERBOARD_BLOB_FILE.is_file()
    print(
        f"Leaderboard: http://{args.host}:{args.port}/top10-leaderboard.html"
        + ("" if lb_ok else " (WARNING: sync top10-leaderboard.html or _leaderboard_blob.txt)")
    )
    print(f"  (alias) http://{args.host}:{args.port}/leaderboard")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
