#!/usr/bin/env python3
"""
Register the Debezium PostgreSQL connector via Kafka Connect REST API.

Run from host (Windows PowerShell) OR from inside a container.

Examples:
  python scripts/register_debezium_connector.py
  python scripts/register_debezium_connector.py --connect-url http://localhost:8083

By default, uses CONNECT_URL env var if present, else http://localhost:8083.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def http(method: str, url: str, body: dict | None = None) -> tuple[int, str]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--connect-url",
        default=os.environ.get("CONNECT_URL", "http://localhost:8083"),
        help="Kafka Connect base URL",
    )
    p.add_argument(
        "--config",
        default=os.path.join("config", "debezium-postgres-connector.json"),
        help="Path to connector JSON payload (contains name + config).",
    )
    args = p.parse_args()

    if not os.path.exists(args.config):
        sys.exit(f"Connector config not found: {args.config}")

    with open(args.config, "r", encoding="utf-8") as f:
        payload = json.load(f)

    name = payload.get("name")
    if not name:
        sys.exit("Connector JSON must include top-level 'name'.")

    base = args.connect_url.rstrip("/")
    status_code, _ = http("GET", f"{base}/")
    if status_code >= 400:
        sys.exit(f"Kafka Connect not reachable at {base} (GET / returned {status_code})")

    # If connector exists, update config; else create.
    code, _ = http("GET", f"{base}/connectors/{name}")
    if code == 200:
        code, text = http("PUT", f"{base}/connectors/{name}/config", payload["config"])
        print(f"Updated connector '{name}' (HTTP {code})")
    else:
        code, text = http("POST", f"{base}/connectors", payload)
        print(f"Created connector '{name}' (HTTP {code})")

    if text:
        print(text)


if __name__ == "__main__":
    main()

