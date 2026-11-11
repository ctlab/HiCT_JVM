#!/usr/bin/env python3
"""Print /matrix/query and referenced schemas from the local HiCT OpenAPI spec."""

import argparse
import json

import requests
import yaml


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:5000")
    args = parser.parse_args()
    paths = [
        "/api/v1/openapi.yaml",
        "/openapi.json",
        "/api/v1/openapi.json",
        "/swagger.json",
    ]
    with requests.Session() as session:
        for path in paths:
            try:
                response = session.get(args.server.rstrip("/") + path, timeout=5)
                response.raise_for_status()
                document = yaml.safe_load(response.text)
                if not isinstance(document, dict):
                    continue
                operation = document.get("paths", {}).get("/matrix/query")
                if operation:
                    print(
                        json.dumps(
                            {
                                "paths": {"/matrix/query": operation},
                                "components": document.get("components", {}),
                            },
                            indent=2,
                        )
                    )
                    return
            except (requests.RequestException, yaml.YAMLError) as error:
                print(f"{path}: {error}")
    parser.exit(1, "No /matrix/query schema found. Check the server URL.\n")


if __name__ == "__main__":
    main()
