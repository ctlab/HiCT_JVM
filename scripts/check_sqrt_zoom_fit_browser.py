#!/usr/bin/env python3
"""Verify that automatic contrast keeps square-root coverage readable while zooming."""
from check_zoom_readability_browser import main

if __name__ == "__main__":
    main(default_normalizations=("sqrt",))
