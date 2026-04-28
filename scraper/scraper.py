import os
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent.parent
HTML_DIR = BASE_DIR / "data" / "raw" / "html"


def fetch_html(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            return response.text

        print(f"Failed: {url} ({response.status_code})")
        return None

    except Exception as e:
        print(f"Error: {url} -> {e}")
        return None


def save_html(name, html):
    os.makedirs(HTML_DIR, exist_ok=True)

    filename = name.replace(" ", "_").replace("/", "_")
    path = HTML_DIR / f"{filename}.html"

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Saved: {filename}")
