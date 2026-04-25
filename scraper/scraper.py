import requests
import os
import time

HTML_DIR = "data/raw/html"


def fetch_html(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            return response.text
        else:
            print(f"⚠️ Failed: {url} ({response.status_code})")
            return None

    except Exception as e:
        print(f"❌ Error: {url} -> {e}")
        return None


def save_html(name, html):
    os.makedirs(HTML_DIR, exist_ok=True)

    filename = name.replace(" ", "_").replace("/", "_")

    path = f"{HTML_DIR}/{filename}.html"

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ Saved: {filename}")