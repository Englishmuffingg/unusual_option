from __future__ import annotations

import os

import requests

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://optioncharts.io/",
}


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    cookie = os.getenv("OPTIONCHARTS_COOKIE", "").strip()
    if cookie:
        s.headers["Cookie"] = cookie
    return s
