#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
抓取贝壳二手房列表页数据并导出 CSV。

示例:
py -3 beike_ershoufang_spider.py --city sh --pages 2 --output sh_ershoufang.csv
"""

from __future__ import annotations

import argparse
import csv
import html
import random
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, asdict
from typing import Iterable, List


UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]


@dataclass
class HouseItem:
    title: str
    detail_url: str
    area: str
    community: str
    total_price_wan: str
    unit_price: str
    house_info: str
    follow_info: str


def clean_html_text(text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", text)
    no_space = re.sub(r"\s+", " ", no_tags).strip()
    return html.unescape(no_space)


class BeikeSpider:
    def __init__(self, city: str, timeout: int = 12, sleep_sec: float = 1.2) -> None:
        self.city = city.strip().lower()
        self.timeout = timeout
        self.sleep_sec = sleep_sec

    def _headers(self) -> dict:
        return {
            "User-Agent": random.choice(UA_LIST),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Referer": f"https://{self.city}.ke.com/ershoufang/",
        }

    def _page_url(self, page: int) -> str:
        return f"https://{self.city}.ke.com/ershoufang/pg{page}/"

    def fetch_html(self, page: int) -> str:
        url = self._page_url(page)
        req = urllib.request.Request(url=url, headers=self._headers())
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.read().decode(charset, errors="ignore")

    def parse_items(self, html_text: str) -> List[HouseItem]:
        li_blocks = re.findall(r'<li\s+class="clear"[\s\S]*?</li>', html_text, flags=re.IGNORECASE)
        items: List[HouseItem] = []

        for block in li_blocks:
            title_m = re.search(
                r'<div\s+class="title">\s*<a[^>]*href="([^"]+)"[^>]*>([^<]+)</a>',
                block,
                flags=re.IGNORECASE,
            )
            if not title_m:
                continue

            detail_url = html.unescape(title_m.group(1).strip())
            title = clean_html_text(title_m.group(2))

            pos_links = re.findall(
                r'<div\s+class="positionInfo">([\s\S]*?)</div>',
                block,
                flags=re.IGNORECASE,
            )
            community = ""
            if pos_links:
                a_nodes = re.findall(r"<a[^>]*>([\s\S]*?)</a>", pos_links[0], flags=re.IGNORECASE)
                if a_nodes:
                    community = clean_html_text(a_nodes[0])

            area = ""
            area_m = re.search(r"(\S+区)", clean_html_text(block))
            if area_m:
                area = area_m.group(1)

            total_m = re.search(
                r'<div\s+class="[^"]*\btotalPrice\b[^"]*"[^>]*>[\s\S]*?<span[^>]*>\s*([^<]+)\s*</span>\s*<i>\s*万\s*</i>',
                block,
                flags=re.IGNORECASE,
            )
            unit_m = re.search(
                r'<div\s+class="[^"]*\bunitPrice\b[^"]*"[^>]*>\s*<span>([\s\S]*?)</span>',
                block,
                flags=re.IGNORECASE,
            )
            house_m = re.search(r'<div\s+class="houseInfo">([\s\S]*?)</div>', block, flags=re.IGNORECASE)
            follow_m = re.search(r'<div\s+class="followInfo">([\s\S]*?)</div>', block, flags=re.IGNORECASE)

            items.append(
                HouseItem(
                    title=title,
                    detail_url=detail_url,
                    area=area,
                    community=community,
                    total_price_wan=(clean_html_text(total_m.group(1)) + "万") if total_m else "",
                    unit_price=clean_html_text(unit_m.group(1)) if unit_m else "",
                    house_info=clean_html_text(house_m.group(1)) if house_m else "",
                    follow_info=clean_html_text(follow_m.group(1)) if follow_m else "",
                )
            )

        return items

    def crawl(self, pages: int) -> List[HouseItem]:
        all_items: List[HouseItem] = []
        for page in range(1, pages + 1):
            try:
                html_text = self.fetch_html(page)
                page_items = self.parse_items(html_text)
                all_items.extend(page_items)
                print(f"[OK] page={page} items={len(page_items)}")
            except urllib.error.HTTPError as exc:
                print(f"[WARN] page={page} HTTPError: {exc.code} {exc.reason}", file=sys.stderr)
            except urllib.error.URLError as exc:
                print(f"[WARN] page={page} URLError: {exc.reason}", file=sys.stderr)
            except Exception as exc:  # noqa: BLE001
                print(f"[WARN] page={page} failed: {exc}", file=sys.stderr)
            time.sleep(self.sleep_sec)
        return all_items


def save_csv(items: Iterable[HouseItem], output: str) -> None:
    rows = [asdict(i) for i in items]
    if not rows:
        print("没有抓取到任何数据")
        return

    fieldnames = list(rows[0].keys())
    with open(output, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"已写入: {output} (共 {len(rows)} 条)")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="抓取贝壳二手房列表页并导出CSV")
    p.add_argument("--city", required=True, help="城市简拼，如 bj/sh/gz/sz")
    p.add_argument("--pages", type=int, default=1, help="抓取页数，默认 1")
    p.add_argument("--output", default="beike_ershoufang.csv", help="输出CSV文件名")
    p.add_argument("--sleep", type=float, default=1.2, help="分页抓取间隔秒数")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.pages < 1:
        raise ValueError("--pages 必须 >= 1")

    spider = BeikeSpider(city=args.city, sleep_sec=args.sleep)
    items = spider.crawl(pages=args.pages)
    save_csv(items, args.output)


if __name__ == "__main__":
    main()
