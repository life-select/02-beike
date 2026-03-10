#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import html
import json
import math
import random
import re
import threading
import time
import tkinter as tk
import urllib.request
import webbrowser
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk


UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]
MOBILE_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
)


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


def normalize_text(text: str) -> str:
    s = text.lower().strip()
    return "".join(ch for ch in s if ch not in " \t\r\n-_/|,，。.;；:：()（）[]【】")


def parse_resblock_url(url: str) -> tuple[str, str] | None:
    u = (url or "").strip()
    m = re.search(r"https?://([a-z]+)\.ke\.com/ershoufang/(?:pg\d+)?c(\d+)/?", u, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).lower(), m.group(2)


class BeikeSpider:
    def __init__(
        self,
        city: str,
        timeout: int = 12,
        sleep_sec: float = 1.2,
        cookie: str = "",
        use_playwright: bool = False,
        storage_state_path: str = "",
        pw_headless: bool = False,
    ) -> None:
        self.city = city.strip().lower()
        self.timeout = timeout
        self.sleep_sec = sleep_sec
        self.cookie = cookie.strip()
        self.use_playwright = use_playwright
        self.storage_state_path = storage_state_path.strip()
        self.pw_headless = pw_headless
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    def _headers_pc(self) -> dict:
        # Keep a stable UA when cookie is provided to reduce fingerprint jitter.
        ua = UA_LIST[0] if self.cookie else random.choice(UA_LIST)
        h = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Sec-CH-UA": '"Not A(Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Referer": f"https://{self.city}.ke.com/ershoufang/",
        }
        if self.cookie:
            h["Cookie"] = self.cookie
        return h

    def _headers_mobile(self) -> dict:
        h = {
            "User-Agent": MOBILE_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Referer": f"https://m.ke.com/{self.city}/ershoufang/",
        }
        if self.cookie:
            h["Cookie"] = self.cookie
        return h

    def _init_playwright(self) -> None:
        if self._context is not None and self._page is not None:
            return
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise RuntimeError("未安装 Playwright，无法使用浏览器会话抓取") from exc

        self._pw = sync_playwright().start()
        launch_kwargs = {
            "headless": self.pw_headless,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        # Prefer system Chrome when available to reduce fingerprint gap.
        try:
            self._browser = self._pw.chromium.launch(channel="chrome", **launch_kwargs)
        except Exception:
            self._browser = self._pw.chromium.launch(**launch_kwargs)
        ctx_kwargs = {
            "user_agent": random.choice(UA_LIST),
            "locale": "zh-CN",
        }
        if self.storage_state_path and Path(self.storage_state_path).exists():
            ctx_kwargs["storage_state"] = self.storage_state_path
        self._context = self._browser.new_context(**ctx_kwargs)
        # Only inject cookie when no storage_state is available.
        if not (self.storage_state_path and Path(self.storage_state_path).exists()):
            injected = self._cookie_items_for_playwright()
            if injected:
                try:
                    self._context.add_cookies(injected)
                except Exception:
                    pass
        self._page = self._context.new_page()

    def _fetch_url_playwright(self, url: str, referer: str = "") -> str:
        self._init_playwright()
        assert self._page is not None
        self._page.goto(url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000), referer=referer or None)
        return self._page.content()

    def _cookie_items_for_playwright(self) -> list[dict]:
        out: list[dict] = []
        if not self.cookie:
            return out
        parts = [p.strip() for p in self.cookie.split(";") if p.strip() and ("=" in p)]
        for p in parts:
            name, value = p.split("=", 1)
            n = name.strip()
            if not n:
                continue
            out.append(
                {
                    "name": n,
                    "value": value.strip(),
                    "domain": ".ke.com",
                    "path": "/",
                    "secure": True,
                    "httpOnly": False,
                }
            )
        return out

    def fetch_url(self, url: str, headers: dict) -> str:
        if self.use_playwright:
            return self._fetch_url_playwright(url, str(headers.get("Referer", "")))
        req = urllib.request.Request(url=url, headers=headers)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.read().decode(charset, errors="ignore")

    def close(self) -> None:
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._pw:
                self._pw.stop()
        except Exception:
            pass
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    def fetch_html_mobile(self, page: int) -> str:
        return self.fetch_url(f"https://m.ke.com/{self.city}/ershoufang/?curPage={page}", self._headers_mobile())

    def _is_captcha_page(self, html_text: str) -> bool:
        return ("<title>CAPTCHA" in html_text) or ("verify" in html_text.lower() and "captcha" in html_text.lower())

    def _is_login_page(self, html_text: str) -> bool:
        return ('content="LOGIN"' in html_text) or ("<title>登录" in html_text)

    def parse_items_mobile(self, html_text: str) -> list[HouseItem]:
        blocks = re.findall(
            r'<div\s+class="kem__house-tile-ershou"\s+data-id="(\d+)">([\s\S]*?)</div>\s*</a>',
            html_text,
            flags=re.IGNORECASE,
        )
        items: list[HouseItem] = []
        for house_id, block in blocks:
            title_m = re.search(r'<div\s+class="house-title">([\s\S]*?)</div>', block, flags=re.IGNORECASE)
            desc_m = re.search(r'<div\s+class="house-desc"[^>]*>([\s\S]*?)</div>', block, flags=re.IGNORECASE)
            total_m = re.search(r'<span\s+class="price-total">([\s\S]*?)</span>', block, flags=re.IGNORECASE)
            unit_m = re.search(r'<span\s+class="price-unit">([\s\S]*?)</span>', block, flags=re.IGNORECASE)
            title = clean_html_text(title_m.group(1)) if title_m else ""
            desc = clean_html_text(desc_m.group(1)) if desc_m else ""
            total = clean_html_text(total_m.group(1)) if total_m else ""
            unit = clean_html_text(unit_m.group(1)) if unit_m else ""
            parts = [p.strip() for p in desc.split("/") if p.strip()]
            community = parts[-1] if parts else ""
            if community.endswith("二手房"):
                community = community[:-3]
            house_info = " / ".join(parts[:-1]) if len(parts) > 1 else desc
            items.append(
                HouseItem(
                    title=title,
                    detail_url=f"https://{self.city}.ke.com/ershoufang/{house_id}.html",
                    area="",
                    community=community,
                    total_price_wan=total,
                    unit_price=unit,
                    house_info=house_info,
                    follow_info="",
                )
            )
        return items

    def _header_search_url(self, keyword: str) -> str:
        from urllib.parse import quote

        return f"https://{self.city}.ke.com/api/headerSearch?keyword={quote(keyword)}"

    def resolve_community_id(self, community: str) -> tuple[str, str, int] | None:
        target = normalize_text(community)
        txt = self.fetch_url(self._header_search_url(community), self._headers_pc())
        results = []
        try:
            data = json.loads(txt)
            results = data.get("data", {}).get("result", [])
        except Exception:
            # Under risk control headerSearch can return non-JSON.
            # Do not guess c-id from HTML to avoid mismatched community ids.
            return None
        picked = None
        for r in results:
            url = str(r.get("url", ""))
            m = re.search(r"/ershoufang/c(\d+)/", url)
            if not m:
                continue
            rid = m.group(1)
            name = clean_html_text(str(r.get("title", "")))
            cnt = int(r.get("count", 0) or 0)
            n = normalize_text(name)
            if n == target:
                return rid, name, cnt
            if picked is None:
                picked = (rid, name, cnt)
        return picked

    def parse_xiaoqu_summary(self, html_text: str) -> tuple[list[HouseItem], int]:
        target_count = 0
        m_count = re.search(r"houseSellNum\"?\s*:\s*(\d+)", html_text)
        if m_count:
            target_count = int(m_count.group(1))

        sample_items: list[HouseItem] = []
        m_list = re.search(r'"ershoufang":(\[[\s\S]*?\]),"ershoufangUrl"', html_text)
        if not m_list:
            return sample_items, target_count
        try:
            arr = json.loads(m_list.group(1))
            for obj in arr:
                house_code = str(obj.get("houseCode", "")).strip()
                sample_items.append(
                    HouseItem(
                        title=clean_html_text(str(obj.get("title", ""))),
                        detail_url=f"https://{self.city}.ke.com/ershoufang/{house_code}.html",
                        area="",
                        community=clean_html_text(str(obj.get("resblockName", ""))),
                        total_price_wan=(str(obj.get("price", "")).strip() + "万") if obj.get("price") else "",
                        unit_price=(str(obj.get("unitPrice", "")).strip() + "元/平") if obj.get("unitPrice") else "",
                        house_info=clean_html_text(str(obj.get("hallNum", ""))),
                        follow_info="",
                    )
                )
        except Exception:
            pass
        return sample_items, target_count

    def parse_items_pc(self, html_text: str) -> list[HouseItem]:
        li_blocks = re.findall(r'<li\s+class="clear"[\s\S]*?</li>', html_text, flags=re.IGNORECASE)
        items: list[HouseItem] = []
        for block in li_blocks:
            title_m = re.search(
                r'<div\s+class="title">\s*<a[^>]*href="([^"]+)"[^>]*>([^<]+)</a>',
                block,
                flags=re.IGNORECASE,
            )
            if not title_m:
                continue

            detail_url = html.unescape(title_m.group(1).strip())
            if detail_url.startswith("/"):
                detail_url = f"https://{self.city}.ke.com{detail_url}"
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
                    area="",
                    community=community,
                    total_price_wan=(clean_html_text(total_m.group(1)) + "万") if total_m else "",
                    unit_price=clean_html_text(unit_m.group(1)) if unit_m else "",
                    house_info=clean_html_text(house_m.group(1)) if house_m else "",
                    follow_info=clean_html_text(follow_m.group(1)) if follow_m else "",
                )
            )
        return items

    @staticmethod
    def _extract_page_meta(html_text: str) -> tuple[int, str]:
        total_pages = 0
        page_tpl = ""
        m_page_data = re.search(r'page-data=["\']([^"\']+)["\']', html_text, flags=re.IGNORECASE)
        if m_page_data:
            try:
                pd = json.loads(html.unescape(m_page_data.group(1)))
                total_pages = int(pd.get("totalPage") or 0)
            except Exception:
                pass
        m_page_url = re.search(r'page-url=["\']([^"\']+)["\']', html_text, flags=re.IGNORECASE)
        if m_page_url:
            page_tpl = html.unescape(m_page_url.group(1)).strip()
        return total_pages, page_tpl

    @staticmethod
    def _extract_total_count(html_text: str) -> int:
        m = re.search(r"共找到\s*(?:<span[^>]*>\s*)?(\d+)(?:\s*</span>)?\s*套", html_text, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))
        m2 = re.search(r"houseSellNum\"?\s*:\s*(\d+)", html_text)
        if m2:
            return int(m2.group(1))
        return 0

    def _page_url_from_tpl(self, tpl: str, page: int) -> str:
        u = tpl
        if "{page}" in u:
            u = u.replace("{page}", str(page))
        if u.startswith("/"):
            return f"https://{self.city}.ke.com{u}"
        if u.startswith("http://") or u.startswith("https://"):
            return u
        return f"https://{self.city}.ke.com/{u.lstrip('/')}"

    @staticmethod
    def dedupe_items(items: list[HouseItem]) -> list[HouseItem]:
        out: list[HouseItem] = []
        seen = set()
        for it in items:
            key = it.detail_url or (it.title + "|" + it.community)
            if key in seen:
                continue
            seen.add(key)
            out.append(it)
        return out

    def crawl_community_direct(
        self,
        community: str,
        max_pages: int = 0,
        fallback_scan: bool = False,
        progress_cb=None,
        item_cb=None,
    ) -> tuple[list[HouseItem], dict]:
        meta = {
            "mode": "direct",
            "community_input": community,
            "community_name": "",
            "resblock_id": "",
            "target_count": 0,
            "direct_blocked": False,
            "fallback_used": False,
            "total_pages": 0,
            "fetched_pages": 0,
        }
        if progress_cb:
            progress_cb(f"正在搜索小区：{community}")
        resolved = self.resolve_community_id(community)
        if not resolved:
            meta["direct_blocked"] = True
            raise RuntimeError("未匹配到小区ID：请确认小区名、城市是否正确，或先在网页确认可检索")

        rid, name, cnt = resolved
        meta["resblock_id"], meta["community_name"], meta["target_count"] = rid, name, cnt

        xq_url = f"https://{self.city}.ke.com/xiaoqu/{rid}/"
        if progress_cb:
            progress_cb(f"已定位小区ID={rid}，拉取小区页摘要")
        xq_html = self.fetch_url(xq_url, self._headers_pc())
        sample_items, xq_cnt = self.parse_xiaoqu_summary(xq_html)
        if xq_cnt:
            meta["target_count"] = xq_cnt

        direct_url = f"https://{self.city}.ke.com/ershoufang/c{rid}/"
        if progress_cb:
            progress_cb("检测小区二手房直连页是否可访问")
        first = self.fetch_url(direct_url, {**self._headers_pc(), "Referer": xq_url})
        is_login = self._is_login_page(first)
        is_captcha = self._is_captcha_page(first)
        if is_login or is_captcha:
            meta["direct_blocked"] = True
            if is_captcha:
                raise RuntimeError(f"直连页被验证码拦截：{direct_url}。请重新登录并完成人机验证后重试")
            raise RuntimeError(f"直连页被登录态拦截：{direct_url}。请同步最新 Cookie 后重试")

        total_pages, page_tpl = self._extract_page_meta(first)
        first_total = self._extract_total_count(first)
        if first_total:
            meta["target_count"] = first_total
        if meta["target_count"] and not total_pages:
            total_pages = int(math.ceil(meta["target_count"] / 30.0))
        if total_pages <= 0:
            total_pages = 1
        if max_pages and max_pages > 0:
            total_pages = min(total_pages, max_pages)
        meta["total_pages"] = total_pages

        out: list[HouseItem] = []
        seen = set()

        def append_unique(page_items: list[HouseItem]) -> list[HouseItem]:
            batch: list[HouseItem] = []
            for it in page_items:
                key = it.detail_url or (it.title + "|" + it.community)
                if key in seen:
                    continue
                seen.add(key)
                out.append(it)
                batch.append(it)
            return batch

        first_items = self.parse_items_pc(first)
        batch = append_unique(first_items)
        meta["fetched_pages"] = 1
        if progress_cb:
            progress_cb(f"在售总数={meta['target_count']}，正在抓取第 1/{total_pages} 页，已获取 {len(out)} 条")
        if item_cb and batch:
            item_cb(batch, len(out) - len(batch) + 1, int(meta["target_count"] or 0))

        for page in range(2, total_pages + 1):
            if page_tpl:
                page_url = self._page_url_from_tpl(page_tpl, page)
            else:
                page_url = f"https://{self.city}.ke.com/ershoufang/pg{page}c{rid}/"
            html_text = self.fetch_url(page_url, {**self._headers_pc(), "Referer": direct_url})
            if self._is_captcha_page(html_text):
                raise RuntimeError(f"第 {page} 页被验证码拦截：{page_url}")
            if self._is_login_page(html_text):
                raise RuntimeError(f"第 {page} 页被登录态拦截：{page_url}")
            page_items = self.parse_items_pc(html_text)
            if not page_items:
                break
            batch = append_unique(page_items)
            meta["fetched_pages"] = page
            if progress_cb:
                progress_cb(f"在售总数={meta['target_count']}，正在抓取第 {page}/{total_pages} 页，已获取 {len(out)} 条")
            if item_cb and batch:
                item_cb(batch, len(out) - len(batch) + 1, int(meta["target_count"] or 0))
            time.sleep(max(self.sleep_sec, 0.2))

        if progress_cb:
            progress_cb(f"抓取完成：在售总数={meta['target_count']}，已获取 {len(out)} 条")
        return out, meta

    def crawl_by_resblock_id(
        self,
        rid: str,
        max_pages: int = 0,
        progress_cb=None,
        item_cb=None,
    ) -> tuple[list[HouseItem], dict]:
        rid = str(rid).strip()
        if not re.fullmatch(r"\d+", rid):
            raise RuntimeError("无效的小区ID（resblock_id）")

        meta = {
            "mode": "direct_url",
            "community_input": "",
            "community_name": "",
            "resblock_id": rid,
            "target_count": 0,
            "direct_blocked": False,
            "fallback_used": False,
            "total_pages": 0,
            "fetched_pages": 0,
        }

        direct_url = f"https://{self.city}.ke.com/ershoufang/c{rid}/"
        xq_url = f"https://{self.city}.ke.com/xiaoqu/{rid}/"
        if progress_cb:
            progress_cb(f"已识别小区ID={rid}，直连抓取中")

        first = self.fetch_url(direct_url, {**self._headers_pc(), "Referer": xq_url})
        is_login = self._is_login_page(first)
        is_captcha = self._is_captcha_page(first)
        if is_login or is_captcha:
            meta["direct_blocked"] = True
            if is_captcha:
                raise RuntimeError(f"直连页被验证码拦截：{direct_url}。请重新登录并完成人机验证后重试")
            raise RuntimeError(f"直连页被登录态拦截：{direct_url}。请同步最新 Cookie 后重试")

        xq_html = self.fetch_url(xq_url, self._headers_pc())
        _, xq_cnt = self.parse_xiaoqu_summary(xq_html)
        if xq_cnt:
            meta["target_count"] = xq_cnt

        total_pages, page_tpl = self._extract_page_meta(first)
        first_total = self._extract_total_count(first)
        if first_total:
            meta["target_count"] = first_total
        if meta["target_count"] and not total_pages:
            total_pages = int(math.ceil(meta["target_count"] / 30.0))
        if total_pages <= 0:
            total_pages = 1
        if max_pages and max_pages > 0:
            total_pages = min(total_pages, max_pages)
        meta["total_pages"] = total_pages

        out: list[HouseItem] = []
        seen = set()

        def append_unique(page_items: list[HouseItem]) -> list[HouseItem]:
            batch: list[HouseItem] = []
            for it in page_items:
                key = it.detail_url or (it.title + "|" + it.community)
                if key in seen:
                    continue
                seen.add(key)
                out.append(it)
                batch.append(it)
            return batch

        first_items = self.parse_items_pc(first)
        if first_items and (not meta["community_name"]):
            meta["community_name"] = first_items[0].community
        batch = append_unique(first_items)
        meta["fetched_pages"] = 1
        if progress_cb:
            progress_cb(f"在售总数={meta['target_count']}，正在抓取第 1/{total_pages} 页，已获取 {len(out)} 条")
        if item_cb and batch:
            item_cb(batch, len(out) - len(batch) + 1, int(meta["target_count"] or 0))

        for page in range(2, total_pages + 1):
            if page_tpl:
                page_url = self._page_url_from_tpl(page_tpl, page)
            else:
                page_url = f"https://{self.city}.ke.com/ershoufang/pg{page}c{rid}/"
            html_text = self.fetch_url(page_url, {**self._headers_pc(), "Referer": direct_url})
            if self._is_captcha_page(html_text):
                raise RuntimeError(f"第 {page} 页被验证码拦截：{page_url}")
            if self._is_login_page(html_text):
                raise RuntimeError(f"第 {page} 页被登录态拦截：{page_url}")
            page_items = self.parse_items_pc(html_text)
            if not page_items:
                break
            if (not meta["community_name"]) and page_items:
                meta["community_name"] = page_items[0].community
            batch = append_unique(page_items)
            meta["fetched_pages"] = page
            if progress_cb:
                progress_cb(f"在售总数={meta['target_count']}，正在抓取第 {page}/{total_pages} 页，已获取 {len(out)} 条")
            if item_cb and batch:
                item_cb(batch, len(out) - len(batch) + 1, int(meta["target_count"] or 0))
            time.sleep(max(self.sleep_sec, 0.2))

        if progress_cb:
            progress_cb(f"抓取完成：在售总数={meta['target_count']}，已获取 {len(out)} 条")
        return out, meta

    def crawl_by_keyword_search(
        self,
        community: str,
        max_pages: int = 0,
        progress_cb=None,
        item_cb=None,
    ) -> tuple[list[HouseItem], dict]:
        from urllib.parse import quote

        keyword = (community or "").strip()
        if not keyword:
            raise RuntimeError("小区名称不能为空")
        target = normalize_text(keyword)
        search_url = f"https://{self.city}.ke.com/ershoufang/rs{quote(keyword)}/"
        first = self.fetch_url(search_url, self._headers_pc())
        if self._is_captcha_page(first):
            raise RuntimeError(f"关键词搜索页被验证码拦截：{search_url}")
        if self._is_login_page(first):
            raise RuntimeError(f"关键词搜索页被登录态拦截：{search_url}")

        total_pages, page_tpl = self._extract_page_meta(first)
        total = self._extract_total_count(first)
        if total_pages <= 0 and total > 0:
            total_pages = int(math.ceil(total / 30.0))
        if total_pages <= 0:
            total_pages = 1
        if max_pages and max_pages > 0:
            total_pages = min(total_pages, max_pages)

        meta = {
            "mode": "keyword_rs",
            "community_input": community,
            "community_name": community,
            "resblock_id": "",
            "target_count": total,
            "direct_blocked": False,
            "fallback_used": False,
            "total_pages": total_pages,
            "fetched_pages": 0,
        }

        out: list[HouseItem] = []
        seen = set()

        def append_filtered(items: list[HouseItem]) -> list[HouseItem]:
            batch: list[HouseItem] = []
            for it in items:
                comm = normalize_text(it.community)
                if not (comm == target or target in comm or comm in target):
                    continue
                key = it.detail_url or (it.title + "|" + it.community)
                if key in seen:
                    continue
                seen.add(key)
                out.append(it)
                batch.append(it)
            return batch

        first_items = self.parse_items_pc(first)
        batch = append_filtered(first_items)
        meta["fetched_pages"] = 1
        if progress_cb:
            progress_cb(f"[RS] 正在抓取第 1/{total_pages} 页，已筛得 {len(out)} 条")
        if item_cb and batch:
            item_cb(batch, len(out) - len(batch) + 1, int(meta["target_count"] or 0))

        for page in range(2, total_pages + 1):
            if page_tpl:
                page_url = self._page_url_from_tpl(page_tpl, page)
            else:
                page_url = f"https://{self.city}.ke.com/ershoufang/pg{page}rs{quote(keyword)}/"
            html_text = self.fetch_url(page_url, {**self._headers_pc(), "Referer": search_url})
            if self._is_captcha_page(html_text):
                raise RuntimeError(f"[RS] 第 {page} 页被验证码拦截：{page_url}")
            if self._is_login_page(html_text):
                raise RuntimeError(f"[RS] 第 {page} 页被登录态拦截：{page_url}")
            page_items = self.parse_items_pc(html_text)
            if not page_items:
                break
            batch = append_filtered(page_items)
            meta["fetched_pages"] = page
            if progress_cb:
                progress_cb(f"[RS] 正在抓取第 {page}/{total_pages} 页，已筛得 {len(out)} 条")
            if item_cb and batch:
                item_cb(batch, len(out) - len(batch) + 1, int(meta["target_count"] or 0))
            time.sleep(max(self.sleep_sec, 0.2))

        if progress_cb:
            progress_cb(f"[RS] 抓取完成：筛得 {len(out)} 条")
        return out, meta


class BeikeGuiApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("贝壳二手房-按小区全量抓取")
        self.root.geometry("1250x700")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self.results: list[HouseItem] = []
        self.searching = False
        self.direct_meta: dict | None = None
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None
        self._config = {"cookies": {}}
        self._last_city = "hz"
        self._last_request: dict | None = None
        self._pending_retry: bool = False

        self.city_var = tk.StringVar(value="hz")
        self.community_var = tk.StringVar(value="蓝城春风燕语")
        self.cookie_var = tk.StringVar(value="")
        self.status_var = tk.StringVar(value="准备就绪")
        self.use_pw_fetch_var = tk.BooleanVar(value=False)
        self.community_history: list[str] = []

        self._build_form()
        self._build_table()
        self._build_status_bar()
        self._load_local_config()
        self.city_var.trace_add("write", self._on_city_change)
        self._apply_saved_cookie(self.city_var.get().strip().lower())

    def _build_form(self) -> None:
        frm = ttk.Frame(self.root, padding=10)
        frm.pack(fill=tk.X)

        ttk.Label(frm, text="城市简拼").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(frm, textvariable=self.city_var, width=10).grid(row=0, column=1, padx=(6, 16))

        ttk.Label(frm, text="小区名称").grid(row=0, column=2, sticky=tk.W)
        self.community_combo = ttk.Combobox(frm, textvariable=self.community_var, width=34)
        self.community_combo.grid(row=0, column=3, padx=(6, 16))

        self.fetch_btn = ttk.Button(frm, text="抓取该小区全部在售", command=self.start_fetch)
        self.fetch_btn.grid(row=0, column=4, padx=(0, 8))

        self.test_cookie_btn = ttk.Button(frm, text="Cookie有效性测试", command=self.start_cookie_test)
        self.test_cookie_btn.grid(row=0, column=5, padx=(0, 8))

        self.export_btn = ttk.Button(frm, text="导出CSV", command=self.export_csv)
        self.export_btn.grid(row=0, column=6)

        ttk.Checkbutton(frm, text="浏览器会话抓取(实验)", variable=self.use_pw_fetch_var).grid(
            row=0, column=7, sticky=tk.W
        )

        ttk.Label(frm, text="Cookie(可选)").grid(row=1, column=0, sticky=tk.W, pady=(8, 0))
        ttk.Entry(frm, textvariable=self.cookie_var, width=108).grid(
            row=1, column=1, columnspan=6, sticky=tk.W, padx=(6, 0), pady=(8, 0)
        )

        self.open_login_btn = ttk.Button(frm, text="1.打开登录浏览器", command=self.open_login_browser)
        self.open_login_btn.grid(row=2, column=1, sticky=tk.W, padx=(6, 8), pady=(8, 0))

        self.open_system_btn = ttk.Button(frm, text="系统浏览器验证(仅手工拷Cookie)", command=self.open_system_browser_verify)
        self.open_system_btn.grid(row=2, column=2, sticky=tk.W, padx=(0, 8), pady=(8, 0))

        self.open_internal_verify_btn = ttk.Button(frm, text="内置验证并重试", command=self.open_internal_verify_and_retry)
        self.open_internal_verify_btn.grid(row=2, column=3, sticky=tk.W, padx=(0, 8), pady=(8, 0))

        self.sync_cookie_btn = ttk.Button(frm, text="2.同步Cookie", command=self.sync_cookie_from_browser)
        self.sync_cookie_btn.grid(row=2, column=4, sticky=tk.W, pady=(8, 0))

        self.sync_retry_btn = ttk.Button(frm, text="同步Cookie并重试", command=self.sync_cookie_and_retry)
        self.sync_retry_btn.grid(row=2, column=5, sticky=tk.W, padx=(8, 0), pady=(8, 0))

        self.close_login_btn = ttk.Button(frm, text="3.关闭登录浏览器", command=self.close_login_browser)
        self.close_login_btn.grid(row=2, column=6, sticky=tk.W, padx=(8, 0), pady=(8, 0))

        self.save_cookie_btn = ttk.Button(frm, text="保存当前Cookie", command=self.save_cookie_for_city)
        self.save_cookie_btn.grid(row=2, column=7, sticky=tk.W, padx=(8, 0), pady=(8, 0))

        self.clear_cookie_btn = ttk.Button(frm, text="清除城市Cookie", command=self.clear_cookie_for_city)
        self.clear_cookie_btn.grid(row=2, column=8, sticky=tk.W, padx=(8, 0), pady=(8, 0))

    def _build_table(self) -> None:
        table_wrap = ttk.Frame(self.root, padding=(10, 0, 10, 10))
        table_wrap.pack(fill=tk.BOTH, expand=True)
        columns = ["seq_no", "title", "community", "total_price_wan", "unit_price", "house_info", "follow_info", "detail_url"]
        self.tree = ttk.Treeview(table_wrap, columns=columns, show="headings")
        headers = {
            "seq_no": "序号",
            "title": "标题",
            "community": "小区",
            "total_price_wan": "总价",
            "unit_price": "单价",
            "house_info": "户型信息",
            "follow_info": "关注信息",
            "detail_url": "链接",
        }
        widths = {
            "seq_no": 70,
            "title": 260,
            "community": 120,
            "total_price_wan": 90,
            "unit_price": 110,
            "house_info": 300,
            "follow_info": 150,
            "detail_url": 260,
        }
        for col in columns:
            self.tree.heading(col, text=headers[col])
            self.tree.column(col, width=widths[col], anchor=tk.W)
        ysb = ttk.Scrollbar(table_wrap, orient=tk.VERTICAL, command=self.tree.yview)
        xsb = ttk.Scrollbar(table_wrap, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        ysb.grid(row=0, column=1, sticky="ns")
        xsb.grid(row=1, column=0, sticky="ew")
        table_wrap.rowconfigure(0, weight=1)
        table_wrap.columnconfigure(0, weight=1)
        self.tree.bind("<Double-1>", self.open_selected_link)

    def _build_status_bar(self) -> None:
        bar = ttk.Frame(self.root, padding=(10, 0, 10, 10))
        bar.pack(fill=tk.X)
        ttk.Label(bar, textvariable=self.status_var).pack(side=tk.LEFT)

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _safe_city(city: str) -> str:
        return (city or "").strip().lower()

    @staticmethod
    def _is_cookie_string_valid(cookie: str) -> tuple[bool, str]:
        s = (cookie or "").strip()
        if not s:
            return False, "Cookie 为空"
        if "=" not in s:
            return False, "Cookie 缺少 '='，看起来不是 name=value 格式"
        parts = [p.strip() for p in s.split(";") if p.strip()]
        if len(parts) < 3:
            return False, "Cookie 项太少，建议从 Network 的 request headers 复制完整 cookie 串"
        bad = []
        for p in parts[:8]:
            if "=" not in p:
                bad.append(p)
        if bad:
            return False, "Cookie 中存在非 name=value 项"
        return True, ""

    def _config_path(self) -> Path:
        return Path(__file__).resolve().parents[1] / "03-配置" / "gui_cookie_config.json"

    def _state_path(self, city: str) -> Path:
        c = self._safe_city(city) or "hz"
        return Path(__file__).resolve().parents[1] / "03-配置" / f"playwright_state_{c}.json"

    def _has_state(self, city: str) -> bool:
        return self._state_path(city).exists()

    def _load_local_config(self) -> None:
        p = self._config_path()
        old_p = Path(__file__).resolve().parents[1] / "02-数据库" / "gui_cookie_config.json"
        try:
            if (not p.exists()) and old_p.exists():
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(old_p.read_text(encoding="utf-8"), encoding="utf-8")
            if p.exists():
                self._config = json.loads(p.read_text(encoding="utf-8"))
            else:
                p.parent.mkdir(parents=True, exist_ok=True)
                self._config = {"cookies": {}}
        except Exception:
            self._config = {"cookies": {}}
        if not isinstance(self._config, dict):
            self._config = {"cookies": {}}
        if "cookies" not in self._config or not isinstance(self._config.get("cookies"), dict):
            self._config["cookies"] = {}
        if "community_history" not in self._config or not isinstance(self._config.get("community_history"), list):
            self._config["community_history"] = []
        if "last_community" not in self._config:
            self._config["last_community"] = "蓝城春风燕语"
        self.community_history = [str(x).strip() for x in self._config.get("community_history", []) if str(x).strip()]
        if not self.community_history:
            self.community_history = ["蓝城春风燕语"]
        self.community_combo["values"] = self.community_history
        last_community = str(self._config.get("last_community", "")).strip()
        self.community_var.set(last_community or "蓝城春风燕语")

    def _save_local_config(self) -> None:
        p = self._config_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self._config, ensure_ascii=False, indent=2), encoding="utf-8")

    def _remember_community(self, community: str) -> None:
        val = (community or "").strip()
        if not val:
            return
        history = [x for x in self.community_history if x != val]
        history.insert(0, val)
        history = history[:20]
        self.community_history = history
        self.community_combo["values"] = self.community_history
        self._config["community_history"] = history
        self._config["last_community"] = val
        self._save_local_config()

    def _get_saved_cookie(self, city: str) -> str:
        c = self._safe_city(city)
        if not c:
            return ""
        node = self._config.get("cookies", {}).get(c, {})
        if not isinstance(node, dict):
            return ""
        return str(node.get("cookie", "")).strip()

    def _set_saved_cookie(self, city: str, cookie: str) -> None:
        c = self._safe_city(city)
        if not c:
            return
        self._config.setdefault("cookies", {})
        self._config["cookies"][c] = {
            "cookie": cookie.strip(),
            "updated_at": self._now_text(),
        }
        self._save_local_config()

    def _remove_saved_cookie(self, city: str) -> None:
        c = self._safe_city(city)
        if not c:
            return
        cookies = self._config.setdefault("cookies", {})
        if c in cookies:
            del cookies[c]
            self._save_local_config()

    def _apply_saved_cookie(self, city: str) -> None:
        c = self._safe_city(city)
        if not c:
            self.cookie_var.set("")
            self._last_city = c
            return
        saved = self._get_saved_cookie(c)
        self.cookie_var.set(saved)
        self._last_city = c
        if saved:
            self.status_var.set(f"已载入本地 Cookie：城市 {c}")
        else:
            self.status_var.set(f"城市 {c} 暂无本地 Cookie，请先同步或手工填写")

    def _on_city_change(self, *_args) -> None:
        c = self._safe_city(self.city_var.get())
        if c == self._last_city:
            return
        self._apply_saved_cookie(c)

    def _close_browser_session(self) -> None:
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._pw:
                self._pw.stop()
        except Exception:
            pass
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    def _on_close(self) -> None:
        self._close_browser_session()
        self.root.destroy()

    def open_login_browser(self, target_url: str = "") -> None:
        city = self.city_var.get().strip().lower()
        if not city:
            messagebox.showerror("参数错误", "请先填写城市简拼，例如 hz")
            return
        if self._context is not None:
            if target_url and self._page is not None:
                try:
                    self._page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                except Exception:
                    pass
                self.status_var.set("已跳转到验证页面，请在浏览器完成验证")
            else:
                messagebox.showinfo("提示", "登录浏览器已打开。请完成登录/验证后点击“2.同步Cookie”。")
            return

        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            messagebox.showerror(
                "缺少依赖",
                "未检测到 Playwright。\n请先执行：\npy -3 -m pip install playwright\npy -3 -m playwright install chromium",
            )
            return

        try:
            self._pw = sync_playwright().start()
            self._browser = self._pw.chromium.launch(headless=False)
            self._context = self._browser.new_context()
            self._page = self._context.new_page()
            nav_url = target_url or f"https://{city}.ke.com/"
            self._page.goto(nav_url, wait_until="domcontentloaded", timeout=30000)
            self.status_var.set("已打开登录浏览器：请手动登录并完成人机验证，然后点击“2.同步Cookie”")
            messagebox.showinfo(
                "登录提示",
                "请在弹出的浏览器中完成登录与人机验证。\n完成后回到本程序，点击“2.同步Cookie”。",
            )
        except Exception as exc:
            self._close_browser_session()
            messagebox.showerror("打开浏览器失败", str(exc))

    def open_system_browser_verify(self, target_url: str = "") -> None:
        city = self.city_var.get().strip().lower() or "hz"
        url = target_url or f"https://{city}.ke.com/"
        webbrowser.open(url)
        self.status_var.set("已打开系统浏览器，请在系统浏览器完成登录/验证后，将Cookie粘贴回本程序并重试")
        messagebox.showinfo(
            "系统浏览器验证",
            "已打开系统浏览器。\n请在系统浏览器完成人机验证，然后把可用Cookie粘贴到本程序，再点击“同步Cookie并重试”。",
        )

    def open_internal_verify_and_retry(self) -> None:
        city = self.city_var.get().strip().lower() or "hz"
        community = self.community_var.get().strip()
        if not community:
            messagebox.showerror("参数错误", "请先填写小区名称")
            return
        from urllib.parse import quote

        target_url = f"https://{city}.ke.com/ershoufang/rs{quote(community)}/"
        self._pending_retry = True
        self._last_request = {
            "city": city,
            "community": community,
            "mode": "community",
            "rid": "",
            "url": target_url,
        }
        self.open_login_browser(target_url=target_url)
        self.status_var.set("已打开内置验证窗口：完成人工验证后，点击“同步Cookie并重试”")

    def close_login_browser(self) -> None:
        self._close_browser_session()
        self.status_var.set("登录浏览器已关闭")

    def sync_cookie_from_browser(self) -> bool:
        city = self.city_var.get().strip().lower() or "hz"
        manual_cookie = self.cookie_var.get().strip()
        if self._context is None:
            if not manual_cookie:
                messagebox.showerror("操作顺序错误", "未检测到登录浏览器会话，且当前 Cookie 为空。请先粘贴 Cookie 或打开登录浏览器。")
                return False
            ok, reason = self._is_cookie_string_valid(manual_cookie)
            if not ok:
                messagebox.showerror("Cookie 格式错误", f"{reason}\n\n请复制整条 request headers 里的 cookie 值后重试。")
                return False
            self._set_saved_cookie(city, manual_cookie)
            self.status_var.set("已保存手工粘贴的 Cookie，可直接抓取")
            return True
        try:
            self.status_var.set("正在同步 Cookie，请稍候...")
            self.root.update_idletasks()
            state_saved = False
            try:
                self._state_path(city).parent.mkdir(parents=True, exist_ok=True)
                self._context.storage_state(path=str(self._state_path(city)))
                state_saved = True
            except Exception:
                state_saved = False
            cookies = self._context.cookies([f"https://{city}.ke.com/", "https://ke.com/"])
            pairs = []
            seen = set()
            for c in cookies:
                domain = str(c.get("domain", ""))
                name = str(c.get("name", "")).strip()
                value = str(c.get("value", ""))
                if (not name) or (".ke.com" not in domain and "ke.com" not in domain):
                    continue
                if name in seen:
                    continue
                seen.add(name)
                pairs.append(f"{name}={value}")

            if not pairs and (not state_saved):
                messagebox.showwarning("未获取到会话", "未提取到 Cookie 且未保存到 state，请确认已登录后重试。")
                return False

            if pairs:
                cookie = "; ".join(pairs)
                self.cookie_var.set(cookie)
                self._set_saved_cookie(city, cookie)
            if state_saved:
                self.status_var.set(f"登录态已同步（state已保存，cookie项={len(pairs)}）。现在可直接抓取。")
            else:
                self.status_var.set(f"Cookie 已同步（{len(pairs)}项）。现在可直接抓取。")
            return True
        except Exception as exc:
            messagebox.showerror("同步Cookie失败", str(exc))
            return False

    def sync_cookie_and_retry(self) -> None:
        if not self._pending_retry or not self._last_request:
            messagebox.showinfo("提示", "当前没有待重试任务。")
            return
        city = self.city_var.get().strip().lower() or "hz"
        ok = True
        if self._context is not None:
            ok = self.sync_cookie_from_browser()
            if not ok:
                return
        elif (not self.cookie_var.get().strip()) and (not self._has_state(city)):
            messagebox.showerror("重试失败", "当前没有可用Cookie且无登录态state。请先做“内置验证并重试”。")
            return
        req = self._last_request.copy()
        self._pending_retry = False
        self._retry_after_cooldown(req, self.cookie_var.get().strip(), cooldown_sec=12)

    def _retry_after_cooldown(self, req: dict, cookie: str, cooldown_sec: int = 12) -> None:
        def worker() -> None:
            for left in range(max(cooldown_sec, 0), 0, -1):
                self.root.after(0, lambda n=left: self.status_var.set(f"验证已完成，冷却中 {n}s 后自动重试..."))
                time.sleep(1)
            self.root.after(
                0,
                lambda: self._start_fetch_request(
                    req.get("city", ""),
                    req.get("community", ""),
                    cookie,
                    mode=req.get("mode", "community"),
                    rid=req.get("rid", ""),
                    url=req.get("url", ""),
                    remember=False,
                ),
            )

        threading.Thread(target=worker, daemon=True).start()

    def save_cookie_for_city(self) -> None:
        city = self._safe_city(self.city_var.get())
        cookie = self.cookie_var.get().strip()
        if not city:
            messagebox.showerror("参数错误", "城市简拼不能为空")
            return
        if not cookie:
            messagebox.showerror("参数错误", "Cookie 为空，无法保存")
            return
        ok, reason = self._is_cookie_string_valid(cookie)
        if not ok:
            messagebox.showerror("Cookie 格式错误", f"{reason}\n\n请复制整条 request headers 里的 cookie 值后再保存。")
            return
        self._set_saved_cookie(city, cookie)
        self.status_var.set(f"已保存 Cookie（城市 {city}）")

    def clear_cookie_for_city(self) -> None:
        city = self._safe_city(self.city_var.get())
        if not city:
            messagebox.showerror("参数错误", "城市简拼不能为空")
            return
        self._remove_saved_cookie(city)
        self.cookie_var.set("")
        self.status_var.set(f"已清除城市 {city} 的本地 Cookie")

    def start_cookie_test(self) -> None:
        if self.searching:
            messagebox.showinfo("提示", "当前正在抓取，请稍后再测试")
            return
        city = self.city_var.get().strip().lower()
        community = self.community_var.get().strip()
        cookie = self.cookie_var.get().strip()
        use_pw = self.use_pw_fetch_var.get()
        if not city:
            messagebox.showerror("参数错误", "城市简拼不能为空，例如 hz")
            return
        if not community:
            messagebox.showerror("参数错误", "小区名称不能为空，例如 蓝城春风燕语")
            return
        if use_pw and self._has_state(city):
            cookie = ""
        else:
            if not cookie:
                messagebox.showerror("参数错误", "Cookie 为空，请先粘贴或同步 Cookie；或先做内置验证保存state。")
                return
            ok, reason = self._is_cookie_string_valid(cookie)
            if not ok:
                messagebox.showerror("Cookie 格式错误", f"{reason}\n\n请复制整条 request headers 里的 cookie 值后重试。")
                return

        self.status_var.set("正在测试 Cookie 有效性...")
        self.test_cookie_btn.configure(state=tk.DISABLED)
        t = threading.Thread(
            target=self._run_cookie_test,
            args=(city, community, cookie, use_pw),
            daemon=True,
        )
        t.start()

    def _run_cookie_test(self, city: str, community: str, cookie: str, use_pw: bool) -> None:
        state_path = self._state_path(city)
        spider = BeikeSpider(
            city=city,
            timeout=20,
            sleep_sec=0.1,
            cookie=cookie,
            use_playwright=use_pw,
            storage_state_path=str(state_path) if state_path.exists() else "",
            pw_headless=(not use_pw),
        )
        result = {"ok": False, "title": "Cookie测试", "msg": ""}
        cookie_items = len([p for p in cookie.split(";") if p.strip()])
        cookie_len = len(cookie)
        try:
            from urllib.parse import quote

            rs_url = f"https://{city}.ke.com/ershoufang/rs{quote(community)}/"
            rs_html = spider.fetch_url(rs_url, spider._headers_pc())
            rs_captcha = spider._is_captcha_page(rs_html)
            rs_login = spider._is_login_page(rs_html)
            rs_items = [] if (rs_captcha or rs_login) else spider.parse_items_pc(rs_html)
            rs_total = 0 if (rs_captcha or rs_login) else spider._extract_total_count(rs_html)

            resolved = spider.resolve_community_id(community)
            c_state = "未测试"
            c_url = ""
            if resolved:
                rid, name, _ = resolved
                c_url = f"https://{city}.ke.com/ershoufang/c{rid}/"
                xq_url = f"https://{city}.ke.com/xiaoqu/{rid}/"
                c_html = spider.fetch_url(c_url, {**spider._headers_pc(), "Referer": xq_url})
                if spider._is_captcha_page(c_html):
                    c_state = "验证码页"
                elif spider._is_login_page(c_html):
                    c_state = "登录页"
                else:
                    c_state = f"正常列表页(页内{len(spider.parse_items_pc(c_html))}条)"
            else:
                name = community

            if (not rs_captcha) and (not rs_login):
                result["ok"] = True
                result["title"] = "Cookie有效(RS可用)"
                result["msg"] = (
                    f"RS检测：正常列表页\n小区：{name}\nRS在售总数(页内识别)：{rs_total}\n"
                    f"RS第一页房源条数：{len(rs_items)}\n"
                    f"C直连检测：{c_state}\nC直连URL：{c_url or '-'}\n"
                    f"Cookie长度={cookie_len}，条目数={cookie_items}"
                )
            else:
                city_url = f"https://{city}.ke.com/ershoufang/"
                city_html = spider.fetch_url(city_url, spider._headers_pc())
                if spider._is_captcha_page(city_html):
                    result["title"] = "需要人工验证"
                    result["msg"] = (
                        f"检测结果：验证码页（全站）\nRS检测={'验证码页' if rs_captcha else '登录页'}\n"
                        f"小区：{name}\nRS URL：{rs_url}\n"
                        f"Cookie长度={cookie_len}，条目数={cookie_items}"
                    )
                else:
                    result["title"] = "需要人工验证"
                    result["msg"] = (
                        f"检测结果：仅搜索路径受限\nRS检测={'验证码页' if rs_captcha else '登录页'}\n"
                        f"小区：{name}\nRS URL：{rs_url}\n"
                        f"Cookie长度={cookie_len}，条目数={cookie_items}"
                    )
        except Exception as exc:
            result["title"] = "Cookie测试失败"
            result["msg"] = f"网络/请求异常：{exc}\nCookie长度={cookie_len}，条目数={cookie_items}"
        finally:
            spider.close()
            self.root.after(0, lambda r=result: self._on_cookie_test_done(r))

    def _on_cookie_test_done(self, result: dict) -> None:
        self.test_cookie_btn.configure(state=tk.NORMAL)
        if result.get("ok"):
            self.status_var.set("Cookie测试通过：可访问正常列表页")
            messagebox.showinfo(result.get("title", "Cookie测试"), result.get("msg", ""))
        else:
            self.status_var.set("Cookie测试未通过")
            messagebox.showwarning(result.get("title", "Cookie测试"), result.get("msg", ""))

    def start_fetch(self) -> None:
        if self.searching:
            return

        city = self.city_var.get().strip().lower()
        community = self.community_var.get().strip()
        cookie = self.cookie_var.get().strip()
        use_pw = self.use_pw_fetch_var.get()
        if not city:
            messagebox.showerror("参数错误", "城市简拼不能为空，例如 hz")
            return
        if not community:
            messagebox.showerror("参数错误", "小区名称不能为空，例如 蓝城春风燕语")
            return
        if use_pw and self._has_state(city):
            cookie = ""
        elif cookie:
            ok, reason = self._is_cookie_string_valid(cookie)
            if not ok:
                messagebox.showerror("Cookie 格式错误", f"{reason}\n\n请先修正 Cookie 再抓取。")
                return
        elif not use_pw:
            messagebox.showerror("参数错误", "当前未启用浏览器会话抓取，Cookie 不能为空。")
            return
        self._remember_community(community)
        self._start_fetch_request(city, community, cookie, mode="community", rid="", url="", remember=False)

    def _start_fetch_request(
        self,
        city: str,
        community: str,
        cookie: str,
        mode: str = "community",
        rid: str = "",
        url: str = "",
        remember: bool = False,
    ) -> None:
        if remember:
            self._remember_community(community)
        self._last_request = {"city": city, "community": community, "mode": mode, "rid": rid, "url": url}
        self._pending_retry = False

        self.searching = True
        self.direct_meta = None
        self.results = []
        self._render_table([])
        self.fetch_btn.configure(state=tk.DISABLED)
        self.status_var.set("正在查询小区并准备分页抓取...")

        t = threading.Thread(
            target=self._run_fetch,
            args=(city, community, cookie, mode, rid),
            daemon=True,
        )
        t.start()

    def _run_fetch(self, city: str, community: str, cookie: str, mode: str, rid: str) -> None:
        state_path = self._state_path(city)
        use_pw = self.use_pw_fetch_var.get()
        spider = BeikeSpider(
            city=city,
            sleep_sec=0.6,
            cookie=cookie,
            use_playwright=use_pw,
            storage_state_path=str(state_path) if state_path.exists() else "",
        )
        try:
            progress_cb = lambda msg: self.root.after(
                0, lambda m=msg: self.status_var.set(f"[{'PW' if use_pw else 'HTTP'}] {m}")
            )
            item_cb = lambda batch, start, total: self.root.after(
                0, lambda b=batch, s=start, t=total: self._append_batch_rows(b, s, t)
            )
            if mode == "url":
                items, meta = spider.crawl_by_resblock_id(rid=rid, max_pages=0, progress_cb=progress_cb, item_cb=item_cb)
            else:
                try:
                    progress_cb("优先尝试RS关键词页抓取...")
                    items, meta = spider.crawl_by_keyword_search(
                        community=community, max_pages=0, progress_cb=progress_cb, item_cb=item_cb
                    )
                except Exception:
                    progress_cb("RS关键词页不可用，尝试C直连路径...")
                    items, meta = spider.crawl_community_direct(
                        community=community, max_pages=0, fallback_scan=False, progress_cb=progress_cb, item_cb=item_cb
                    )
            self.root.after(0, lambda: self._on_direct_info(meta))
            self.root.after(0, lambda: self._on_fetch_done(items, community))
        except Exception as exc:
            err_msg = str(exc)
            self.root.after(0, lambda m=err_msg: self._on_search_error(m))
        finally:
            spider.close()

    def _on_fetch_done(self, items: list[HouseItem], community: str) -> None:
        self.results = items
        self._render_table(items)

        if self.direct_meta:
            rid = self.direct_meta.get("resblock_id") or "-"
            target = self.direct_meta.get("target_count") or 0
            blocked = self.direct_meta.get("direct_blocked")
            total_pages = self.direct_meta.get("total_pages") or 0
            fetched_pages = self.direct_meta.get("fetched_pages") or 0
            name = self.direct_meta.get("community_name") or community
            self.status_var.set(
                f"抓取完成：{len(items)} 条，小区={name}，resblock_id={rid}，在售总数={target}，"
                f"分页={fetched_pages}/{total_pages}，直连拦截={blocked}"
            )
        else:
            self.status_var.set(f"抓取完成：共 {len(items)} 条（小区：{community}）")

        self.searching = False
        self.fetch_btn.configure(state=tk.NORMAL)

    def _on_direct_info(self, meta: dict) -> None:
        self.direct_meta = meta

    def _on_search_error(self, err_msg: str) -> None:
        self.searching = False
        self.fetch_btn.configure(state=tk.NORMAL)
        self.status_var.set("搜索失败")
        need_verify = ("验证码拦截" in err_msg) or ("登录态拦截" in err_msg)
        if not need_verify:
            messagebox.showerror("搜索失败", err_msg)
            return

        self._pending_retry = True
        url_m = re.search(r"(https?://\S+)", err_msg)
        blocked_url = url_m.group(1) if url_m else ""
        ask = messagebox.askyesno(
            "需要人工验证",
            f"{err_msg}\n\n是否现在打开系统浏览器并跳到该页面完成人机验证？\n验证后把Cookie粘贴到输入框，再点“同步Cookie并重试”。",
        )
        if ask:
            self.open_system_browser_verify(blocked_url)
            self.status_var.set("请在系统浏览器完成人机验证，并粘贴Cookie后点击“同步Cookie并重试”")

    def _render_table(self, items: list[HouseItem]) -> None:
        for old in self.tree.get_children():
            self.tree.delete(old)
        for idx, item in enumerate(items, start=1):
            self.tree.insert(
                "",
                tk.END,
                values=(
                    idx,
                    item.title,
                    item.community,
                    item.total_price_wan,
                    item.unit_price,
                    item.house_info,
                    item.follow_info,
                    item.detail_url,
                ),
            )

    def _append_batch_rows(self, items: list[HouseItem], start_seq: int, total_count: int) -> None:
        for offset, item in enumerate(items):
            self.results.append(item)
            seq = start_seq + offset
            self.tree.insert(
                "",
                tk.END,
                values=(
                    seq,
                    item.title,
                    item.community,
                    item.total_price_wan,
                    item.unit_price,
                    item.house_info,
                    item.follow_info,
                    item.detail_url,
                ),
            )
        if total_count:
            self.status_var.set(f"在售总数={total_count}，已显示 {len(self.results)} 条")

    def export_csv(self) -> None:
        if not self.results:
            messagebox.showinfo("提示", "当前没有可导出的数据")
            return
        base_dir = Path(__file__).resolve().parents[1] / "02-数据库"
        base_dir.mkdir(parents=True, exist_ok=True)
        out_path = filedialog.asksaveasfilename(
            title="导出CSV",
            defaultextension=".csv",
            initialdir=str(base_dir),
            initialfile="beike_search_result.csv",
            filetypes=[("CSV 文件", "*.csv")],
        )
        if not out_path:
            return
        rows = [asdict(i) for i in self.results]
        with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        self.status_var.set(f"已导出: {out_path}（{len(rows)} 条）")

    def open_selected_link(self, _event: tk.Event) -> None:
        selected = self.tree.selection()
        if not selected:
            return
        values = self.tree.item(selected[0], "values")
        if len(values) < 8:
            return
        if values[7]:
            webbrowser.open(values[7])


def main() -> None:
    root = tk.Tk()
    BeikeGuiApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
