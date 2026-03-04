#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import threading
import tkinter as tk
import webbrowser
from difflib import SequenceMatcher
from dataclasses import asdict
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from beike_ershoufang_spider import BeikeSpider, HouseItem


class BeikeGuiApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("贝壳二手房搜索")
        self.root.geometry("1250x700")

        self.results: list[HouseItem] = []
        self.searching = False

        self.city_var = tk.StringVar(value="sh")
        self.pages_var = tk.StringVar(value="3")
        self.sleep_var = tk.StringVar(value="1.2")
        self.keyword_var = tk.StringVar(value="")
        self.status_var = tk.StringVar(value="准备就绪")

        self._build_form()
        self._build_table()
        self._build_status_bar()

    def _build_form(self) -> None:
        frm = ttk.Frame(self.root, padding=10)
        frm.pack(fill=tk.X)

        ttk.Label(frm, text="城市简拼").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(frm, textvariable=self.city_var, width=10).grid(row=0, column=1, padx=(6, 16))

        ttk.Label(frm, text="抓取页数").grid(row=0, column=2, sticky=tk.W)
        ttk.Entry(frm, textvariable=self.pages_var, width=8).grid(row=0, column=3, padx=(6, 16))

        ttk.Label(frm, text="间隔(秒)").grid(row=0, column=4, sticky=tk.W)
        ttk.Entry(frm, textvariable=self.sleep_var, width=8).grid(row=0, column=5, padx=(6, 16))

        ttk.Label(frm, text="小区关键词").grid(row=0, column=6, sticky=tk.W)
        ttk.Entry(frm, textvariable=self.keyword_var, width=24).grid(row=0, column=7, padx=(6, 16))

        self.search_btn = ttk.Button(frm, text="开始搜索", command=self.start_search)
        self.search_btn.grid(row=0, column=8, padx=(0, 8))

        self.export_btn = ttk.Button(frm, text="导出CSV", command=self.export_csv)
        self.export_btn.grid(row=0, column=9)

    def _build_table(self) -> None:
        table_wrap = ttk.Frame(self.root, padding=(10, 0, 10, 10))
        table_wrap.pack(fill=tk.BOTH, expand=True)

        columns = [
            "title",
            "community",
            "total_price_wan",
            "unit_price",
            "house_info",
            "follow_info",
            "detail_url",
        ]
        self.tree = ttk.Treeview(table_wrap, columns=columns, show="headings")

        headers = {
            "title": "标题",
            "community": "小区",
            "total_price_wan": "总价",
            "unit_price": "单价",
            "house_info": "户型信息",
            "follow_info": "关注信息",
            "detail_url": "链接",
        }
        widths = {
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

    def start_search(self) -> None:
        if self.searching:
            return

        city = self.city_var.get().strip().lower()
        keyword = self.keyword_var.get().strip()

        try:
            pages = int(self.pages_var.get().strip())
            sleep_sec = float(self.sleep_var.get().strip())
            if pages < 1:
                raise ValueError
            if sleep_sec < 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("参数错误", "请检查抓取页数和间隔秒数（页数>=1，间隔>=0）")
            return

        if not city:
            messagebox.showerror("参数错误", "城市简拼不能为空，例如 sh 或 bj")
            return

        self.searching = True
        self.search_btn.configure(state=tk.DISABLED)
        self.status_var.set("抓取中，请稍候...")

        t = threading.Thread(
            target=self._run_search,
            args=(city, pages, sleep_sec, keyword),
            daemon=True,
        )
        t.start()

    def _run_search(self, city: str, pages: int, sleep_sec: float, keyword: str) -> None:
        try:
            spider = BeikeSpider(city=city, sleep_sec=sleep_sec)
            items = spider.crawl(pages=pages)
            if keyword:
                filtered = self._filter_items(items, keyword)
            else:
                filtered = items
            self.root.after(0, lambda: self._on_search_done(items, filtered, keyword))
        except Exception as exc:  # noqa: BLE001
            self.root.after(0, lambda: self._on_search_error(str(exc)))

    def _on_search_done(self, all_items: list[HouseItem], items: list[HouseItem], keyword: str) -> None:
        self.results = items
        self._render_table(items)
        if keyword:
            self.status_var.set(
                f"搜索完成：原始 {len(all_items)} 条，关键词过滤后 {len(items)} 条（关键词：{keyword}）"
            )
        else:
            self.status_var.set(f"搜索完成，共 {len(items)} 条")

        self.searching = False
        self.search_btn.configure(state=tk.NORMAL)

    def _on_search_error(self, err_msg: str) -> None:
        self.searching = False
        self.search_btn.configure(state=tk.NORMAL)
        self.status_var.set("搜索失败")
        messagebox.showerror("搜索失败", err_msg)

    def _render_table(self, items: list[HouseItem]) -> None:
        for old in self.tree.get_children():
            self.tree.delete(old)

        for item in items:
            self.tree.insert(
                "",
                tk.END,
                values=(
                    item.title,
                    item.community,
                    item.total_price_wan,
                    item.unit_price,
                    item.house_info,
                    item.follow_info,
                    item.detail_url,
                ),
            )

    def _filter_items(self, items: list[HouseItem], keyword: str) -> list[HouseItem]:
        kw = self._normalize(keyword)
        if not kw:
            return items

        def item_text(it: HouseItem) -> str:
            return self._normalize(f"{it.title} {it.community} {it.house_info}")

        # 第一阶段：精确包含
        exact = [i for i in items if kw in item_text(i)]
        if exact:
            return exact

        # 第二阶段：模糊匹配（主要看标题和小区）
        fuzzy: list[HouseItem] = []
        for i in items:
            title = self._normalize(i.title)
            community = self._normalize(i.community)
            score = max(SequenceMatcher(None, kw, title).ratio(), SequenceMatcher(None, kw, community).ratio())
            if score >= 0.45:
                fuzzy.append(i)
        return fuzzy

    @staticmethod
    def _normalize(text: str) -> str:
        s = text.lower().strip()
        # 去除常见分隔符，避免“蓝城 桃李春风”等输入形式影响匹配
        return "".join(ch for ch in s if ch not in " \t\r\n-_/|,，。.;；:：()（）[]【】")

    def export_csv(self) -> None:
        if not self.results:
            messagebox.showinfo("提示", "当前没有可导出的数据")
            return

        default_name = Path("beike_search_result.csv").resolve()
        out_path = filedialog.asksaveasfilename(
            title="导出CSV",
            defaultextension=".csv",
            initialfile=default_name.name,
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
        if len(values) < 7:
            return
        url = values[6]
        if url:
            webbrowser.open(url)


def main() -> None:
    root = tk.Tk()
    app = BeikeGuiApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
