#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt

from beike_ershoufang_gui import BeikeSpider, HouseItem


def normalize_text(text: str) -> str:
    s = text.lower().strip()
    return "".join(ch for ch in s if ch not in " \t\r\n-_/|,，。.;；:：()（）[]【】")


def extract_float(text: str) -> float | None:
    if not text:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", str(text).replace(",", ""))
    return float(m.group(1)) if m else None


def extract_house_code(detail_url: str) -> str:
    m = re.search(r"/ershoufang/(\d+)\.html", detail_url or "")
    return m.group(1) if m else ""


def default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "02-数据库" / "beike_price_tracker.db"


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS communities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            community_input TEXT,
            community_name TEXT NOT NULL,
            community_key TEXT NOT NULL,
            resblock_id TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            UNIQUE(city, community_key),
            UNIQUE(city, resblock_id)
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            community_id INTEGER NOT NULL,
            snapshot_date TEXT NOT NULL,
            captured_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            item_count INTEGER NOT NULL,
            note TEXT,
            UNIQUE(community_id, snapshot_date),
            FOREIGN KEY(community_id) REFERENCES communities(id)
        );

        CREATE TABLE IF NOT EXISTS listings_snapshot (
            snapshot_id INTEGER NOT NULL,
            seq_no INTEGER NOT NULL,
            house_code TEXT NOT NULL,
            detail_url TEXT,
            title TEXT,
            community_name TEXT,
            total_price_wan REAL,
            unit_price_yuan REAL,
            house_info TEXT,
            follow_info TEXT,
            PRIMARY KEY(snapshot_id, house_code),
            FOREIGN KEY(snapshot_id) REFERENCES snapshots(id)
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_community_date ON snapshots(community_id, snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_listings_snapshot_seq ON listings_snapshot(snapshot_id, seq_no);
        """
    )
    conn.commit()


def upsert_community(
    conn: sqlite3.Connection,
    city: str,
    community_input: str,
    community_name: str,
    resblock_id: str,
) -> int:
    ckey = normalize_text(community_name or community_input)

    row = None
    if resblock_id:
        row = conn.execute(
            "SELECT id FROM communities WHERE city=? AND resblock_id=?",
            (city, resblock_id),
        ).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT id FROM communities WHERE city=? AND community_key=?",
            (city, ckey),
        ).fetchone()

    if row:
        cid = int(row[0])
        conn.execute(
            """
            UPDATE communities
            SET community_input=?, community_name=?, community_key=?, resblock_id=?, updated_at=datetime('now','localtime')
            WHERE id=?
            """,
            (community_input, community_name, ckey, resblock_id, cid),
        )
        conn.commit()
        return cid

    cur = conn.execute(
        """
        INSERT INTO communities(city, community_input, community_name, community_key, resblock_id)
        VALUES(?,?,?,?,?)
        """,
        (city, community_input, community_name, ckey, resblock_id),
    )
    conn.commit()
    return int(cur.lastrowid)


def upsert_snapshot(
    conn: sqlite3.Connection,
    community_id: int,
    snapshot_date: str,
    item_count: int,
    note: str,
) -> int:
    row = conn.execute(
        "SELECT id FROM snapshots WHERE community_id=? AND snapshot_date=?",
        (community_id, snapshot_date),
    ).fetchone()

    if row:
        sid = int(row[0])
        conn.execute(
            """
            UPDATE snapshots
            SET item_count=?, note=?, captured_at=datetime('now','localtime')
            WHERE id=?
            """,
            (item_count, note, sid),
        )
        conn.execute("DELETE FROM listings_snapshot WHERE snapshot_id=?", (sid,))
        conn.commit()
        return sid

    cur = conn.execute(
        """
        INSERT INTO snapshots(community_id, snapshot_date, item_count, note)
        VALUES(?,?,?,?)
        """,
        (community_id, snapshot_date, item_count, note),
    )
    conn.commit()
    return int(cur.lastrowid)


def prepare_sorted_rows(items: List[HouseItem]) -> List[Dict]:
    rows = []
    for it in items:
        house_code = extract_house_code(it.detail_url)
        if not house_code:
            continue
        rows.append(
            {
                "house_code": house_code,
                "detail_url": it.detail_url,
                "title": it.title,
                "community_name": it.community,
                "total_price_wan": extract_float(it.total_price_wan),
                "unit_price_yuan": extract_float(it.unit_price),
                "house_info": it.house_info,
                "follow_info": it.follow_info,
            }
        )

    rows.sort(key=lambda x: (x["total_price_wan"] is None, x["total_price_wan"] or 0.0, x["house_code"]))
    for idx, row in enumerate(rows, start=1):
        row["seq_no"] = idx
    return rows


def save_snapshot_rows(conn: sqlite3.Connection, snapshot_id: int, rows: List[Dict]) -> None:
    conn.executemany(
        """
        INSERT INTO listings_snapshot(
            snapshot_id, seq_no, house_code, detail_url, title, community_name,
            total_price_wan, unit_price_yuan, house_info, follow_info
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                snapshot_id,
                r["seq_no"],
                r["house_code"],
                r["detail_url"],
                r["title"],
                r["community_name"],
                r["total_price_wan"],
                r["unit_price_yuan"],
                r["house_info"],
                r["follow_info"],
            )
            for r in rows
        ],
    )
    conn.commit()


def export_snapshot_csv(csv_path: Path, rows: List[Dict]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def find_community_id(conn: sqlite3.Connection, city: str, community: str) -> int | None:
    target = normalize_text(community)
    rows = conn.execute(
        "SELECT id, community_name, community_input, community_key FROM communities WHERE city=?",
        (city,),
    ).fetchall()
    if not rows:
        return None

    # 1) exact key
    for r in rows:
        if (r[3] or "") == target:
            return int(r[0])
    # 2) exact by input/name normalize
    for r in rows:
        n1 = normalize_text(r[1] or "")
        n2 = normalize_text(r[2] or "")
        if target == n1 or target == n2:
            return int(r[0])
    # 3) contains
    for r in rows:
        n1 = normalize_text(r[1] or "")
        n2 = normalize_text(r[2] or "")
        if (target in n1) or (n1 in target) or (target in n2) or (n2 in target):
            return int(r[0])
    return None


def load_snapshot_rows(conn: sqlite3.Connection, community_id: int, snapshot_date: str) -> List[Dict]:
    rows = conn.execute(
        """
        SELECT l.seq_no, l.house_code, l.detail_url, l.title, l.community_name,
               l.total_price_wan, l.unit_price_yuan, l.house_info, l.follow_info
        FROM listings_snapshot l
        JOIN snapshots s ON s.id=l.snapshot_id
        WHERE s.community_id=? AND s.snapshot_date=?
        ORDER BY l.seq_no ASC
        """,
        (community_id, snapshot_date),
    ).fetchall()

    out = []
    for r in rows:
        out.append(
            {
                "seq_no": int(r[0]),
                "house_code": r[1],
                "detail_url": r[2],
                "title": r[3],
                "community_name": r[4],
                "total_price_wan": r[5],
                "unit_price_yuan": r[6],
                "house_info": r[7],
                "follow_info": r[8],
            }
        )
    return out


def plot_histogram(rows: List[Dict], title: str, output_png: Path) -> None:
    output_png.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError("该日期没有数据")

    x = [r["seq_no"] for r in rows]
    y = [r["total_price_wan"] or 0 for r in rows]

    plt.figure(figsize=(14, 6))
    plt.bar(x, y, width=0.9)
    plt.xlabel("Index (sorted by total price asc)")
    plt.ylabel("Total Price (Wan CNY)")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_png, dpi=160)
    plt.close()


def plot_compare(rows_a: List[Dict], rows_b: List[Dict], title: str, output_png: Path) -> Tuple[int, int, int]:
    output_png.parent.mkdir(parents=True, exist_ok=True)

    map_a = {r["house_code"]: r for r in rows_a}
    map_b = {r["house_code"]: r for r in rows_b}
    codes_a = set(map_a.keys())
    codes_b = set(map_b.keys())

    common = sorted(codes_a & codes_b, key=lambda c: (map_a[c]["total_price_wan"] is None, map_a[c]["total_price_wan"] or 0, c))
    added = sorted(codes_b - codes_a)
    removed = sorted(codes_a - codes_b)

    if not common and not added and not removed:
        raise ValueError("两天都没有数据")

    x = list(range(1, len(common) + 1))
    a_vals = [(map_a[c]["total_price_wan"] or 0) for c in common]
    b_vals = [(map_b[c]["total_price_wan"] or 0) for c in common]
    deltas = [b - a for a, b in zip(a_vals, b_vals)]

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=False)

    if common:
        w = 0.42
        axes[0].bar([i - w / 2 for i in x], a_vals, width=w, label="Date A")
        axes[0].bar([i + w / 2 for i in x], b_vals, width=w, label="Date B")
        axes[0].set_ylabel("Total Price (Wan CNY)")
        axes[0].set_title(f"{title} - Price Bars")
        axes[0].legend()
    else:
        axes[0].text(0.5, 0.5, "No common listings", ha="center", va="center")

    if common:
        colors = ["#2ca02c" if d < 0 else "#d62728" if d > 0 else "#7f7f7f" for d in deltas]
        axes[1].bar(x, deltas, color=colors, width=0.9)
        axes[1].axhline(0, color="black", linewidth=0.8)
        axes[1].set_xlabel("Common Listing Index (sorted by Date A price)")
        axes[1].set_ylabel("Price Change (Wan)")
        axes[1].set_title("Global Price Change (Date B - Date A)")
    else:
        axes[1].text(0.5, 0.5, "No common listings", ha="center", va="center")

    fig.suptitle(
        f"added={len(added)} removed={len(removed)} common={len(common)}",
        fontsize=10,
    )
    fig.tight_layout()
    fig.savefig(output_png, dpi=160)
    plt.close(fig)

    return len(common), len(added), len(removed)


def cmd_collect(args: argparse.Namespace) -> None:
    db_path = Path(args.db).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    snapshot_date = args.date or date.today().isoformat()
    spider = BeikeSpider(city=args.city, sleep_sec=args.sleep, cookie=args.cookie)

    items, meta = spider.crawl_community_direct(
        community=args.community,
        max_pages=args.max_pages,
        fallback_scan=False,
    )

    with sqlite3.connect(db_path) as conn:
        ensure_db(conn)

        community_name = meta.get("community_name") or args.community
        resblock_id = meta.get("resblock_id") or ""
        community_id = upsert_community(
            conn,
            city=args.city,
            community_input=args.community,
            community_name=community_name,
            resblock_id=resblock_id,
        )

        rows = prepare_sorted_rows(items)
        snapshot_id = upsert_snapshot(
            conn,
            community_id=community_id,
            snapshot_date=snapshot_date,
            item_count=len(rows),
            note=json.dumps(meta, ensure_ascii=False),
        )
        save_snapshot_rows(conn, snapshot_id, rows)

    csv_name = f"{args.city}_{normalize_text(community_name)}_{snapshot_date}.csv"
    csv_path = db_path.parent / "snapshots" / csv_name
    export_snapshot_csv(csv_path, rows)

    print(f"[OK] db={db_path}")
    print(f"[OK] snapshot_date={snapshot_date} community={community_name} resblock_id={resblock_id}")
    print(f"[OK] listings={len(rows)} csv={csv_path}")
    print(f"[INFO] direct_blocked={meta.get('direct_blocked')} fallback_used={meta.get('fallback_used')}")


def cmd_hist(args: argparse.Namespace) -> None:
    db_path = Path(args.db).resolve()
    out_png = Path(args.out).resolve() if args.out else (db_path.parent / "plots" / f"hist_{args.city}_{normalize_text(args.community)}_{args.date}.png")

    with sqlite3.connect(db_path) as conn:
        community_id = find_community_id(conn, args.city, args.community)
        if not community_id:
            raise ValueError("数据库里找不到这个小区，请先执行 collect")
        rows = load_snapshot_rows(conn, community_id, args.date)

    plot_histogram(rows, f"{args.city} {args.community} {args.date}", out_png)
    print(f"[OK] histogram={out_png} rows={len(rows)}")


def cmd_compare(args: argparse.Namespace) -> None:
    db_path = Path(args.db).resolve()
    out_png = Path(args.out).resolve() if args.out else (db_path.parent / "plots" / f"compare_{args.city}_{normalize_text(args.community)}_{args.date_a}_vs_{args.date_b}.png")

    with sqlite3.connect(db_path) as conn:
        community_id = find_community_id(conn, args.city, args.community)
        if not community_id:
            raise ValueError("数据库里找不到这个小区，请先执行 collect")
        rows_a = load_snapshot_rows(conn, community_id, args.date_a)
        rows_b = load_snapshot_rows(conn, community_id, args.date_b)

    common, added, removed = plot_compare(
        rows_a,
        rows_b,
        f"{args.city} {args.community} {args.date_a} vs {args.date_b}",
        out_png,
    )
    print(f"[OK] compare={out_png}")
    print(f"[OK] common={common} added={added} removed={removed}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="指定小区房价追踪：编号、入库、历史对比可视化")
    p.add_argument("--db", default=str(default_db_path()), help="SQLite数据库路径")

    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("collect", help="抓取指定小区在售并写入数据库（日快照）")
    c.add_argument("--city", required=True, help="城市简拼，如 hz")
    c.add_argument("--community", required=True, help="小区名称，如 蓝城春风燕语")
    c.add_argument("--date", default="", help="快照日期 YYYY-MM-DD，默认今天")
    c.add_argument("--max-pages", type=int, default=120, help="最大扫描页数")
    c.add_argument("--sleep", type=float, default=1.0, help="分页抓取间隔秒")
    c.add_argument("--cookie", default="", help="登录Cookie（可选）")

    h = sub.add_parser("hist", help="按指定日期画当前总价从低到高的直方图")
    h.add_argument("--city", required=True)
    h.add_argument("--community", required=True)
    h.add_argument("--date", required=True, help="YYYY-MM-DD")
    h.add_argument("--out", default="", help="输出PNG路径（可选）")

    cp = sub.add_parser("compare", help="对比两个历史日期并输出全局变动图")
    cp.add_argument("--city", required=True)
    cp.add_argument("--community", required=True)
    cp.add_argument("--date-a", required=True, help="基准日期 YYYY-MM-DD")
    cp.add_argument("--date-b", required=True, help="对比日期 YYYY-MM-DD")
    cp.add_argument("--out", default="", help="输出PNG路径（可选）")

    return p


def main() -> None:
    args = build_parser().parse_args()
    if args.cmd == "collect":
        cmd_collect(args)
    elif args.cmd == "hist":
        cmd_hist(args)
    elif args.cmd == "compare":
        cmd_compare(args)
    else:
        raise ValueError("unknown command")


if __name__ == "__main__":
    main()
