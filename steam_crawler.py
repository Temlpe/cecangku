#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Steam 爬虫 – 修复 MySQL 2013 断连
环境变量：
  DB_HOST/DB_USER/DB_PASSWORD/DB_NAME/STEAM_API_KEY
"""
import os
import time
import json
import requests
import mysql.connector
from mysql.connector import errorcode, errors
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys

# -------------------------- 配置 --------------------------
DB_CFG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4',
    'autocommit': False,               # 手动 commit
    'connect_timeout': 15,             # 关键：建立连接超时
    'connection_timeout': 28800,       # 8 h，配合服务端
    'use_unicode': True,
    'sql_mode': 'TRADITIONAL'
}

STEAM_API_KEY = os.getenv('STEAM_API_KEY')

# -------------------------- 数据库上下文 --------------------------
class Conn:
    """线程级长连接 + 自动重连 + 事务"""
    def __init__(self, cfg):
        self.cfg = cfg
        self._conn = None

    def _reconnect(self):
        """建立新连接"""
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = mysql.connector.connect(**self.cfg)

    def ping(self):
        """心跳检测，失败则重连"""
        try:
            self._conn.ping(reconnect=False, attempts=1, delay=0)
        except (errors.InterfaceError, errors.OperationalError):
            self._reconnect()

    def cursor(self, prepared=False):
        self.ping()
        return self._conn.cursor(prepared=prepared)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        if self._conn:
            self._conn.close()

    def __enter__(self):
        self._reconnect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()


# -------------------------- 建表 --------------------------
def create_tables(conn: Conn):
    create_game_table_sql = """
    CREATE TABLE IF NOT EXISTS steam_games (
        app_id INT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        type VARCHAR(50),
        is_free BOOLEAN,
        price_final FLOAT,
        price_original FLOAT,
        discount_percent INT,
        release_date DATE,
        developers VARCHAR(255),
        publishers VARCHAR(255),
        genres VARCHAR(512),
        platforms VARCHAR(100),
        short_description TEXT,
        full_description LONGTEXT,
        header_image VARCHAR(512),
        crawl_time DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    create_progress_table_sql = """
    CREATE TABLE IF NOT EXISTS crawl_progress (
        id INT PRIMARY KEY AUTO_INCREMENT,
        last_app_id INT NOT NULL DEFAULT 0,
        total_apps INT NOT NULL DEFAULT 0,
        last_crawl_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    init_progress_sql = """
    INSERT IGNORE INTO crawl_progress (id, last_app_id, total_apps)
    VALUES (1, 0, 0);
    """
    with conn.cursor() as cur:
        cur.execute(create_game_table_sql)
        cur.execute(create_progress_table_sql)
        cur.execute(init_progress_sql)
    conn.commit()
    print("表初始化完成")


# -------------------------- 进度 --------------------------
def load_progress(conn: Conn):
    with conn.cursor() as cur:
        cur.execute("SELECT last_app_id, total_apps FROM crawl_progress WHERE id = 1")
        row = cur.fetchone()
        return row if row else (0, 0)


def save_progress(conn: Conn, last_app_id, total_apps):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE crawl_progress SET last_app_id=%s, total_apps=%s WHERE id=1",
            (last_app_id, total_apps)
        )
    conn.commit()


# -------------------------- 已爬集合 --------------------------
def get_crawled_appids(conn: Conn):
    with conn.cursor() as cur:
        cur.execute("SELECT app_id FROM steam_games")
        return {row[0] for row in cur.fetchall()}


# -------------------------- 保存游戏 --------------------------
def save_app_to_db(conn: Conn, app):
    sql = """
    INSERT INTO steam_games (
        app_id, name, type, is_free, price_final, price_original,
        discount_percent, release_date, developers, publishers, genres,
        platforms, short_description, full_description, header_image, crawl_time
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        name=VALUES(name),
        type=VALUES(type),
        is_free=VALUES(is_free),
        price_final=VALUES(price_final),
        price_original=VALUES(price_original),
        discount_percent=VALUES(discount_percent),
        release_date=VALUES(release_date),
        developers=VALUES(developers),
        publishers=VALUES(publishers),
        genres=VALUES(genres),
        platforms=VALUES(platforms),
        short_description=VALUES(short_description),
        full_description=VALUES(full_description),
        header_image=VALUES(header_image),
        crawl_time=VALUES(crawl_time)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            app.appid, app.name, app.type, app.is_free, app.price_final,
            app.price_original, app.discount_percent, app.release_date,
            app.developers, app.publishers, app.genres, app.platforms,
            app.short_description, app.full_description, app.header_image,
            datetime.now()
        ))
    return cur.rowcount == 1


# -------------------------- 业务类 --------------------------
class App:
    def __init__(self, appid, name):
        self.appid = appid
        self.name = name
        self.type = None
        self.is_free = None
        self.price_final = None
        self.price_original = None
        self.discount_percent = None
        self.release_date = None
        self.developers = None
        self.publishers = None
        self.genres = None
        self.platforms = None
        self.short_description = None
        self.full_description = None
        self.header_image = None


# -------------------------- Steam API --------------------------
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))


def get_app_list(last_app_id=0):
    if not STEAM_API_KEY:
        print("STEAM_API_KEY 未配置")
        return None
    url = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
    params = {
        'key': STEAM_API_KEY,
        'include_games': True,
        'max_results': 50000,
        'last_appid': last_app_id
    }
    try:
        r = session.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        apps = [App(a['appid'], a['name']) for a in data['response']['apps']]
        return {
            'apps': apps,
            'last_app_id': data['response'].get('last_appid', 0),
            'have_more': data['response'].get('have_more_results', False)
        }
    except Exception as e:
        print("获取列表失败:", e)
        return None


def get_app_details(appid):
    url = "https://store.steampowered.com/api/appdetails"
    params = {'appids': appid, 'cc': 'cn', 'l': 'schinese'}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        r = session.get(url, params=params, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data or str(appid) not in data or not data[str(appid)]['success']:
            return None
        return data[str(appid)]['data']
    except Exception as e:
        print(f"获取详情 {appid} 失败:", e)
        return None


def parse_app_details(app, details):
    app.type = details.get('type')
    app.is_free = details.get('is_free')
    if 'price_overview' in details:
        po = details['price_overview']
        app.price_final = po.get('final') / 100 if po.get('final') else None
        app.price_original = po.get('initial') / 100 if po.get('initial') else None
        app.discount_percent = po.get('discount_percent')
    rd = details.get('release_date', {})
    if rd and not rd.get('coming_soon'):
        date_str = rd.get('date')
        if date_str:
            for fmt in ['%b %d, %Y', '%d %b, %Y', '%Y-%m-%d']:
                try:
                    app.release_date = datetime.strptime(date_str, fmt).date()
                    break
                except ValueError:
                    pass
    app.developers = ','.join(details['developers']) if details.get('developers') else None
    app.publishers = ','.join(details['publishers']) if details.get('publishers') else None
    app.genres = ','.join(g['description'] for g in details.get('genres', [])) or None
    platforms = []
    pf = details.get('platforms', {})
    if pf.get('windows'): platforms.append('Windows')
    if pf.get('mac'): platforms.append('macOS')
    if pf.get('linux'): platforms.append('Linux')
    app.platforms = ','.join(platforms) or None
    app.short_description = details.get('short_description')
    app.full_description = details.get('detailed_description')
    app.header_image = details.get('header_image')
    return app


# -------------------------- 主函数 --------------------------
def main():
    if not all([DB_CFG['host'], DB_CFG['user'], DB_CFG['password'], DB_CFG['database'], STEAM_API_KEY]):
        print("缺少配置")
        return

    with Conn(DB_CFG) as conn:
        create_tables(conn)
        crawled_appids = get_crawled_appids(conn)
        last_app_id, total_apps = load_progress(conn)
        have_more = True
        new_count = 0

        try:
            while have_more:
                batch = None
                for _ in range(5):
                    batch = get_app_list(last_app_id)
                    if batch:
                        break
                    time.sleep(5)
                if not batch:
                    save_progress(conn, last_app_id, total_apps)
                    return

                todo = [a for a in batch['apps'] if a.appid not in crawled_appids]
                if todo:
                    print(f"本批次新增 {len(todo)} 个")
                    for app in todo:
                        detail = get_app_details(app.appid)
                        if detail:
                            parse_app_details(app, detail)
                            if save_app_to_db(conn, app):
                                new_count += 1
                                total_apps += 1
                                crawled_appids.add(app.appid)
                                if new_count % 10 == 0:
                                    print(f"已新增 {new_count} 个 | 最后ID {app.appid}")
                        time.sleep(1)
                else:
                    print("本批次无新增")

                last_app_id = batch['last_app_id']
                have_more = batch['have_more']
                save_progress(conn, last_app_id, total_apps)
                print(f"批次完成 last_app_id={last_app_id} have_more={have_more}")

        except KeyboardInterrupt:
            print("\n手动中断，保存进度...")
            save_progress(conn, last_app_id, total_apps)
        except Exception as e:
            print("异常:", e)
            save_progress(conn, last_app_id, total_apps)

        # 重置进度
        save_progress(conn, 0, total_apps)
        print(f"\n完成！新增 {new_count} | 累计 {total_apps}")


if __name__ == "__main__":
    main()
