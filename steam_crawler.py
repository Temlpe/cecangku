import os
import time
import json
import requests
import mysql.connector
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys

# -------------------------- 配置读取 --------------------------
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}

STEAM_API_KEY = os.getenv('STEAM_API_KEY')

# -------------------------- 应用类 --------------------------
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

# -------------------------- 数据库操作 --------------------------
def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"数据库连接失败：{e}")
        sys.exit(1)

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    # 游戏表
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

    # 批次进度表（用于续跑AppList获取）
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

    try:
        cursor.execute(create_game_table_sql)
        cursor.execute(create_progress_table_sql)
        cursor.execute(init_progress_sql)
        conn.commit()
        print("表初始化完成")
    except Exception as e:
        print(f"创建表失败：{e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def get_crawled_appids():
    """获取已爬取的所有AppID（用于筛选新增）"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT app_id FROM steam_games;")
    crawled_ids = set([row[0] for row in cursor.fetchall()])
    cursor.close()
    conn.close()
    print(f"已爬取应用数量：{len(crawled_ids)}")
    return crawled_ids

# -------------------------- 进度操作 --------------------------
def save_progress(last_app_id, total_apps):
    conn = get_db_connection()
    cursor = conn.cursor()
    update_sql = """
    UPDATE crawl_progress 
    SET last_app_id = %s, total_apps = %s 
    WHERE id = 1;
    """
    try:
        cursor.execute(update_sql, (last_app_id, total_apps))
        conn.commit()
        print(f"进度已保存：last_app_id={last_app_id}, 累计应用数={total_apps}")
    except Exception as e:
        print(f"保存进度失败：{e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def load_progress():
    conn = get_db_connection()
    cursor = conn.cursor()
    select_sql = """
    SELECT last_app_id, total_apps FROM crawl_progress WHERE id = 1;
    """
    try:
        cursor.execute(select_sql)
        result = cursor.fetchone()
        return result if result else (0, 0)
    except Exception as e:
        print(f"加载进度失败：{e}")
        return 0, 0
    finally:
        cursor.close()
        conn.close()

# -------------------------- Steam API --------------------------
def create_session_with_retry():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def get_app_list(last_app_id=0):
    if not STEAM_API_KEY:
        print("STEAM_API_KEY 未配置！")
        return None
    url = f"https://api.steampowered.com/IStoreService/GetAppList/v1/ "
    params = {
        'key': STEAM_API_KEY,
        'include_games': True,
        'max_results': 50000,
        'last_appid': last_app_id
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data or 'response' not in data or 'apps' not in data['response']:
            raise Exception("API返回格式错误")
        apps = [App(app['appid'], app['name']) for app in data['response']['apps']]
        return {
            'apps': apps,
            'last_app_id': data['response'].get('last_appid', 0),
            'have_more': data['response'].get('have_more_results', False)
        }
    except Exception as e:
        print(f"获取应用列表失败：{e}")
        return None

def get_app_details(appid):
    url = f"https://store.steampowered.com/api/appdetails "
    params = {
        'appids': appid,
        'cc': 'cn',
        'l': 'schinese'
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    session = create_session_with_retry()
    try:
        response = session.get(url, params=params, headers=headers, timeout=10)
        if response.status_code != 200:
            return None
        data = response.json()
        if not data or str(appid) not in data or not data[str(appid)]['success']:
            return None
        return data[str(appid)]['data']
    except Exception as e:
        print(f"获取应用{appid}详情失败：{e}")
        return None
    finally:
        session.close()

def parse_app_details(app, details):
    app.type = details.get('type')
    app.is_free = details.get('is_free')
    if 'price_overview' in details:
        price = details['price_overview']
        app.price_final = price.get('final') / 100 if price.get('final') else None
        app.price_original = price.get('initial') / 100 if price.get('initial') else None
        app.discount_percent = price.get('discount_percent')
    if 'release_date' in details and not details['release_date'].get('coming_soon'):
        release_date_str = details['release_date'].get('date')
        if release_date_str:
            try:
                for fmt in ['%b %d, %Y', '%d %b, %Y', '%Y-%m-%d']:
                    app.release_date = datetime.strptime(release_date_str, fmt).date()
                    break
            except ValueError:
                app.release_date = None
    app.developers = ','.join(details['developers']) if details.get('developers') else None
    app.publishers = ','.join(details['publishers']) if details.get('publishers') else None
    app.genres = ','.join([g['description'] for g in details.get('genres', [])]) if details.get('genres') else None
    platforms = []
    if 'platforms' in details:
        if details['platforms'].get('windows'): platforms.append('Windows')
        if details['platforms'].get('mac'): platforms.append('macOS')
        if details['platforms'].get('linux'): platforms.append('Linux')
    app.platforms = ','.join(platforms) if platforms else None
    app.short_description = details.get('short_description')
    app.full_description = details.get('detailed_description')
    app.header_image = details.get('header_image')
    return app

def save_app_to_db(app):
    conn = get_db_connection()
    cursor = conn.cursor()
    insert_sql = """
    INSERT INTO steam_games (
        app_id, name, type, is_free, price_final, price_original,
        discount_percent, release_date, developers, publishers, genres,
        platforms, short_description, full_description, header_image, crawl_time
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        type = VALUES(type),
        is_free = VALUES(is_free),
        price_final = VALUES(price_final),
        price_original = VALUES(price_original),
        discount_percent = VALUES(discount_percent),
        release_date = VALUES(release_date),
        developers = VALUES(developers),
        publishers = VALUES(publishers),
        genres = VALUES(genres),
        platforms = VALUES(platforms),
        short_description = VALUES(short_description),
        full_description = VALUES(full_description),
        header_image = VALUES(header_image),
        crawl_time = VALUES(crawl_time)
    """
    crawl_time = datetime.now()
    values = (
        app.appid, app.name, app.type, app.is_free, app.price_final,
        app.price_original, app.discount_percent, app.release_date,
        app.developers, app.publishers, app.genres, app.platforms,
        app.short_description, app.full_description, app.header_image, crawl_time
    )
    try:
        cursor.execute(insert_sql, values)
        conn.commit()
        return cursor.rowcount == 1  # 1=新增，2=更新（但这里只处理新增）
    except Exception as e:
        print(f"保存应用{app.appid}失败：{e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

# -------------------------- 主函数 --------------------------
def main():
    # 检查配置
    if not all([DB_CONFIG['host'], DB_CONFIG['user'], DB_CONFIG['password'], DB_CONFIG['database'], STEAM_API_KEY]):
        print("缺少配置！")
        return

    # 初始化表
    create_tables()

    # 获取已爬取的AppID（核心：用于筛选新增）
    crawled_appids = get_crawled_appids()

    # 加载批次进度
    last_app_id, total_apps = load_progress()
    have_more = True
    new_count = 0  # 仅统计新增

    try:
        while have_more:
            # 获取批次应用列表
            app_list_data = None
            for tries in range(5):
                app_list_data = get_app_list(last_app_id)
                if app_list_data:
                    break
                print(f"重试获取列表（{tries+1}/5）...")
                time.sleep(5)

            if not app_list_data:
                print("获取列表失败，保存进度后退出")
                save_progress(last_app_id, total_apps)
                return

            # 仅筛选：未爬取过的AppID（新增应用）
            apps_to_process = [app for app in app_list_data['apps'] if app.appid not in crawled_appids]

            # 处理新增应用
            if apps_to_process:
                print(f"本批次新增应用数：{len(apps_to_process)}")
                for app in apps_to_process:
                    details = get_app_details(app.appid)
                    if details:
                        app = parse_app_details(app, details)
                        is_new = save_app_to_db(app)
                        if is_new:
                            new_count += 1
                            total_apps += 1
                            crawled_appids.add(app.appid)  # 加入已爬取集合，避免同批次重复
                            if new_count % 10 == 0:
                                print(f"新增{new_count}个 | 最后ID：{app.appid}")
                    time.sleep(1)  # 防限流
            else:
                print("本批次无新增应用")

            # 更新批次进度
            last_app_id = app_list_data['last_app_id']
            have_more = app_list_data['have_more']
            save_progress(last_app_id, total_apps)
            print(f"批次完成：last_app_id={last_app_id} | 是否有更多={have_more}")

    except KeyboardInterrupt:
        print("\n手动中断，保存进度...")
        save_progress(last_app_id, total_apps)
    except Exception as e:
        print(f"异常：{e}")
        save_progress(last_app_id, total_apps)

    # 重置批次进度（下次重新全量扫描，确保不遗漏新增）
    save_progress(0, total_apps)
    print(f"\n本次爬取完成！新增应用：{new_count} | 累计应用：{total_apps}")
    print("进度已重置，下次将扫描所有AppID并仅爬取新增")

if __name__ == "__main__":
    main()
