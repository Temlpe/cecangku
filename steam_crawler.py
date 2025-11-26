import os
import time
import json
import requests
import mysql.connector
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys

# -------------------------- 适配 GitHub Actions：从环境变量读取配置 --------------------------
# 数据库配置（GitHub Actions 会注入 Secrets 为环境变量）
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),        # 对应 GitHub Secrets: DB_HOST
    'user': os.getenv('DB_USER'),        # 对应 GitHub Secrets: DB_USER
    'password': os.getenv('DB_PASSWORD'),# 对应 GitHub Secrets: DB_PASSWORD
    'database': os.getenv('DB_NAME'),    # 对应 GitHub Secrets: DB_NAME
    'charset': 'utf8mb4'
}

# Steam API 密钥（对应 GitHub Secrets: STEAM_API_KEY）
STEAM_API_KEY = os.getenv('STEAM_API_KEY')

# -------------------------- 应用类定义 --------------------------
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

# -------------------------- 数据库基础操作 --------------------------
def get_db_connection():
    """建立数据库连接（适配云数据库）"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"数据库连接失败：{e}")
        sys.exit(1)  # 连接失败则退出程序

def create_tables():
    """创建游戏表 + 进度表（GitHub Actions 首次运行时自动创建）"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. 创建游戏表（原表结构）
    create_game_table_sql = """
    CREATE TABLE IF NOT EXISTS steam_games (
        app_id INT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        type VARCHAR(50) COMMENT '游戏类型（game/dlc/tool 等）',
        is_free BOOLEAN COMMENT '是否免费',
        price_final FLOAT COMMENT '最终价格（元）',
        price_original FLOAT COMMENT '原价（元）',
        discount_percent INT COMMENT '折扣百分比',
        release_date DATE COMMENT '发布日期',
        developers VARCHAR(255) COMMENT '开发商（多个用逗号分隔）',
        publishers VARCHAR(255) COMMENT '发行商（多个用逗号分隔）',
        genres VARCHAR(512) COMMENT '游戏标签（多个用逗号分隔）',
        platforms VARCHAR(100) COMMENT '支持平台（Windows/macOS/Linux）',
        short_description TEXT COMMENT '简介',
        full_description LONGTEXT COMMENT '完整描述',
        header_image VARCHAR(512) COMMENT '封面图 URL',
        crawl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '爬取时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    # 2. 创建进度表（替代本地json文件，持久化进度）
    create_progress_table_sql = """
    CREATE TABLE IF NOT EXISTS crawl_progress (
        id INT PRIMARY KEY AUTO_INCREMENT,
        last_app_id INT NOT NULL DEFAULT 0,
        total_apps INT NOT NULL DEFAULT 0,
        last_crawl_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    # 3. 初始化进度表（若为空则插入初始数据）
    init_progress_sql = """
    INSERT IGNORE INTO crawl_progress (id, last_app_id, total_apps)
    VALUES (1, 0, 0);
    """

    try:
        cursor.execute(create_game_table_sql)
        cursor.execute(create_progress_table_sql)
        cursor.execute(init_progress_sql)
        conn.commit()
        print("游戏表和进度表初始化完成")
    except Exception as e:
        print(f"创建表失败：{e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# -------------------------- 进度操作（数据库替代本地文件） --------------------------
def save_progress(last_app_id, total_apps):
    """保存进度到数据库（替代本地json）"""
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
        print(f"进度已保存到数据库：last_app_id={last_app_id}, total_apps={total_apps}")
    except Exception as e:
        print(f"保存进度失败：{e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def load_progress():
    """从数据库加载进度（替代本地json）"""
    conn = get_db_connection()
    cursor = conn.cursor()
    select_sql = """
    SELECT last_app_id, total_apps FROM crawl_progress WHERE id = 1;
    """
    try:
        cursor.execute(select_sql)
        result = cursor.fetchone()
        if result:
            last_app_id, total_apps = result
            print(f"从数据库加载进度成功：last_app_id={last_app_id}, 已处理{total_apps}个应用")
            return last_app_id, total_apps
        else:
            print("进度表无数据，从初始状态开始")
            return 0, 0
    except Exception as e:
        print(f"加载进度失败：{e}")
        return 0, 0
    finally:
        cursor.close()
        conn.close()

# -------------------------- Steam API 操作 --------------------------
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
    """获取应用列表"""
    if not STEAM_API_KEY:
        print("STEAM_API_KEY 未配置！")
        return None
    url = f"https://api.steampowered.com/IStoreService/GetAppList/v1/"
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
    """获取应用详细信息"""
    url = f"https://store.steampowered.com/api/appdetails"
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
    """解析应用详情"""
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
    """保存应用到数据库"""
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
        # 返回是否为新增记录（1=新增，2=更新）
        return cursor.rowcount == 1
    except Exception as e:
        print(f"保存应用{app.appid}失败：{e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

# -------------------------- 主函数 --------------------------
def main():
    # 1. 检查必要配置
    if not all([DB_CONFIG['host'], DB_CONFIG['user'], DB_CONFIG['password'], DB_CONFIG['database'], STEAM_API_KEY]):
        print("缺少数据库配置或Steam API密钥！")
        return

    # 2. 初始化数据库表
    create_tables()

    # 3. 加载上次进度（从数据库）
    last_app_id, total_apps = load_progress()
    have_more = True

    try:
        while have_more:
            # 获取应用列表
            app_list_data = None
            for tries in range(5):
                app_list_data = get_app_list(last_app_id)
                if app_list_data:
                    break
                print(f"重试获取应用列表（{tries+1}/5）...")
                time.sleep(5)

            if not app_list_data:
                print("多次获取应用列表失败，保存进度后退出")
                save_progress(last_app_id, total_apps)
                return

            # 处理每个应用
            for app in app_list_data['apps']:
                details = get_app_details(app.appid)
                if details:
                    app = parse_app_details(app, details)
                    is_new = save_app_to_db(app)
                    if is_new:
                        total_apps += 1
                        if total_apps % 10 == 0:
                            print(f"累计新增{total_apps}个应用，最后ID：{app.appid}")
                time.sleep(1)  # 避免API限流

            # 更新进度
            last_app_id = app_list_data['last_app_id']
            have_more = app_list_data['have_more']
            save_progress(last_app_id, total_apps)
            print(f"批次处理完成：最后ID={last_app_id}，是否有更多={have_more}")

    except KeyboardInterrupt:
        print("\n程序被手动中断，保存进度...")
        save_progress(last_app_id, total_apps)
    except Exception as e:
        print(f"程序异常：{e}")
        save_progress(last_app_id, total_apps)

    print(f"本次爬取完成！累计新增应用：{total_apps}，最后处理ID：{last_app_id}")

if __name__ == "__main__":
    main()
