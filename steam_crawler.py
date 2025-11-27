import os
import time
import json
import requests
import mysql.connector
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys
import logging

# -------------------------- 配置读取 --------------------------
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4',
    # 关键修改：增加连接超时时间 (30秒)
    'connect_timeout': 30,
    # 关键修改：启用自动重连
    'autocommit': True
}

STEAM_API_KEY = os.getenv('STEAM_API_KEY')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("steam_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

# -------------------------- 数据库连接管理 --------------------------
class DBConnectionManager:
    """管理数据库连接，包含重试机制"""
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # 重试间隔(秒)
    
    @staticmethod
    def get_connection():
        """获取数据库连接（带重试机制）"""
        for attempt in range(1, DBConnectionManager.MAX_RETRIES + 1):
            try:
                conn = mysql.connector.connect(**DB_CONFIG)
                logger.info("数据库连接成功")
                return conn
            except mysql.connector.Error as err:
                # 关键修改：分类处理错误
                if err.errno == 2013:  # Lost connection
                    logger.warning(f"连接失败 (尝试 {attempt}/{DBConnectionManager.MAX_RETRIES}): {err}")
                    if attempt < DBConnectionManager.MAX_RETRIES:
                        time.sleep(DBConnectionManager.RETRY_DELAY * attempt)
                        continue
                elif err.errno == 2003:  # Can't connect to MySQL server
                    logger.error(f"无法连接到数据库服务器，请检查网络和配置: {err}")
                elif err.errno == 1045:  # Access denied
                    logger.critical(f"数据库认证失败，请检查用户名/密码: {err}")
                    sys.exit(1)
                else:
                    logger.error(f"未知数据库错误: {err}")
                
                if attempt == DBConnectionManager.MAX_RETRIES:
                    logger.critical("达到最大重试次数，程序终止")
                    sys.exit(1)
            except Exception as e:
                logger.critical(f"严重数据库错误: {e}")
                sys.exit(1)
        
        logger.critical("无法建立数据库连接")
        sys.exit(1)

# -------------------------- 数据库操作 --------------------------
def create_tables():
    """创建必要表结构"""
    conn = DBConnectionManager.get_connection()
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

    # 批次进度表
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
        logger.info("数据库表初始化完成")
    except Exception as e:
        logger.error(f"创建表失败: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def get_crawled_appids():
    """获取已爬取的所有AppID（使用单连接）"""
    conn = DBConnectionManager.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT app_id FROM steam_games;")
        crawled_ids = {row[0] for row in cursor.fetchall()}
        logger.info(f"已爬取应用数量: {len(crawled_ids)}")
        return crawled_ids
    finally:
        cursor.close()
        conn.close()

# -------------------------- 进度操作 --------------------------
def save_progress(last_app_id, total_apps):
    """保存爬取进度"""
    conn = DBConnectionManager.get_connection()
    cursor = conn.cursor()
    update_sql = """
    UPDATE crawl_progress 
    SET last_app_id = %s, total_apps = %s 
    WHERE id = 1;
    """
    try:
        cursor.execute(update_sql, (last_app_id, total_apps))
        conn.commit()
        logger.info(f"进度保存成功: last_app_id={last_app_id}, total_apps={total_apps}")
        return True
    except Exception as e:
        logger.error(f"保存进度失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def load_progress():
    """加载爬取进度"""
    conn = DBConnectionManager.get_connection()
    cursor = conn.cursor()
    select_sql = """
    SELECT last_app_id, total_apps FROM crawl_progress WHERE id = 1;
    """
    try:
        cursor.execute(select_sql)
        result = cursor.fetchone()
        if result:
            logger.info(f"加载进度成功: last_app_id={result[0]}, total_apps={result[1]}")
            return result[0], result[1]
        logger.warning("进度表为空，使用默认值 (0, 0)")
        return 0, 0
    except Exception as e:
        logger.error(f"加载进度失败: {e}")
        return 0, 0
    finally:
        cursor.close()
        conn.close()

# -------------------------- Steam API --------------------------
def create_session_with_retry():
    """创建带重试机制的HTTP会话"""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # 增加重试次数
        backoff_factor=2,  # 增加退避时间
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def get_app_list(last_app_id=0):
    """获取应用列表（带重试）"""
    if not STEAM_API_KEY:
        logger.critical("STEAM_API_KEY 未配置！")
        return None
    
    url = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
    params = {
        'key': STEAM_API_KEY,
        'include_games': True,
        'max_results': 50000,
        'last_appid': last_app_id
    }
    
    session = create_session_with_retry()
    try:
        response = session.get(url, params=params, timeout=(10, 30))  # (connect, read)
        response.raise_for_status()
        data = response.json()
        
        if not data or 'response' not in data or 'apps' not in data['response']:
            logger.error("API返回格式错误")
            return None
            
        apps = [App(app['appid'], app['name']) for app in data['response']['apps']]
        return {
            'apps': apps,
            'last_app_id': data['response'].get('last_appid', 0),
            'have_more': data['response'].get('have_more_results', False)
        }
    except Exception as e:
        logger.error(f"获取应用列表失败: {e}")
        return None
    finally:
        session.close()

def get_app_details(appid):
    """获取应用详情"""
    url = "https://store.steampowered.com/api/appdetails"
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
        response = session.get(url, params=params, headers=headers, timeout=(15, 45))
        if response.status_code != 200:
            logger.warning(f"应用详情请求失败 (HTTP {response.status_code}): {appid}")
            return None
            
        data = response.json()
        if not data or str(appid) not in data or not data[str(appid)]['success']:
            return None
        return data[str(appid)]['data']
    except Exception as e:
        logger.error(f"获取应用{appid}详情失败: {e}")
        return None
    finally:
        session.close()

def parse_app_details(app, details):
    """解析应用详情数据"""
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
    """保存应用数据到数据库（带连接重试）"""
    conn = None
    cursor = None
    try:
        conn = DBConnectionManager.get_connection()
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
        
        cursor.execute(insert_sql, values)
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"保存应用{app.appid}失败: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# -------------------------- 主函数 --------------------------
def main():
    # 检查配置
    required_env = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'STEAM_API_KEY']
    missing = [var for var in required_env if not os.getenv(var)]
    if missing:
        logger.critical(f"缺少必要环境变量: {', '.join(missing)}")
        sys.exit(1)

    # 初始化表
    try:
        create_tables()
    except Exception as e:
        logger.critical(f"数据库初始化失败: {e}")
        sys.exit(1)

    # 获取已爬取的AppID
    try:
        crawled_appids = get_crawled_appids()
    except Exception as e:
        logger.critical(f"获取已爬取ID失败: {e}")
        sys.exit(1)

    # 加载进度
    last_app_id, total_apps = load_progress()
    have_more = True
    new_count = 0
    apps_processed = 0
    start_time = time.time()

    try:
        while have_more:
            # 获取应用列表
            app_list_data = get_app_list(last_app_id)
            if not app_list_data:
                logger.warning("获取应用列表失败，等待后重试...")
                time.sleep(10)
                continue

            # 筛选新增应用
            new_apps = [
                app for app in app_list_data['apps'] 
                if app.appid not in crawled_appids
            ]
            
            logger.info(f"批次数据: total={len(app_list_data['apps'])}, new={len(new_apps)}")
            
            # 处理新增应用
            for app in new_apps:
                details = get_app_details(app.appid)
                if details:
                    try:
                        app = parse_app_details(app, details)
                        if save_app_to_db(app):
                            new_count += 1
                            total_apps += 1
                            crawled_appids.add(app.appid)
                            
                            # 每10个应用保存进度
                            if new_count % 10 == 0:
                                save_progress(app.appid, total_apps)
                                logger.info(f"进度: 新增{new_count} | 总计{total_apps} | 当前ID:{app.appid}")
                    except Exception as e:
                        logger.error(f"处理应用{app.appid}时出错: {e}")
                
                # 防爬虫限制
                time.sleep(1.5)
                apps_processed += 1
                
                # 每处理50个应用保存进度
                if apps_processed % 50 == 0:
                    save_progress(app.appid, total_apps)
            
            # 更新批次进度
            last_app_id = app_list_data['last_app_id']
            have_more = app_list_data['have_more']
            save_progress(last_app_id, total_apps)
            
            logger.info(f"批次完成: last_app_id={last_app_id} | 还有更多? {have_more}")
            time.sleep(2)  # 批次间小延迟

    except KeyboardInterrupt:
        logger.info("\n用户中断操作，保存进度...")
        save_progress(last_app_id, total_apps)
    except Exception as e:
        logger.critical(f"程序异常终止: {e}", exc_info=True)
        save_progress(last_app_id, total_apps)
        raise
    finally:
        # 重置进度（下次全量扫描）
        save_progress(0, total_apps)
        
        elapsed = time.time() - start_time
        logger.info(f"\n{'='*50}")
        logger.info(f"爬取完成! 新增: {new_count} | 总计: {total_apps} | 耗时: {elapsed:.2f}秒")
        logger.info(f"平均速度: {apps_processed/elapsed:.2f} apps/秒")
        logger.info(f"{'='*50}")

if __name__ == "__main__":
    logger.info("===== Steam爬虫启动 =====")
    main()
    logger.info("===== Steam爬虫结束 =====")
