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
from mysql.connector import Error, errorcode

# 配置日志（GitHub Actions需要详细日志）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# -------------------------- 配置读取 --------------------------
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4',
    'connect_timeout': 15,  # 延长连接超时时间
    'autocommit': True      # 减少事务开销
}

STEAM_API_KEY = os.getenv('STEAM_API_KEY')

# GitHub Actions 环境检测
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

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

# -------------------------- 数据库连接池 --------------------------
class DBConnectionPool:
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DBConnectionPool, cls).__new__(cls)
        return cls._instance
    
    def get_connection(self, max_retries=5):
        """获取数据库连接（带重试机制）"""
        if self._connection and self._connection.is_connected():
            return self._connection
            
        for attempt in range(max_retries):
            try:
                # 尝试复用现有连接
                if self._connection and self._connection.is_connected():
                    return self._connection
                    
                # 创建新连接
                self._connection = mysql.connector.connect(**DB_CONFIG)
                logger.info("数据库连接成功")
                return self._connection
                
            except Error as err:
                wait_time = 2 ** attempt  # 指数退避
                error_msg = f"数据库连接失败 (尝试 {attempt+1}/{max_retries}): {err}"
                
                # 特别处理错误2013
                if err.errno == errorcode.CR_SERVER_LOST:
                    logger.warning(f"网络中断错误: {error_msg}")
                else:
                    logger.error(error_msg)
                
                if attempt < max_retries - 1:
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.critical("所有重试尝试失败，程序终止")
                    sys.exit(1)
                    
        return None

    def close_connection(self):
        """安全关闭连接"""
        if self._connection and self._connection.is_connected():
            try:
                self._connection.close()
                logger.info("数据库连接已关闭")
            except Error as err:
                logger.error(f"关闭连接时出错: {err}")
            finally:
                self._connection = None

# -------------------------- 数据库操作 --------------------------
def create_tables():
    """创建必要的数据库表"""
    conn = None
    cursor = None
    try:
        conn = DBConnectionPool().get_connection()
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
        
        cursor.execute(create_game_table_sql)
        cursor.execute(create_progress_table_sql)
        cursor.execute(init_progress_sql)
        conn.commit()
        logger.info("数据库表初始化完成")
        
    except Error as err:
        logger.error(f"创建表失败: {err}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        # 不关闭连接，由连接池管理

def get_crawled_appids():
    """获取已爬取的所有AppID"""
    conn = None
    cursor = None
    crawled_ids = set()
    try:
        conn = DBConnectionPool().get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT app_id FROM steam_games;")
        crawled_ids = {row[0] for row in cursor.fetchall()}
        logger.info(f"已爬取应用数量: {len(crawled_ids)}")
        
    except Error as err:
        logger.error(f"获取已爬取ID失败: {err}")
    finally:
        if cursor:
            cursor.close()
    return crawled_ids

# -------------------------- 进度操作 --------------------------
def save_progress(last_app_id, total_apps):
    """保存爬取进度"""
    conn = None
    cursor = None
    try:
        conn = DBConnectionPool().get_connection()
        cursor = conn.cursor()
        update_sql = """
        UPDATE crawl_progress 
        SET last_app_id = %s, total_apps = %s 
        WHERE id = 1;
        """
        cursor.execute(update_sql, (last_app_id, total_apps))
        logger.info(f"进度已保存: last_app_id={last_app_id}, 累计应用数={total_apps}")
        
    except Error as err:
        logger.error(f"保存进度失败: {err}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()

def load_progress():
    """加载爬取进度"""
    conn = None
    cursor = None
    try:
        conn = DBConnectionPool().get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT last_app_id, total_apps FROM crawl_progress WHERE id = 1;")
        result = cursor.fetchone()
        return result if result else (0, 0)
        
    except Error as err:
        logger.error(f"加载进度失败: {err}")
        return 0, 0
    finally:
        if cursor:
            cursor.close()

# -------------------------- Steam API --------------------------
def create_session_with_retry():
    """创建带重试机制的请求会话"""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # 增加重试次数
        backoff_factor=1.5,  # 增加退避时间
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    
    # GitHub Actions特殊配置
    if IS_GITHUB_ACTIONS:
        session.headers.update({
            'User-Agent': 'GitHubActions-Scraper/1.0 (+https://github.com/your-repo)',
            'Accept': 'application/json'
        })
    
    return session

def get_app_list(last_app_id=0):
    """获取应用列表（带重试）"""
    if not STEAM_API_KEY:
        logger.error("STEAM_API_KEY 未配置！")
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
        response = session.get(url, params=params, timeout=(10, 30))
        response.raise_for_status()
        data = response.json()
        
        if not data or 'response' not in data or 'apps' not in data['response']:
            raise ValueError("API返回格式错误")
            
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
    """获取应用详情（带重试）"""
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
        response = session.get(url, params=params, headers=headers, timeout=(10, 30))
        if response.status_code != 200:
            logger.warning(f"应用{appid}详情获取失败: HTTP {response.status_code}")
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
    """保存应用到数据库（带重试）"""
    conn = None
    cursor = None
    try:
        conn = DBConnectionPool().get_connection()
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
        return True
        
    except Error as err:
        logger.error(f"保存应用{app.appid}失败: {err}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        # 不关闭连接，由连接池管理

# -------------------------- 主函数 --------------------------
def main():
    """主执行函数"""
    # 检查必要配置
    required_env = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'STEAM_API_KEY']
    missing = [env for env in required_env if not os.getenv(env)]
    if missing:
        logger.critical(f"缺少必要环境变量: {', '.join(missing)}")
        sys.exit(1)
    
    # 初始化数据库
    create_tables()
    
    # 获取已爬取数据
    crawled_appids = get_crawled_appids()
    
    # 加载进度
    last_app_id, total_apps = load_progress()
    have_more = True
    new_count = 0
    batch_count = 0
    
    try:
        while have_more:
            batch_count += 1
            logger.info(f"开始处理批次 #{batch_count} (last_app_id={last_app_id})")
            
            # 获取应用列表（带重试）
            app_list_data = get_app_list(last_app_id)
            if not app_list_data:
                logger.warning("获取应用列表失败，尝试重试...")
                time.sleep(5)
                continue
            
            # 筛选新增应用
            new_apps = [app for app in app_list_data['apps'] if app.appid not in crawled_appids]
            logger.info(f"批次发现 {len(new_apps)} 个新应用 (共 {len(app_list_data['apps'])} 个)")
            
            # 处理新应用
            for i, app in enumerate(new_apps):
                try:
                    details = get_app_details(app.appid)
                    if details:
                        app = parse_app_details(app, details)
                        if save_app_to_db(app):
                            new_count += 1
                            total_apps += 1
                            crawled_appids.add(app.appid)
                            
                            # 每10个应用报告一次
                            if new_count % 10 == 0:
                                logger.info(f"新增 {new_count} 个应用 | 最后ID: {app.appid}")
                    
                    # 防限流：GitHub Actions需要更长的间隔
                    time.sleep(1.5 if IS_GITHUB_ACTIONS else 1)
                    
                except Exception as e:
                    logger.error(f"处理应用 {app.appid} 时出错: {str(e)}")
            
            # 更新进度
            last_app_id = app_list_data['last_app_id']
            have_more = app_list_data['have_more']
            save_progress(last_app_id, total_apps)
            
            logger.info(f"批次 #{batch_count} 完成 | last_app_id={last_app_id} | 还有更多: {have_more}")
            
            # GitHub Actions特殊延迟
            if IS_GITHUB_ACTIONS:
                time.sleep(2)
                
    except KeyboardInterrupt:
        logger.info("\n手动中断，保存进度...")
        save_progress(last_app_id, total_apps)
    except Exception as e:
        logger.exception(f"程序异常终止: {str(e)}")
        save_progress(last_app_id, total_apps)
        sys.exit(1)
    
    # 重置进度（下次从头扫描）
    save_progress(0, total_apps)
    logger.info(f"\n爬取完成! 新增: {new_count} | 累计: {total_apps}")
    logger.info("进度已重置，下次将扫描所有AppID")

if __name__ == "__main__":
    try:
        main()
    finally:
        # 确保连接池关闭
        DBConnectionPool().close_connection()
