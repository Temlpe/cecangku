import os
import time
import json
import datetime
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import mysql.connector
from mysql.connector import Error
import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry
import warnings

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 忽略SSL警告
warnings.simplefilter('ignore', InsecureRequestWarning)

# 环境变量配置
STEAM_API_KEY = os.environ.get('STEAM_API_KEY')
DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'database': os.environ.get('DB_NAME'),
    'port': int(os.environ.get('DB_PORT', 3306)),
    'charset': 'utf8mb4'
}

# Steam API配置
STEAM_APP_LIST_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
STEAM_APP_DETAILS_URL = "https://store.steampowered.com/api/appdetails?l=english&appids="

# 数据类定义
@dataclass
class App:
    appid: int
    name: str

def create_session_with_retry() -> requests.Session:
    """创建带重试机制的Session"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def create_database_connection() -> Optional[mysql.connector.MySQLConnection]:
    """创建数据库连接"""
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            logger.info("成功连接到数据库")
            return connection
    except Error as e:
        logger.error(f"数据库连接错误: {e}")
    return connection

def create_table_if_not_exists(connection: mysql.connector.MySQLConnection) -> None:
    """创建steam_games表（如果不存在）"""
    create_table_query = """
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
        crawl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '爬取时间',
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        logger.info("steam_games表已准备就绪")
    except Error as e:
        logger.error(f"创建表错误: {e}")

def get_existing_app_ids(connection: mysql.connector.MySQLConnection) -> set:
    """获取数据库中已存在的app_id列表"""
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT app_id FROM steam_games")
        existing_ids = {row[0] for row in cursor.fetchall()}
        logger.info(f"数据库中已有 {len(existing_ids)} 个应用")
        return existing_ids
    except Error as e:
        logger.error(f"获取已存在app_id错误: {e}")
        return set()

def get_last_modified_timestamp(connection: mysql.connector.MySQLConnection) -> Optional[int]:
    """获取最后爬取的时间戳（用于增量更新）"""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT UNIX_TIMESTAMP(MAX(crawl_time)) 
            FROM steam_games 
            WHERE crawl_time IS NOT NULL
        """)
        result = cursor.fetchone()[0]
        return int(result) if result else None
    except Error as e:
        logger.error(f"获取最后更新时间失败: {e}")
        return None

def get_steam_app_list() -> List[App]:
    """获取Steam应用列表（支持分页和增量更新）"""
    if not STEAM_API_KEY:
        logger.error("STEAM_API_KEY 未配置！")
        return []
    
    all_apps: List[App] = []
    last_app_id = 0
    have_more = True
    session = create_session_with_retry()
    
    # 获取增量更新的时间戳
    connection = create_database_connection()
    if_modified_since = get_last_modified_timestamp(connection) if connection else None
    if connection:
        connection.close()
    
    try:
        while have_more:
            params = {
                'key': STEAM_API_KEY,
                'include_games': True,
                'include_dlc': True,  # 可选：是否包含DLC
                'include_software': False,  # 可选：是否包含软件
                'include_videos': False,    # 可选：是否包含视频
                'include_hardware': False,  # 可选：是否包含硬件
                'max_results': 50000,
                'last_appid': last_app_id
            }
            
            # 增量更新参数（只获取更新的应用）
            if if_modified_since:
                params['if_modified_since'] = if_modified_since
            
            logger.info(f"获取应用列表 - last_appid: {last_app_id}, if_modified_since: {if_modified_since}")
            
            response = session.get(
                STEAM_APP_LIST_URL, 
                params=params, 
                timeout=(10, 30),
                verify=False
            )
            response.raise_for_status()
            data = response.json()
            
            if not data or 'response' not in data:
                logger.error("API返回格式错误")
                break
            
            response_data = data['response']
            apps_data = response_data.get('apps', [])
            
            if not apps_data:
                logger.info("没有更多应用数据")
                break
            
            # 转换为App对象列表
            apps = [App(app['appid'], app['name']) for app in apps_data]
            all_apps.extend(apps)
            
            # 更新分页参数
            last_app_id = response_data.get('last_appid', 0)
            have_more = response_data.get('have_more_results', False)
            
            logger.info(f"本次获取 {len(apps)} 个应用，累计 {len(all_apps)} 个")
            
            # 避免请求过快
            time.sleep(1)
            
    except Exception as e:
        logger.error(f"获取应用列表失败: {e}")
    finally:
        session.close()
    
    logger.info(f"共获取到 {len(all_apps)} 个应用")
    return all_apps

def get_app_details(app_id: int) -> Optional[Dict[str, Any]]:
    """获取单个App的详细信息"""
    session = create_session_with_retry()
    try:
        url = f"{STEAM_APP_DETAILS_URL}{app_id}"
        response = session.get(url, timeout=(10, 30), verify=False)
        response.raise_for_status()
        data = response.json()
        
        if str(app_id) in data and data[str(app_id)]['success']:
            return data[str(app_id)]['data']
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"获取App {app_id} 详情失败: {e}")
        return None
    except json.JSONDecodeError:
        logger.warning(f"解析App {app_id} 数据失败")
        return None
    finally:
        session.close()

def parse_app_data(app_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """解析App数据为数据库插入格式"""
    # 过滤非游戏/DLC类型（根据需求调整）
    app_type = app_data.get('type')
    if app_type not in ['game', 'dlc', 'demo']:
        return None
    
    # 处理价格信息
    price_final = None
    price_original = None
    discount_percent = None
    
    if 'price_overview' in app_data:
        price_info = app_data['price_overview']
        # 转换为人民币（Steam API返回的是美分）
        try:
            price_final = price_info.get('final', 0) / 100 * 7.2  # 可替换为实时汇率
            price_original = price_info.get('initial', 0) / 100 * 7.2
            discount_percent = price_info.get('discount_percent')
        except (TypeError, ValueError):
            pass
    
    # 处理发布日期
    release_date = None
    if 'release_date' in app_data and app_data['release_date'].get('date'):
        date_str = app_data['release_date']['date']
        date_formats = ['%b %d, %Y', '%d %b, %Y', '%Y-%m-%d', '%b %Y', '%Y']
        for fmt in date_formats:
            try:
                release_date = datetime.datetime.strptime(date_str, fmt).date()
                break
            except ValueError:
                continue
    
    # 处理开发商和发行商
    developers = ', '.join(app_data.get('developers', [])) if 'developers' in app_data else None
    publishers = ', '.join(app_data.get('publishers', [])) if 'publishers' in app_data else None
    
    # 处理游戏标签
    genres = None
    if 'genres' in app_data:
        try:
            genres = ', '.join([g['description'] for g in app_data['genres']])
        except (KeyError, TypeError):
            pass
    
    # 处理支持平台
    platforms = []
    if 'platforms' in app_data:
        if app_data['platforms'].get('windows'):
            platforms.append('Windows')
        if app_data['platforms'].get('mac'):
            platforms.append('macOS')
        if app_data['platforms'].get('linux'):
            platforms.append('Linux')
    platforms_str = ', '.join(platforms) if platforms else None
    
    return {
        'app_id': app_data.get('steam_appid'),
        'name': app_data.get('name'),
        'type': app_type,
        'is_free': app_data.get('is_free'),
        'price_final': price_final,
        'price_original': price_original,
        'discount_percent': discount_percent,
        'release_date': release_date,
        'developers': developers,
        'publishers': publishers,
        'genres': genres,
        'platforms': platforms_str,
        'short_description': app_data.get('short_description'),
        'full_description': app_data.get('about_the_game'),
        'header_image': app_data.get('header_image'),
        'crawl_time': datetime.datetime.now()
    }

def upsert_app_data(connection: mysql.connector.MySQLConnection, app_data: Dict[str, Any]) -> bool:
    """插入或更新App数据"""
    if not app_data or not app_data.get('app_id'):
        return False
    
    upsert_query = """
    INSERT INTO steam_games (
        app_id, name, type, is_free, price_final, price_original,
        discount_percent, release_date, developers, publishers,
        genres, platforms, short_description, full_description,
        header_image, crawl_time
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
        crawl_time = VALUES(crawl_time);
    """
    
    values = (
        app_data['app_id'],
        app_data['name'],
        app_data['type'],
        app_data['is_free'],
        app_data['price_final'],
        app_data['price_original'],
        app_data['discount_percent'],
        app_data['release_date'],
        app_data['developers'],
        app_data['publishers'],
        app_data['genres'],
        app_data['platforms'],
        app_data['short_description'],
        app_data['full_description'],
        app_data['header_image'],
        app_data['crawl_time']
    )
    
    try:
        cursor = connection.cursor()
        cursor.execute(upsert_query, values)
        connection.commit()
        
        action = "更新" if cursor.rowcount == 0 else "插入"
        logger.info(f"{action}成功: {app_data['app_id']} - {app_data['name']}")
        return True
    except Error as e:
        logger.error(f"操作App {app_data['app_id']} 错误: {e}")
        connection.rollback()
        return False

def main():
    """主函数"""
    # 初始化数据库连接
    connection = create_database_connection()
    if not connection:
        logger.error("无法连接数据库，退出程序")
        return
    
    try:
        # 确保表存在
        create_table_if_not_exists(connection)
        
        # 获取已存在的app_id
        existing_app_ids = get_existing_app_ids(connection)
        
        # 获取Steam应用列表
        app_list = get_steam_app_list()
        if not app_list:
            logger.warning("未获取到任何应用列表")
            return
        
        # 筛选新应用（增量更新）
        new_apps = [app for app in app_list if app.appid not in existing_app_ids]
        logger.info(f"发现 {len(new_apps)} 个新应用需要处理")
        
        # 配置爬取参数
        max_requests = int(os.environ.get('MAX_REQUESTS', 1000))
        delay = float(os.environ.get('REQUEST_DELAY', 2))
        batch_size = 50
        
        # 处理新应用
        processed = 0
        success = 0
        failed = 0
        
        for idx, app in enumerate(new_apps[:max_requests]):
            processed += 1
            
            # 获取应用详情
            app_details = get_app_details(app.appid)
            if not app_details:
                failed += 1
                time.sleep(delay)
                continue
            
            # 解析数据
            parsed_data = parse_app_data(app_details)
            if not parsed_data:
                failed += 1
                time.sleep(delay)
                continue
            
            # 插入/更新数据库
            if upsert_app_data(connection, parsed_data):
                success += 1
            else:
                failed += 1
            
            # 打印进度
            if (idx + 1) % batch_size == 0:
                logger.info(f"进度: {idx + 1}/{len(new_apps[:max_requests])} - 成功: {success}, 失败: {failed}")
            
            # 请求延迟
            time.sleep(delay)
        
        # 最终统计
        logger.info(f"""
        处理完成！
        总数: {processed}
        成功: {success}
        失败: {failed}
        """)
        
    finally:
        # 关闭数据库连接
        if connection.is_connected():
            connection.close()
            logger.info("数据库连接已关闭")

if __name__ == "__main__":
    main()
