import os
import time
import json
import datetime
import mysql.connector
from mysql.connector import Error
import requests
from urllib3.exceptions import InsecureRequestWarning
import warnings

# 忽略SSL警告
warnings.simplefilter('ignore', InsecureRequestWarning)

# 从环境变量获取数据库配置
DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'database': os.environ.get('DB_NAME'),
    'port': 3306,
    'charset': 'utf8mb4'
}

# Steam API相关配置
STEAM_APP_LIST_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
STEAM_APP_DETAILS_URL = "https://store.steampowered.com/api/appdetails?l=schinese&appids="

def create_database_connection():
    """创建数据库连接"""
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print("成功连接到数据库")
            return connection
    except Error as e:
        print(f"数据库连接错误: {e}")
    return connection

def create_table_if_not_exists(connection):
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
        crawl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '爬取时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        print("steam_games表已准备就绪")
    except Error as e:
        print(f"创建表错误: {e}")

def get_existing_app_ids(connection):
    """获取数据库中已存在的app_id列表"""
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT app_id FROM steam_games")
        existing_ids = [row[0] for row in cursor.fetchall()]
        return set(existing_ids)
    except Error as e:
        print(f"获取已存在app_id错误: {e}")
        return set()

def get_steam_app_list():
    """从Steam API获取所有App列表"""
    try:
        response = requests.get(STEAM_APP_LIST_URL, verify=False)
        response.raise_for_status()
        data = response.json()
        return data['applist']['apps']
    except requests.exceptions.RequestException as e:
        print(f"获取App列表失败: {e}")
        return []

def get_app_details(app_id):
    """获取单个App的详细信息"""
    try:
        url = f"{STEAM_APP_DETAILS_URL}{app_id}"
        response = requests.get(url, verify=False)
        response.raise_for_status()
        data = response.json()
        
        if str(app_id) in data and data[str(app_id)]['success']:
            return data[str(app_id)]['data']
        return None
    except requests.exceptions.RequestException as e:
        print(f"获取App {app_id} 详情失败: {e}")
        return None
    except json.JSONDecodeError:
        print(f"解析App {app_id} 数据失败")
        return None

def parse_app_data(app_data):
    """解析App数据为数据库插入格式"""
    # 处理价格信息
    price_final = None
    price_original = None
    discount_percent = None
    
    if 'price_overview' in app_data:
        price_info = app_data['price_overview']
        # 转换为人民币（Steam API返回的是美分，需要转换）
        price_final = price_info.get('final', 0) / 100 * 7.2  # 简单汇率转换，实际可能需要更精确的方式
        price_original = price_info.get('initial', 0) / 100 * 7.2
        discount_percent = price_info.get('discount_percent')
    
    # 处理发布日期
    release_date = None
    if 'release_date' in app_data and app_data['release_date'].get('date'):
        try:
            release_date = datetime.datetime.strptime(
                app_data['release_date']['date'], 
                '%b %d, %Y'
            ).date()
        except ValueError:
            try:
                release_date = datetime.datetime.strptime(
                    app_data['release_date']['date'], 
                    '%d %b, %Y'
                ).date()
            except ValueError:
                release_date = None
    
    # 处理开发商和发行商（转为逗号分隔的字符串）
    developers = ', '.join(app_data.get('developers', [])) if 'developers' in app_data else None
    publishers = ', '.join(app_data.get('publishers', [])) if 'publishers' in app_data else None
    
    # 处理游戏标签
    genres = ', '.join([g['description'] for g in app_data.get('genres', [])]) if 'genres' in app_data else None
    
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
        'type': app_data.get('type'),
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

def insert_app_data(connection, app_data):
    """将App数据插入数据库"""
    insert_query = """
    INSERT INTO steam_games (
        app_id, name, type, is_free, price_final, price_original,
        discount_percent, release_date, developers, publishers,
        genres, platforms, short_description, full_description,
        header_image, crawl_time
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        cursor.execute(insert_query, values)
        connection.commit()
        print(f"成功插入App: {app_data['app_id']} - {app_data['name']}")
        return True
    except Error as e:
        if "Duplicate entry" not in str(e):  # 忽略重复插入错误
            print(f"插入App {app_data['app_id']} 错误: {e}")
        connection.rollback()
        return False

def main():
    # 连接数据库
    connection = create_database_connection()
    if not connection:
        return
    
    # 确保表存在
    create_table_if_not_exists(connection)
    
    # 获取已存在的app_id
    existing_app_ids = get_existing_app_ids(connection)
    print(f"数据库中已有 {len(existing_app_ids)} 个App")
    
    # 获取Steam所有App列表
    print("获取Steam App列表...")
    app_list = get_steam_app_list()
    if not app_list:
        connection.close()
        return
    
    print(f"共获取到 {len(app_list)} 个App，开始检查新App...")
    
    # 筛选出新的AppID
    new_apps = [app for app in app_list if app['appid'] not in existing_app_ids]
    print(f"发现 {len(new_apps)} 个新App，开始获取详情...")
    
    # 为了避免请求过于频繁被Steam封禁，添加延迟和请求限制
    max_requests_per_run = 1000  # 每次运行最多请求1000个App
    delay_between_requests = 2  # 每个请求之间的延迟（秒）
    
    processed_count = 0
    success_count = 0
    
    for app in new_apps[:max_requests_per_run]:
        app_id = app['appid']
        processed_count += 1
        
        # 获取App详情
        app_details = get_app_details(app_id)
        if not app_details:
            time.sleep(delay_between_requests)
            continue
        
        # 解析数据并插入数据库
        parsed_data = parse_app_data(app_details)
        if insert_app_data(connection, parsed_data):
            success_count += 1
        
        # 避免请求过于频繁
        time.sleep(delay_between_requests)
        
        # 打印进度
        if processed_count % 50 == 0:
            print(f"已处理 {processed_count}/{len(new_apps[:max_requests_per_run])} 个App，成功插入 {success_count} 个")
    
    print(f"处理完成，共处理 {processed_count} 个新App，成功插入 {success_count} 个")
    
    # 关闭数据库连接
    connection.close()
    print("数据库连接已关闭")

if __name__ == "__main__":
    main()
