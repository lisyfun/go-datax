#!/usr/bin/env python3
"""
生成 500 万测试数据的 Python 脚本
使用批量插入提高性能

使用方法:
1. 安装依赖: pip install mysql-connector-python
2. 运行脚本: python3 scripts/generate_5m_data.py
"""

import mysql.connector
import random
import string
import time
from datetime import datetime

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 13306,
    'user': 'root',
    'password': '123456',
    'database': 'datax_source',
    'charset': 'utf8mb4'
}

# 配置参数
TOTAL_RECORDS = 5000000  # 500 万条记录
BATCH_SIZE = 10000       # 每批插入 1 万条
COMMIT_INTERVAL = 10     # 每 10 批提交一次

def generate_random_phone():
    """生成随机手机号"""
    return f"138{random.randint(10000000, 99999999)}"

def generate_random_password():
    """生成随机密码"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=12))

def generate_batch_data(start_id, batch_size):
    """生成一批数据"""
    data = []
    for i in range(batch_size):
        user_id = start_id + i
        username = f"user_{user_id:010d}"
        email = f"user_{user_id:010d}@example.com"
        password = generate_random_password()
        full_name = f"User {user_id:010d}"
        phone = generate_random_phone()
        status = 1 if random.random() > 0.1 else 0  # 90% 活跃用户
        
        data.append((username, email, password, full_name, phone, status))
    
    return data

def create_connection():
    """创建数据库连接"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        print(f"数据库连接失败: {e}")
        return None

def clear_table(cursor):
    """清空表"""
    print("清空 users 表...")
    cursor.execute("TRUNCATE TABLE users")
    print("表已清空")

def insert_batch_data(cursor, data):
    """批量插入数据"""
    sql = """
    INSERT INTO users (username, email, password, full_name, phone, status) 
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(sql, data)

def main():
    """主函数"""
    print(f"开始生成 {TOTAL_RECORDS:,} 条测试数据...")
    print(f"批次大小: {BATCH_SIZE:,}")
    print(f"提交间隔: 每 {COMMIT_INTERVAL} 批")
    
    # 创建数据库连接
    conn = create_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        # 清空表
        clear_table(cursor)
        
        # 关闭自动提交
        conn.autocommit = False
        
        # 计算批次数
        total_batches = (TOTAL_RECORDS + BATCH_SIZE - 1) // BATCH_SIZE
        
        start_time = time.time()
        inserted_records = 0
        
        print(f"\n开始插入数据，总共 {total_batches} 批...")
        
        for batch_num in range(1, total_batches + 1):
            batch_start_time = time.time()
            
            # 计算当前批次的起始ID和大小
            start_id = (batch_num - 1) * BATCH_SIZE + 1
            current_batch_size = min(BATCH_SIZE, TOTAL_RECORDS - inserted_records)
            
            # 生成数据
            data = generate_batch_data(start_id, current_batch_size)
            
            # 插入数据
            insert_batch_data(cursor, data)
            inserted_records += current_batch_size
            
            # 定期提交
            if batch_num % COMMIT_INTERVAL == 0 or batch_num == total_batches:
                conn.commit()
                
                batch_end_time = time.time()
                elapsed_time = time.time() - start_time
                batch_time = batch_end_time - batch_start_time
                
                # 计算进度和速度
                progress = (inserted_records / TOTAL_RECORDS) * 100
                records_per_second = inserted_records / elapsed_time if elapsed_time > 0 else 0
                
                print(f"批次 {batch_num:4d}/{total_batches} | "
                      f"已插入: {inserted_records:8,} | "
                      f"进度: {progress:5.1f}% | "
                      f"速度: {records_per_second:8,.0f} 条/秒 | "
                      f"批次耗时: {batch_time:.2f}s")
        
        # 最终统计
        total_time = time.time() - start_time
        avg_speed = TOTAL_RECORDS / total_time if total_time > 0 else 0
        
        print(f"\n数据生成完成！")
        print(f"总记录数: {TOTAL_RECORDS:,}")
        print(f"总耗时: {total_time:.2f} 秒")
        print(f"平均速度: {avg_speed:,.0f} 条/秒")
        
        # 验证数据
        cursor.execute("SELECT COUNT(*) FROM users")
        actual_count = cursor.fetchone()[0]
        print(f"实际插入记录数: {actual_count:,}")
        
        # 显示状态统计
        cursor.execute("""
            SELECT 
                COUNT(CASE WHEN status = 1 THEN 1 END) AS active_users,
                COUNT(CASE WHEN status = 0 THEN 1 END) AS inactive_users
            FROM users
        """)
        active, inactive = cursor.fetchone()
        print(f"活跃用户: {active:,}, 非活跃用户: {inactive:,}")
        
        # 显示前 5 条记录
        print("\n前 5 条记录示例:")
        cursor.execute("SELECT id, username, email, full_name, status FROM users ORDER BY id LIMIT 5")
        for row in cursor.fetchall():
            print(f"ID: {row[0]}, 用户名: {row[1]}, 邮箱: {row[2]}, 姓名: {row[3]}, 状态: {row[4]}")
            
    except mysql.connector.Error as e:
        print(f"数据库操作失败: {e}")
        conn.rollback()
    except KeyboardInterrupt:
        print("\n\n用户中断操作，正在回滚...")
        conn.rollback()
    except Exception as e:
        print(f"发生错误: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("数据库连接已关闭")

if __name__ == "__main__":
    main()
