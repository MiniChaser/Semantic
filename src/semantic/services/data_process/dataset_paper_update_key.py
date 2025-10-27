
import psycopg2
from psycopg2.extras import RealDictCursor

from semantic.utils.title_normalizer import TitleNormalizer


# ==================== 配置 ====================
DB_CONFIG = {
    'host': '114.132.124.54',
    'port': 5432,
    'dbname': 'atip_db',
    'user': 'chenchao',
    'password': 'chenchao'
}

MAIN_TABLE = 'dataset_papers'        # 分区主表名
PARTITIONED_COLUMN = 'id'            # 用于排序分页的列（建议是主键）
PAGE_SIZE = 1000                     # 每页1000条

# 初始化 TitleNormalizer
title_normalizer = TitleNormalizer()

# ==================== 获取所有子分区 ====================
def get_partitions(conn, main_table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.relname AS partition_name
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            WHERE p.relname = %s
            ORDER BY c.relname;
        """, (main_table,))
        return [row[0] for row in cur.fetchall()]

# ==================== 处理单个分区：分页查询 + 更新所有记录 ====================
def process_partition(conn, partition_name):
    print(f"\n🔄 开始处理分区: {partition_name}")

    # 查询语句：不再过滤 NULL，查询所有记录
    select_sql = f"""
        SELECT id, title 
        FROM {partition_name}
        ORDER BY {PARTITIONED_COLUMN}
        LIMIT %s OFFSET %s
    """

    # 更新语句
    update_sql = f"""
        UPDATE {partition_name} 
        SET title_key = %s 
        WHERE id = %s
    """

    offset = 0
    total_updated = 0

    while True:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(select_sql, (PAGE_SIZE, offset))
            rows = cur.fetchall()

        if not rows:
            break  # 无更多数据

        # 准备更新数据
        update_data = []
        for row in rows:
            title = row['title'] or ''
            title_key = title_normalizer.normalize(title)
            update_data.append((title_key, row['id']))  # (title_key, id)

        # 批量更新
        with conn.cursor() as cur:
            cur.executemany(update_sql, update_data)
            conn.commit()

        count = len(update_data)
        total_updated += count
        print(f"  ✅ 已更新 {count} 条 (偏移: {offset})")

        offset += PAGE_SIZE

    print(f"✅ 分区 {partition_name} 处理完成，共更新 {total_updated} 条记录。")

# ==================== 主函数 ====================
def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("🔗 数据库连接成功")

        partitions = get_partitions(conn, MAIN_TABLE)
        if not partitions:
            print(f"❌ 未找到表 '{MAIN_TABLE}' 的任何分区。")
            return

        print(f"📊 共发现 {len(partitions)} 个分区: {partitions}")

        # 逐个处理每个分区
        for partition in partitions:
            try:
                process_partition(conn, partition)
            except Exception as e:
                print(f"❌ 处理分区 {partition} 时出错: {str(e)}")
                conn.rollback()  # 出错回滚，继续下一个分区

    except Exception as e:
        print(f"❌ 数据库错误: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    main()