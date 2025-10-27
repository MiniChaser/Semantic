import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os

# ================ 配置数据库连接 ===================
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 5432,
    'dbname': 'atip_db',
    'user': 'chenchao',
    'password': 'chenchao'
}

# 输出目录
OUTPUT_DIR = 'partitioned_csv_output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 主表名（分区表名）
MAIN_TABLE = 'dataset_paper'

# ================ 获取分区表信息 ===================
def get_partitions(conn, main_table):
    query = """
    SELECT c.relname AS partition_name
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_class p ON p.oid = i.inhparent
    WHERE p.relname = %s
    ORDER BY c.relname;
    """
    with conn.cursor() as cur:
        cur.execute(query, (main_table,))
        partitions = [row[0] for row in cur.fetchall()]
    return partitions

# ================ 导出单个分区 ===================
def export_partition_to_csv(engine, partition_name, output_dir):
    output_file = os.path.join(output_dir, f"{partition_name}.csv")
    query = f"SELECT * FROM {partition_name};"
    
    try:
        df = pd.read_sql_query(query, engine)
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"✅ 导出完成: {output_file} (共 {len(df)} 行)")
    except Exception as e:
        print(f"❌ 导出失败 {partition_name}: {str(e)}")

# ================ 主程序 ===================
def main():
    # 建立连接
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("🔗 数据库连接成功")

        # 使用 SQLAlchemy engine（pandas 需要）
        engine_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@" \
                     f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        engine = create_engine(engine_url)

        # 获取所有分区
        partitions = get_partitions(conn, MAIN_TABLE)
        if not partitions:
            print(f"⚠️ 未找到表 '{MAIN_TABLE}' 的任何分区。请检查表名是否正确，是否为分区表。")
            return

        print(f"📊 找到 {len(partitions)} 个分区: {partitions}")

        # 逐个导出
        for partition in partitions:
            export_partition_to_csv(engine, partition, OUTPUT_DIR)

        print(f"🎉 所有分区数据已导出到目录: {OUTPUT_DIR}")

    except Exception as e:
        print(f"❌ 数据库错误: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()
        if 'engine' in locals():
            engine.dispose()

if __name__ == '__main__':
    main()