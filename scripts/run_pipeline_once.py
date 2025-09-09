#!/usr/bin/env python3
"""
单次运行数据管道脚本
"""

import sys
import os

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from semantic.services.pipeline_service import DataPipelineService
from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager

def main():
    """主函数"""
    try:
        print("正在初始化数据管道...")
        
        # 加载配置
        config = AppConfig.from_env()
        
        # 验证配置
        if not config.validate():
            print("配置验证失败，请检查环境变量")
            return False
        
        print(f"配置加载成功: {config}")
        
        # 获取数据库管理器
        db_manager = get_db_manager()
        
        # 创建管道服务
        pipeline = DataPipelineService(config, db_manager)
        
        # 运行管道
        print("开始执行数据管道...")
        success = pipeline.run_pipeline()
        
        if success:
            print("✅ 数据管道执行成功!")
            
            # 询问是否导出CSV
            export_csv = input("是否导出数据到CSV文件? (y/n): ").lower().strip()
            if export_csv == 'y':
                output_path = input("请输入输出路径 (默认: data/dblp_papers_export.csv): ").strip()
                if not output_path:
                    output_path = "data/dblp_papers_export.csv"
                
                if pipeline.export_to_csv(output_path):
                    print("✅ CSV导出完成!")
                else:
                    print("❌ CSV导出失败!")
            
            return True
        else:
            print("❌ 数据管道执行失败!")
            return False
            
    except KeyboardInterrupt:
        print("\n用户中断执行")
        return False
    except Exception as e:
        print(f"执行失败: {e}")
        return False
    finally:
        # 清理资源
        try:
            db_manager = get_db_manager()
            db_manager.disconnect()
        except:
            pass

if __name__ == "__main__":
    sys.exit(0 if main() else 1)