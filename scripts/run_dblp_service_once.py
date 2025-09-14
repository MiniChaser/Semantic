#!/usr/bin/env python3
"""
Single run data pipeline script
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from semantic.services.pipeline_service import DataPipelineService
from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager

def main():
    """Main function"""
    try:
        print("Initializing data pipeline...")
        
        # Load configuration
        config = AppConfig.from_env()
        
        # Validate configuration
        if not config.validate():
            print("Configuration validation failed, please check environment variables")
            return False
        
        print(f"Configuration loaded successfully: {config}")
        
        # Get Database manager class
        db_manager = get_db_manager()
        
        # Create pipeline service
        pipeline = DataPipelineService(config, db_manager)
        
        # Run pipeline
        print("Starting data pipeline execution...")
        success = pipeline.run_pipeline()
        
        if success:
            print("✅ Data pipeline execution completed successfully!")
            
            # Automatically export to CSV
            output_path = "data/dblp_papers_export.csv"
            if pipeline.export_to_csv(output_path):
                print("✅ CSV export completed!")
            else:
                print("❌ CSV export failed!")
            
            return True
        else:
            print("❌ Data pipeline execution failed!")
            return False
            
    except KeyboardInterrupt:
        print("\nUser interrupted execution")
        return False
    except Exception as e:
        print(f"Execution failed: {e}")
        return False
    finally:
        # Clean up resources
        try:
            db_manager = get_db_manager()
            db_manager.disconnect()
        except:
            pass

if __name__ == "__main__":
    sys.exit(0 if main() else 1)