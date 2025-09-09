#!/usr/bin/env python3
"""
Database setup script
Creates all tables, indexes, and triggers for the DBLP semantic processing system
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from semantic.database.connection import get_db_manager
from semantic.database.schema import DatabaseSchema

def main():
    """Main function to setup database"""
    print("🚀 DBLP Semantic Database Setup")
    print("=" * 50)
    
    try:
        # Get database manager
        db_manager = get_db_manager()
        
        # Test connection
        print("📡 Testing database connection...")
        if not db_manager.connect():
            print("❌ Failed to connect to database")
            print("Please check your .env file and ensure PostgreSQL is running")
            return False
        
        if not db_manager.test_connection():
            print("❌ Database connection test failed")
            return False
        
        print("✅ Database connection successful")
        
        # Create schema
        print("\n🏗️ Creating database schema...")
        schema = DatabaseSchema(db_manager)
        
        if schema.create_all_tables():
            print("✅ Database schema created successfully")
            
            # Show table information
            print("\n📊 Table Information:")
            for table in ['dblp_papers', 'dblp_processing_meta', 'scheduler_jobs']:
                info = schema.get_table_info(table)
                if info:
                    print(f"\n📄 Table: {table}")
                    print(f"   Rows: {info['row_count']}")
                    print(f"   Size: {info['table_size']}")
                    print(f"   Columns: {len(info['columns'])}")
                else:
                    print(f"⚠️  Could not get info for table: {table}")
            
            # Migrate legacy timestamps if needed
            print("\n🔄 Migrating legacy timestamp columns...")
            if schema.migrate_legacy_timestamps():
                print("✅ Timestamp migration completed")
            else:
                print("⚠️  Timestamp migration failed (this may be normal for new installations)")
            
            print("\n🎉 Database setup completed successfully!")
            return True
        else:
            print("❌ Failed to create database schema")
            return False
    
    except KeyboardInterrupt:
        print("\n⚠️  Setup interrupted by user")
        return False
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return False
    finally:
        # Clean up
        try:
            db_manager = get_db_manager()
            db_manager.disconnect()
        except:
            pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)