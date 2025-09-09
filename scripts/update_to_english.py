#!/usr/bin/env python3
"""
Script to update all Python files to use English comments and update time column usage
"""

import os
import re
import sys
import glob

# Define translation mappings for common Chinese comments/strings
TRANSLATIONS = {
    # Common comments
    "Database connection management module": "Database connection management module",
    "Provides unified database connection and configuration management": "Provides unified database connection and configuration management",
    "Database configuration class": "Database configuration class",
    "Database manager class": "Database manager class",
    "Get database connection string": "Get database connection string",
    "Get database connection parameters": "Get database connection parameters",
    "Setup logger": "Setup logger",
    "Establish database connection": "Establish database connection",
    "Close database connection": "Close database connection",
    "Get database connection": "Get database connection",
    "Context manager for database cursor": "Context manager for database cursor",
    "Test database connection": "Test database connection",
    "Execute SQL query": "Execute SQL query",
    "Execute query and return single record": "Execute query and return single record",
    "Execute query and return all records": "Execute query and return all records",
    "Support for with statement": "Support for with statement",
    "全局Database manager class实例": "Global database manager instance",
    "获取全局Database manager class实例": "Get global database manager instance",
    "重置全局Database manager class实例": "Reset global database manager instance",
    
    # DBLP Service comments
    "DBLP data processing service": "DBLP data processing service",
    "Provides DBLP data download, parsing and processing functionality": "Provides DBLP data download, parsing and processing functionality",
    "DBLP data downloader": "DBLP data downloader",
    "Download DBLP XML.gz file": "Download DBLP XML.gz file",
    "Extract XML.gz file": "Extract XML.gz file",
    "Cleanup downloaded files": "Cleanup downloaded files",
    "DBLP XML parser": "DBLP XML parser",
    "Parse DBLP XML file": "Parse DBLP XML file",
    "Extract single paper data": "Extract single paper data",
    "Safely extract element text": "Safely extract element text",
    "Clean text data": "Clean text data",
    "Extract DOI from ee element": "Extract DOI from ee element",
    "Get processing statistics": "Get processing statistics",
    "Reset statistics": "Reset statistics",
    
    # Error messages
    "Database connection established": "Database connection established",
    "Failed to connect to database": "Failed to connect to database",
    "Database connection closed": "Database connection closed",
    "无法Establish database connection": "Unable to establish database connection",
    "Database operation failed": "Database operation failed",
    "Connection test failed": "Connection test failed",
    "Query execution failed": "Query execution failed",
    "Download failed": "Download failed",
    "Extraction failed": "Extraction failed",
    "XML parsing failed": "XML parsing failed",
    "Failed to insert paper": "Failed to insert paper",
    "Batch operation failed": "Batch operation failed",
    "Failed to get paper": "Failed to get paper",
    "Failed to get papers by venue": "Failed to get papers by venue",
    "Failed to get last update time": "Failed to get last update time",
    "Failed to get statistics": "Failed to get statistics",
    "Failed to create tables": "Failed to create tables",
    
    # Success messages
    "Parsing completed": "Parsing completed",
    "Download completed": "Download completed",
    "Extraction completed": "Extraction completed",
    "Batch operation completed": "Batch operation completed",
    
    # Pipeline messages
    "Starting data pipeline execution": "Starting data pipeline execution",
    "Data pipeline execution completed successfully": "Data pipeline execution completed successfully",
    "Data pipeline execution failed": "Data pipeline execution failed",
    "Step 1": "Step 1",
    "Step 2": "Step 2", 
    "Step 3": "Step 3",
    "Step 4": "Step 4",
    "Step 5": "Step 5",
    "Step 6": "Step 6",
    "Step 7": "Step 7",
    "Prepare DBLP data": "Prepare DBLP data",
    "Extract paper data": "Extract paper data",
    "Load papers to database": "Load papers to database",
    "Post processing": "Post processing",
    
    # Scheduler messages
    "Scheduler started": "Scheduler started",
    "Scheduler stopped": "Scheduler stopped",
    "Scheduled task triggered": "Scheduled task triggered",
    "Task executed successfully": "Task executed successfully",
    "Task execution failed": "Task execution failed",
    "Job executed successfully": "Job executed successfully",
    "Job execution failed": "Job execution failed",
    "Job execution missed": "Job execution missed",
}

def translate_chinese_to_english(text):
    """Translate Chinese text to English using predefined mappings"""
    for chinese, english in TRANSLATIONS.items():
        text = text.replace(chinese, english)
    return text

def update_file_with_english(file_path):
    """Update a Python file to use English comments and messages"""
    print(f"Processing: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Track if any changes were made
        original_content = content
        
        # Translate common Chinese phrases
        content = translate_chinese_to_english(content)
        
        # Update time column references from created_at/updated_at to create_time/update_time
        # But keep both for backward compatibility
        patterns_to_update = [
            # SQL column references
            (r'ORDER BY create_time', 'ORDER BY create_time'),
            (r'ORDER BY update_time', 'ORDER BY update_time'),
            (r'WHERE create_time', 'WHERE create_time'),
            (r'WHERE update_time', 'WHERE update_time'),
            (r'SELECT create_time', 'SELECT create_time'),
            (r'SELECT update_time', 'SELECT update_time'),
        ]
        
        for pattern, replacement in patterns_to_update:
            content = re.sub(pattern, replacement, content)
        
        # Write back if changes were made
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"  ✅ Updated: {file_path}")
            return True
        else:
            print(f"  ⏭️ No changes: {file_path}")
            return False
            
    except Exception as e:
        print(f"  ❌ Error processing {file_path}: {e}")
        return False

def main():
    """Main function"""
    if len(sys.argv) > 1:
        # Process specific files
        files = sys.argv[1:]
    else:
        # Process all Python files in src/
        files = glob.glob("src/**/*.py", recursive=True)
        files.extend(glob.glob("scripts/*.py"))
    
    total_files = len(files)
    updated_files = 0
    
    print(f"Processing {total_files} Python files...")
    print("=" * 60)
    
    for file_path in sorted(files):
        if update_file_with_english(file_path):
            updated_files += 1
    
    print("=" * 60)
    print(f"Summary: Updated {updated_files}/{total_files} files")

if __name__ == "__main__":
    main()