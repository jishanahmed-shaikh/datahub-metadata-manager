#!/usr/bin/env python3
"""
DataHub Metadata Manager - Main Entry Point
"""
from app import app
from config import FLASK_HOST, FLASK_PORT, FLASK_DEBUG

if __name__ == '__main__':
    print("🚀 Starting DataHub Metadata Manager...")
    print(f"📍 Server will be available at: http://{FLASK_HOST}:{FLASK_PORT}")
    print("📊 Features available:")
    print("   • Catalog/Schema/Table browsing with pagination")
    print("   • Manual metadata entry with tags and descriptions")
    print("   • CSV bulk metadata upload")
    print("   • Smart auto-discovery of missing schemas/tables")
    print("   • DataHub emission with proper tags, domains, and ownership")
    print("   • Session management and data validation")
    print()
    
    app.run(
        debug=FLASK_DEBUG,
        host=FLASK_HOST,
        port=FLASK_PORT
    )