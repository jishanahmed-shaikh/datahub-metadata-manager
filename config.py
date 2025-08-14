"""
Configuration file for DataHub Metadata Manager
"""
import os

# Trino Configuration
TRINO_HOST = os.getenv('TRINO_HOST', '3.108.199.0')
TRINO_PORT = int(os.getenv('TRINO_PORT', 32092))
TRINO_USER = os.getenv('TRINO_USER', 'root')

# DataHub Configuration
DATAHUB_GMS = os.getenv('DATAHUB_GMS', 'http://localhost:8080')
PLATFORM = os.getenv('DATAHUB_PLATFORM', 'trino')
PLATFORM_INSTANCE = os.getenv('DATAHUB_PLATFORM_INSTANCE', 'trino-default')
ENV = os.getenv('DATAHUB_ENV', 'DEV')
OWNER_URN = os.getenv('DATAHUB_OWNER_URN', 'urn:li:corpuser:data_engineer')

# Flask Configuration
FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('FLASK_PORT', 5000))
FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')

# Upload Configuration
UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'uploads')
MAX_CONTENT_LENGTH = int(os.getenv('MAX_CONTENT_LENGTH', 16 * 1024 * 1024))  # 16MB

# Predefined tags
TABLE_TAGS = [
    "PII", "Transactional", "Master Data", "Reference", 
    "Analytical", "Staging", "Archive", "Sensitive"
]

COLUMN_TAGS = [
    "Primary Key", "Foreign Key", "PII", "Financial", 
    "Business", "Temporal", "Metadata", "Calculated", 
    "Sensitive", "Encrypted"
]