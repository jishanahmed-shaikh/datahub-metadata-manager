"""
Configuration file for DataHub Metadata Manager
"""
import os

# Trino Configuration
TRINO_HOST = os.getenv('TRINO_HOST')
TRINO_PORT = int(os.getenv('TRINO_PORT'))
TRINO_USER = os.getenv('TRINO_USER')

# DataHub Configuration
DATAHUB_GMS = os.getenv('DATAHUB_GMS')
PLATFORM = os.getenv('DATAHUB_PLATFORM')
PLATFORM_INSTANCE = os.getenv('DATAHUB_PLATFORM_INSTANCE')
ENV = os.getenv('DATAHUB_ENV')
OWNER_URN = os.getenv('DATAHUB_OWNER_URN')

# Flask Configuration
FLASK_HOST = os.getenv('FLASK_HOST')
FLASK_PORT = int(os.getenv('FLASK_PORT'))
FLASK_DEBUG = os.getenv('FLASK_DEBUG').lower() == 'true'
SECRET_KEY = os.getenv('SECRET_KEY')

# Upload Configuration
UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER')
MAX_CONTENT_LENGTH = int(os.getenv('MAX_CONTENT_LENGTH'))  # 16MB

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