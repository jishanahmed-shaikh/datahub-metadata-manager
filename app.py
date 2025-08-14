import logging
import os
import pandas as pd
from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
from werkzeug.utils import secure_filename
from trino.dbapi import connect
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    BooleanTypeClass,
    NumberTypeClass,
    AuditStampClass,
    OtherSchemaClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    GlobalTagsClass,
    TagAssociationClass,
    DomainsClass
)
import datetime
import json

# Flask app setup
app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Create uploads directory
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('datahub_app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import configuration
from config import (
    TRINO_HOST, TRINO_PORT, TRINO_USER,
    DATAHUB_GMS, PLATFORM, PLATFORM_INSTANCE, ENV, OWNER_URN,
    FLASK_HOST, FLASK_PORT, FLASK_DEBUG, SECRET_KEY,
    UPLOAD_FOLDER, MAX_CONTENT_LENGTH, TABLE_TAGS, COLUMN_TAGS
)

# Global variables to store data
current_catalogs = []
current_schemas = []
current_tables = []
current_table_columns = {}
selected_catalog = ""
selected_schema = ""
current_metadata = {}
uploaded_metadata = {}

# Session-based storage to prevent persistence across page reloads
import uuid
session_id = str(uuid.uuid4())

# Tags are now imported from config.py

class TrinoConnector:
    def __init__(self):
        self.conn = None
        self.cursor = None
    
    def connect(self, catalog=None, schema=None):
        try:
            self.conn = connect(
                host=TRINO_HOST,
                port=TRINO_PORT,
                user=TRINO_USER,
                catalog=catalog or "system",
                schema=schema or "information_schema",
            )
            self.cursor = self.conn.cursor()
            logger.info(f"Successfully connected to Trino")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {str(e)}")
            return False
    
    def get_catalogs(self):
        try:
            if not self.connect():
                return []
            query = "SHOW CATALOGS"
            self.cursor.execute(query)
            catalogs = [row[0] for row in self.cursor.fetchall()]
            logger.info(f"Found {len(catalogs)} catalogs")
            return catalogs
        except Exception as e:
            logger.error(f"Failed to fetch catalogs: {str(e)}")
            return []
    
    def get_schemas(self, catalog):
        try:
            if not self.connect(catalog):
                return []
            query = f"SHOW SCHEMAS FROM {catalog}"
            self.cursor.execute(query)
            schemas = [row[0] for row in self.cursor.fetchall()]
            logger.info(f"Found {len(schemas)} schemas in catalog {catalog}")
            return schemas
        except Exception as e:
            logger.error(f"Failed to fetch schemas from catalog {catalog}: {str(e)}")
            return []
    
    def get_tables(self, catalog, schema):
        try:
            if not self.connect(catalog, schema):
                return []
            query = f"SHOW TABLES FROM {catalog}.{schema}"
            self.cursor.execute(query)
            tables = [row[0] for row in self.cursor.fetchall()]
            logger.info(f"Found {len(tables)} tables in {catalog}.{schema}")
            return tables
        except Exception as e:
            logger.error(f"Failed to fetch tables from {catalog}.{schema}: {str(e)}")
            return []
    
    def get_table_columns(self, catalog, schema, table_name):
        try:
            if not self.connect(catalog, schema):
                return []
            query = f"DESCRIBE {catalog}.{schema}.{table_name}"
            self.cursor.execute(query)
            columns = self.cursor.fetchall()
            return [{'name': col[0], 'type': col[1]} for col in columns]
        except Exception as e:
            logger.error(f"Failed to get columns for {catalog}.{schema}.{table_name}: {str(e)}")
            return []
    
    def get_table_summary(self, catalog, schema, table_name):
        try:
            columns = self.get_table_columns(catalog, schema, table_name)
            if not columns:
                return None
            
            # Get row count (optional, might be slow for large tables)
            try:
                count_query = f"SELECT COUNT(*) FROM {catalog}.{schema}.{table_name}"
                self.cursor.execute(count_query)
                row_count = self.cursor.fetchone()[0]
            except:
                row_count = "N/A"
            
            return {
                'table_name': table_name,
                'columns': columns,
                'row_count': row_count
            }
        except Exception as e:
            logger.error(f"Failed to get table summary for {table_name}: {str(e)}")
            return None

trino_connector = TrinoConnector()

def check_missing_schemas_tables(discovered_schemas, discovered_tables):
    """Check which schemas/tables from CSV are not currently loaded"""
    global current_catalogs, current_schemas, current_tables, selected_catalog, selected_schema
    
    missing_info = {
        'has_missing': False,
        'missing_catalogs': [],
        'missing_schemas': [],
        'missing_tables': [],
        'current_catalog': selected_catalog,
        'current_schema': selected_schema,
        'discovered_schemas': list(discovered_schemas),
        'discovered_tables': list(discovered_tables)
    }
    
    try:
        # Check if we need to load catalogs
        if not selected_catalog or not current_catalogs:
            missing_info['missing_catalogs'] = ['Need to load catalogs first']
            missing_info['has_missing'] = True
        
        # Check for missing schemas
        for schema_name in discovered_schemas:
            if schema_name not in current_schemas:
                missing_info['missing_schemas'].append(schema_name)
                missing_info['has_missing'] = True
        
        # Check for missing tables (only if we have a selected schema)
        if selected_schema:
            for table_key in discovered_tables:
                schema_name, table_name = table_key.split('.', 1)
                if schema_name == selected_schema and table_name not in current_tables:
                    missing_info['missing_tables'].append(table_key)
                    missing_info['has_missing'] = True
        else:
            # No schema selected, so all tables are "missing"
            missing_info['missing_tables'] = list(discovered_tables)
            missing_info['has_missing'] = True
        
        logger.info(f"Missing check results: {missing_info}")
        return missing_info
        
    except Exception as e:
        logger.error(f"Error checking missing schemas/tables: {str(e)}")
        missing_info['has_missing'] = True
        missing_info['error'] = str(e)
        return missing_info

def auto_discover_from_csv(discovered_schemas, discovered_tables):
    """Auto-discover and load schemas/tables from CSV that aren't currently loaded"""
    global current_catalogs, current_schemas, current_tables, current_table_columns
    global selected_catalog, selected_schema
    
    results = {
        'new_schemas_found': [],
        'new_tables_found': [],
        'schemas_loaded': [],
        'tables_loaded': [],
        'errors': []
    }
    
    try:
        # If no catalog is selected, try to load catalogs first
        if not selected_catalog and not current_catalogs:
            logger.info("No catalog selected, loading catalogs for auto-discovery")
            current_catalogs = trino_connector.get_catalogs()
            if current_catalogs and 'hive' in current_catalogs:
                selected_catalog = 'hive'  # Default to hive catalog
                logger.info(f"Auto-selected catalog: {selected_catalog}")
        
        # Discover new schemas
        for schema_name in discovered_schemas:
            if schema_name not in current_schemas:
                results['new_schemas_found'].append(schema_name)
                
                # Try to load this schema if we have a catalog
                if selected_catalog:
                    try:
                        schema_tables = trino_connector.get_tables(selected_catalog, schema_name)
                        if schema_tables:  # Schema exists and has tables
                            if schema_name not in current_schemas:
                                current_schemas.append(schema_name)
                                results['schemas_loaded'].append(schema_name)
                            
                            # If this is the first schema or matches current selection, load its tables
                            if not selected_schema or selected_schema == schema_name:
                                selected_schema = schema_name
                                
                                # Load tables and columns for this schema
                                for table_name in schema_tables:
                                    table_key = f"{schema_name}.{table_name}"
                                    if table_name not in current_tables:
                                        current_tables.append(table_name)
                                        results['tables_loaded'].append(table_key)
                                    
                                    # Load columns for this table
                                    if table_name not in current_table_columns:
                                        columns = trino_connector.get_table_columns(selected_catalog, schema_name, table_name)
                                        current_table_columns[table_name] = columns
                                
                                logger.info(f"Auto-loaded schema {schema_name} with {len(schema_tables)} tables")
                    except Exception as e:
                        error_msg = f"Failed to load schema {schema_name}: {str(e)}"
                        results['errors'].append(error_msg)
                        logger.error(error_msg)
        
        # Discover new tables in current schema
        if selected_schema:
            for table_key in discovered_tables:
                schema_name, table_name = table_key.split('.', 1)
                if schema_name == selected_schema and table_name not in current_tables:
                    results['new_tables_found'].append(table_key)
                    
                    try:
                        # Verify table exists in Trino
                        table_columns = trino_connector.get_table_columns(selected_catalog, schema_name, table_name)
                        if table_columns:
                            current_tables.append(table_name)
                            current_table_columns[table_name] = table_columns
                            results['tables_loaded'].append(table_key)
                            logger.info(f"Auto-loaded table {table_key}")
                    except Exception as e:
                        error_msg = f"Failed to load table {table_key}: {str(e)}"
                        results['errors'].append(error_msg)
                        logger.error(error_msg)
        
        logger.info(f"Auto-discovery results: {results}")
        return results
        
    except Exception as e:
        error_msg = f"Auto-discovery failed: {str(e)}"
        results['errors'].append(error_msg)
        logger.error(error_msg)
        return results

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/clear_session', methods=['POST'])
def clear_session():
    """Clear all session data"""
    global current_catalogs, current_schemas, current_tables, current_table_columns
    global selected_catalog, selected_schema, current_metadata, uploaded_metadata
    
    current_catalogs = []
    current_schemas = []
    current_tables = []
    current_table_columns = {}
    selected_catalog = ""
    selected_schema = ""
    current_metadata = {}
    uploaded_metadata = {}
    
    logger.info("Session data cleared - all metadata and selections reset")
    return jsonify({'success': True, 'message': 'Session cleared - all data reset'})

@app.route('/load_catalogs', methods=['POST'])
def load_catalogs():
    global current_catalogs
    try:
        current_catalogs = trino_connector.get_catalogs()
        logger.info(f"Loaded {len(current_catalogs)} catalogs")
        return jsonify({
            'success': True, 
            'message': f'Successfully loaded {len(current_catalogs)} catalogs',
            'catalogs': current_catalogs
        })
    except Exception as e:
        logger.error(f"Error loading catalogs: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/load_schemas', methods=['POST'])
def load_schemas():
    global current_schemas, selected_catalog
    try:
        data = request.json
        catalog = data.get('catalog')
        if not catalog:
            return jsonify({'success': False, 'message': 'Catalog not specified'})
        
        selected_catalog = catalog
        current_schemas = trino_connector.get_schemas(catalog)
        logger.info(f"Loaded {len(current_schemas)} schemas from catalog {catalog}")
        return jsonify({
            'success': True, 
            'message': f'Successfully loaded {len(current_schemas)} schemas from {catalog}',
            'schemas': current_schemas
        })
    except Exception as e:
        logger.error(f"Error loading schemas: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/load_tables', methods=['POST'])
def load_tables():
    global current_tables, current_table_columns, selected_schema
    try:
        data = request.json
        schema = data.get('schema')
        if not schema or not selected_catalog:
            return jsonify({'success': False, 'message': 'Catalog or schema not specified'})
        
        selected_schema = schema
        current_tables = trino_connector.get_tables(selected_catalog, schema)
        
        # Load columns for all tables
        current_table_columns = {}
        for table in current_tables:
            columns = trino_connector.get_table_columns(selected_catalog, schema, table)
            current_table_columns[table] = columns
        
        logger.info(f"Loaded {len(current_tables)} tables from {selected_catalog}.{schema}")
        return jsonify({
            'success': True, 
            'message': f'Successfully loaded {len(current_tables)} tables from {selected_catalog}.{schema}',
            'tables': current_tables,
            'table_columns': current_table_columns
        })
    except Exception as e:
        logger.error(f"Error loading tables: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get_catalogs')
def get_catalogs():
    return jsonify({'catalogs': current_catalogs})

@app.route('/get_schemas')
def get_schemas():
    return jsonify({'schemas': current_schemas, 'selected_catalog': selected_catalog})

@app.route('/get_tables')
def get_tables():
    return jsonify({
        'tables': current_tables, 
        'table_columns': current_table_columns,
        'selected_catalog': selected_catalog,
        'selected_schema': selected_schema
    })

@app.route('/get_table_summary/<table_name>')
def get_table_summary(table_name):
    try:
        if not selected_catalog or not selected_schema:
            return jsonify({'success': False, 'message': 'Catalog or schema not selected'})
        
        summary = trino_connector.get_table_summary(selected_catalog, selected_schema, table_name)
        if summary:
            return jsonify({'success': True, 'summary': summary})
        else:
            return jsonify({'success': False, 'message': 'Failed to get table summary'})
    except Exception as e:
        logger.error(f"Error getting table summary: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get_tags')
def get_tags():
    return jsonify({
        'table_tags': TABLE_TAGS,
        'column_tags': COLUMN_TAGS
    })

@app.route('/get_all_available_tables')
def get_all_available_tables():
    """Get all tables available for emission (from Trino + metadata)"""
    all_tables = set(current_tables)
    
    # Add tables from metadata
    for table_key in list(current_metadata.keys()) + list(uploaded_metadata.keys()):
        if '.' in table_key:
            schema_name, table_name = table_key.split('.', 1)
            if schema_name == selected_schema:
                all_tables.add(table_name)
    
    return jsonify({
        'tables': list(all_tables),
        'trino_tables': current_tables,
        'metadata_tables': [key.split('.', 1)[1] for key in list(current_metadata.keys()) + list(uploaded_metadata.keys()) if '.' in key and key.split('.', 1)[0] == selected_schema]
    })

@app.route('/get_discovery_status')
def get_discovery_status():
    """Get current discovery status after CSV upload"""
    return jsonify({
        'selected_catalog': selected_catalog,
        'selected_schema': selected_schema,
        'current_schemas': current_schemas,
        'current_tables': current_tables,
        'table_columns_count': len(current_table_columns)
    })

@app.route('/load_missing_items', methods=['POST'])
def load_missing_items():
    """Load missing schemas/tables that were identified from CSV upload"""
    try:
        data = request.json
        missing_info = data.get('missing_items', {})
        
        results = {
            'success': False,
            'message': '',
            'loaded_catalogs': [],
            'loaded_schemas': [],
            'loaded_tables': [],
            'errors': []
        }
        
        # Load catalogs if needed
        if missing_info.get('missing_catalogs'):
            try:
                global current_catalogs, selected_catalog
                current_catalogs = trino_connector.get_catalogs()
                if current_catalogs and 'hive' in current_catalogs:
                    selected_catalog = 'hive'
                    results['loaded_catalogs'] = current_catalogs
                    logger.info(f"Loaded catalogs: {current_catalogs}")
            except Exception as e:
                results['errors'].append(f"Failed to load catalogs: {str(e)}")
        
        # Load missing schemas
        missing_schemas = missing_info.get('missing_schemas', [])
        for schema_name in missing_schemas:
            try:
                if selected_catalog:
                    # Verify schema exists
                    schema_tables = trino_connector.get_tables(selected_catalog, schema_name)
                    if schema_tables:
                        if schema_name not in current_schemas:
                            current_schemas.append(schema_name)
                            results['loaded_schemas'].append(schema_name)
                        logger.info(f"Verified schema {schema_name} exists with {len(schema_tables)} tables")
                    else:
                        results['errors'].append(f"Schema {schema_name} not found or has no tables")
            except Exception as e:
                results['errors'].append(f"Failed to verify schema {schema_name}: {str(e)}")
        
        # Load missing tables (for current schema)
        missing_tables = missing_info.get('missing_tables', [])
        tables_to_load = []
        
        for table_key in missing_tables:
            schema_name, table_name = table_key.split('.', 1)
            if schema_name == selected_schema:
                tables_to_load.append(table_name)
        
        if tables_to_load and selected_catalog and selected_schema:
            try:
                # Load all tables for the current schema
                all_schema_tables = trino_connector.get_tables(selected_catalog, selected_schema)
                
                # Load columns for missing tables
                global current_tables, current_table_columns
                for table_name in tables_to_load:
                    if table_name in all_schema_tables:
                        if table_name not in current_tables:
                            current_tables.append(table_name)
                            results['loaded_tables'].append(f"{selected_schema}.{table_name}")
                        
                        # Load columns
                        if table_name not in current_table_columns:
                            columns = trino_connector.get_table_columns(selected_catalog, selected_schema, table_name)
                            current_table_columns[table_name] = columns
                        
                        logger.info(f"Loaded table {selected_schema}.{table_name}")
                    else:
                        results['errors'].append(f"Table {table_name} not found in schema {selected_schema}")
            except Exception as e:
                results['errors'].append(f"Failed to load tables: {str(e)}")
        
        # Determine success
        results['success'] = (len(results['loaded_catalogs']) > 0 or 
                            len(results['loaded_schemas']) > 0 or 
                            len(results['loaded_tables']) > 0)
        
        if results['success']:
            loaded_items = []
            if results['loaded_catalogs']:
                loaded_items.append(f"{len(results['loaded_catalogs'])} catalogs")
            if results['loaded_schemas']:
                loaded_items.append(f"{len(results['loaded_schemas'])} schemas")
            if results['loaded_tables']:
                loaded_items.append(f"{len(results['loaded_tables'])} tables")
            
            results['message'] = f"Successfully loaded: {', '.join(loaded_items)}"
        else:
            results['message'] = "No items were loaded"
        
        logger.info(f"Load missing items results: {results}")
        return jsonify(results)
        
    except Exception as e:
        logger.error(f"Error loading missing items: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Failed to load missing items: {str(e)}',
            'errors': [str(e)]
        })

@app.route('/debug_metadata')
def debug_metadata():
    """Debug endpoint to check metadata state"""
    return jsonify({
        'selected_catalog': selected_catalog,
        'selected_schema': selected_schema,
        'current_tables': current_tables,
        'manual_metadata_keys': list(current_metadata.keys()),
        'uploaded_metadata_keys': list(uploaded_metadata.keys()),
        'manual_metadata': current_metadata,
        'uploaded_metadata': uploaded_metadata
    })

@app.route('/upload_metadata', methods=['POST'])
def upload_metadata():
    global uploaded_metadata
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file selected'})
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'message': 'No file selected'})
        
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            # Read and process CSV
            df = pd.read_csv(filepath)
            logger.info(f"Uploaded CSV with {len(df)} rows and columns: {list(df.columns)}")
            
            # Expected CSV format: SchemaName, Domain, OwnerName, TableName, TableDescription, TableTag, ColumnName, ColumnDescription, ColumnTag, ColumnDataType
            required_columns = ['SchemaName', 'TableName', 'ColumnName', 'ColumnDescription']
            if not all(col in df.columns for col in required_columns):
                return jsonify({
                    'success': False, 
                    'message': f'CSV must contain columns: {", ".join(required_columns)}'
                })
            
            # Process metadata and discover new schemas/tables
            uploaded_metadata = {}
            discovered_schemas = set()
            discovered_tables = set()
            
            for _, row in df.iterrows():
                schema_name = row['SchemaName']
                table_name = row['TableName']
                table_key = f"{schema_name}.{table_name}"
                
                # Track discovered schemas and tables
                discovered_schemas.add(schema_name)
                discovered_tables.add(table_key)
                
                if table_key not in uploaded_metadata:
                    uploaded_metadata[table_key] = {
                        'table_info': {
                            'schema': schema_name,
                            'domain': row.get('Domain', ''),
                            'owner': row.get('OwnerName', ''),
                            'description': row.get('TableDescription', ''),
                            'tag': row.get('TableTag', '')
                        },
                        'columns': {}
                    }
                
                uploaded_metadata[table_key]['columns'][row['ColumnName']] = {
                    'description': row['ColumnDescription'],
                    'tag': row.get('ColumnTag', ''),
                    'data_type': row.get('ColumnDataType', 'string')
                }
            
            logger.info(f"Processed metadata for {len(uploaded_metadata)} tables")
            logger.info(f"Discovered schemas: {discovered_schemas}")
            logger.info(f"Discovered tables: {discovered_tables}")
            
            # Check for missing schemas/tables that need to be loaded
            missing_check = check_missing_schemas_tables(discovered_schemas, discovered_tables)
            
            if missing_check['has_missing']:
                # Return with missing items info - don't auto-load, ask user first
                return jsonify({
                    'success': True, 
                    'message': f'Successfully uploaded metadata for {len(uploaded_metadata)} tables',
                    'metadata': uploaded_metadata,
                    'missing_items': missing_check,
                    'requires_loading': True
                })
            else:
                # All schemas/tables are already loaded, proceed normally
                return jsonify({
                    'success': True, 
                    'message': f'Successfully uploaded metadata for {len(uploaded_metadata)} tables',
                    'metadata': uploaded_metadata,
                    'requires_loading': False
                })
        else:
            return jsonify({'success': False, 'message': 'Please upload a CSV file'})
    
    except Exception as e:
        logger.error(f"Error uploading metadata: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/add_metadata', methods=['POST'])
def add_metadata():
    global current_metadata
    try:
        data = request.json
        table_name = data.get('table_name')
        table_tag = data.get('table_tag', '')
        table_description = data.get('table_description', '')
        column_name = data.get('column_name')
        column_description = data.get('column_description')
        column_tag = data.get('column_tag', '')
        data_type = data.get('data_type', 'string')
        
        if not all([table_name, column_name, column_description]):
            return jsonify({'success': False, 'message': 'Missing required fields'})
        
        table_key = f"{selected_schema}.{table_name}" if selected_schema else table_name
        
        if table_key not in current_metadata:
            current_metadata[table_key] = {
                'table_info': {
                    'schema': selected_schema,
                    'description': table_description,
                    'tag': table_tag
                },
                'columns': {}
            }
        
        current_metadata[table_key]['columns'][column_name] = {
            'description': column_description,
            'tag': column_tag,
            'data_type': data_type
        }
        
        logger.info(f"Added metadata for {table_key}.{column_name}")
        return jsonify({'success': True, 'message': 'Metadata added successfully'})
    
    except Exception as e:
        logger.error(f"Error adding metadata: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get_metadata')
def get_metadata():
    # Combine manual and uploaded metadata
    combined_metadata = {}
    
    # Add manual metadata
    for table_key, table_data in current_metadata.items():
        combined_metadata[table_key] = table_data
    
    # Add uploaded metadata
    for table_key, table_data in uploaded_metadata.items():
        if table_key not in combined_metadata:
            combined_metadata[table_key] = table_data
        else:
            # Merge columns
            combined_metadata[table_key]['columns'].update(table_data['columns'])
    
    return jsonify({'metadata': combined_metadata})

@app.route('/get_metadata_with_source')
def get_metadata_with_source():
    """Get metadata with source information (manual vs CSV)"""
    metadata_with_source = {
        'manual': current_metadata,
        'csv': uploaded_metadata,
        'combined': {}
    }
    
    # Create combined metadata with source tracking
    combined = {}
    
    # Add manual metadata
    for table_key, table_data in current_metadata.items():
        if table_key not in combined:
            combined[table_key] = {
                'table_info': table_data.get('table_info', {}),
                'columns': {},
                'sources': {'table': 'manual', 'columns': {}}
            }
        
        for col_name, col_data in table_data.get('columns', {}).items():
            combined[table_key]['columns'][col_name] = col_data
            combined[table_key]['sources']['columns'][col_name] = 'manual'
    
    # Add uploaded metadata
    for table_key, table_data in uploaded_metadata.items():
        if table_key not in combined:
            combined[table_key] = {
                'table_info': table_data.get('table_info', {}),
                'columns': {},
                'sources': {'table': 'csv', 'columns': {}}
            }
        else:
            # Merge table info (CSV takes precedence if manual doesn't have it)
            if not combined[table_key]['table_info'].get('description') and table_data.get('table_info', {}).get('description'):
                combined[table_key]['table_info'].update(table_data['table_info'])
                combined[table_key]['sources']['table'] = 'csv'
        
        for col_name, col_data in table_data.get('columns', {}).items():
            combined[table_key]['columns'][col_name] = col_data
            combined[table_key]['sources']['columns'][col_name] = 'csv'
    
    metadata_with_source['combined'] = combined
    
    return jsonify(metadata_with_source)

def create_field_schema(column_info, metadata=None):
    """Create SchemaFieldClass from column info and metadata"""
    col_name = column_info['name']
    col_type = column_info['type'].lower()
    
    # Determine field type - fix the logic for number detection
    if col_type == 'number' or any(num_type in col_type for num_type in ['int', 'bigint', 'double', 'decimal', 'float', 'numeric']):
        field_type = NumberTypeClass()
    elif 'boolean' in col_type or 'bool' in col_type:
        field_type = BooleanTypeClass()
    elif 'varchar' in col_type or 'char' in col_type or 'string' in col_type or 'text' in col_type:
        field_type = StringTypeClass()
    else:
        # Default to string for unknown types
        field_type = StringTypeClass()
        logger.warning(f"Unknown column type '{col_type}' for column '{col_name}', defaulting to string")
    
    # Get description from metadata if available
    description = f"Column {col_name}"
    
    if metadata and col_name in metadata:
        col_metadata = metadata[col_name]
        description = col_metadata.get('description', description)
    
    # Create the field schema (tags will be handled separately at dataset level)
    field_schema = SchemaFieldClass(
        fieldPath=col_name,
        type=SchemaFieldDataTypeClass(type=field_type),
        nativeDataType=col_type,
        description=description,
    )
    
    logger.debug(f"Created field schema for {col_name}: type={type(field_type).__name__}, nativeType={col_type}")
    return field_schema

@app.route('/test_datahub_connection', methods=['POST'])
def test_datahub_connection():
    try:
        emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS)
        # Try to create a simple test connection
        logger.info("Testing DataHub connection...")
        
        # Simple test - try to initialize the emitter
        test_result = emitter._gms_server
        if test_result:
            logger.info("DataHub connection test successful")
            return jsonify({
                'success': True, 
                'message': f'Successfully connected to DataHub at {DATAHUB_GMS}'
            })
        else:
            return jsonify({
                'success': False, 
                'message': 'Failed to establish DataHub connection'
            })
    except Exception as e:
        logger.error(f"DataHub connection test failed: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'DataHub connection failed: {str(e)}'
        })

@app.route('/test_trino_connection', methods=['POST'])
def test_trino_connection():
    try:
        if not selected_catalog or not selected_schema:
            return jsonify({
                'success': False, 
                'message': 'Please select catalog and schema first'
            })
        
        test_connector = TrinoConnector()
        if test_connector.connect(selected_catalog, selected_schema):
            # Try a simple query
            test_connector.cursor.execute("SELECT 1")
            result = test_connector.cursor.fetchone()
            if result:
                logger.info("Trino connection test successful")
                return jsonify({
                    'success': True, 
                    'message': f'Successfully connected to Trino at {TRINO_HOST}:{TRINO_PORT}'
                })
        
        return jsonify({
            'success': False, 
            'message': 'Failed to establish Trino connection'
        })
    except Exception as e:
        logger.error(f"Trino connection test failed: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Trino connection failed: {str(e)}'
        })

@app.route('/emit_to_datahub', methods=['POST'])
def emit_to_datahub():
    try:
        data = request.json
        table_names = data.get('tables', [])
        
        if not table_names:
            return jsonify({'success': False, 'message': 'No tables selected'})
        
        if not selected_catalog or not selected_schema:
            return jsonify({
                'success': False, 
                'message': 'Catalog and schema must be selected before emitting'
            })
        
        # Get combined metadata
        combined_metadata = {}
        
        # Add manual metadata
        for table_key, table_data in current_metadata.items():
            combined_metadata[table_key] = table_data
        
        # Add uploaded metadata
        for table_key, table_data in uploaded_metadata.items():
            if table_key not in combined_metadata:
                combined_metadata[table_key] = table_data
            else:
                # Merge columns
                if 'columns' not in combined_metadata[table_key]:
                    combined_metadata[table_key]['columns'] = {}
                combined_metadata[table_key]['columns'].update(table_data['columns'])
        
        logger.info(f"Combined metadata keys: {list(combined_metadata.keys())}")
        logger.info(f"Selected schema: {selected_schema}, Selected catalog: {selected_catalog}")
        logger.info(f"Tables to emit: {table_names}")
        
        try:
            emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS)
        except Exception as e:
            logger.error(f"Failed to create DataHub emitter: {str(e)}")
            return jsonify({
                'success': False, 
                'message': f'Failed to connect to DataHub: {str(e)}'
            })
        
        successful_emissions = []
        failed_emissions = []
        
        for table_name in table_names:
            try:
                # Get table summary with proper parameters
                table_summary = trino_connector.get_table_summary(selected_catalog, selected_schema, table_name)
                if not table_summary:
                    # Check if we have metadata for this table even if it's not in Trino
                    table_key = f"{selected_schema}.{table_name}"
                    if table_key in combined_metadata and combined_metadata[table_key].get('columns'):
                        logger.warning(f"Table {table_name} not found in Trino but has metadata - creating basic schema")
                        # Create a basic table summary from metadata
                        table_summary = {
                            'table_name': table_name,
                            'columns': []
                        }
                        # Create columns from metadata
                        for col_name, col_data in combined_metadata[table_key]['columns'].items():
                            table_summary['columns'].append({
                                'name': col_name,
                                'type': col_data.get('data_type', 'string')
                            })
                    else:
                        failed_emissions.append(f"{table_name}: Table not found in Trino and no metadata available")
                        continue
                
                # Create field schemas
                field_schemas = []
                table_key = f"{selected_schema}.{table_name}"
                table_metadata = combined_metadata.get(table_key, {})
                
                logger.info(f"Processing table: {table_name}, table_key: {table_key}")
                logger.info(f"Found metadata for table: {table_key in combined_metadata}")
                if table_key in combined_metadata:
                    logger.info(f"Metadata columns: {list(table_metadata.get('columns', {}).keys())}")
                
                # Get column metadata if available
                column_metadata = {}
                if 'columns' in table_metadata:
                    column_metadata = table_metadata['columns']
                
                for column_info in table_summary['columns']:
                    field_schema = create_field_schema(column_info, column_metadata)
                    field_schemas.append(field_schema)
                
                # Get table description and metadata
                table_description = f"Table `{table_name}` from Trino catalog {selected_catalog}.{selected_schema}"
                table_info = {}
                
                if table_metadata and 'table_info' in table_metadata:
                    table_info = table_metadata['table_info']
                    if table_info.get('description'):
                        table_description = table_info['description']
                
                # Build dataset snapshot
                dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{selected_catalog}.{selected_schema}.{table_name},{ENV})"
                now = datetime.datetime.now()
                
                # Create aspects list
                aspects = []
                
                # Add dataset properties (keep description clean)
                aspects.append(DatasetPropertiesClass(description=table_description))
                
                # Add schema metadata
                aspects.append(SchemaMetadataClass(
                    schemaName=f"{table_name}_schema",
                    platform=f"urn:li:dataPlatform:{PLATFORM}",
                    version=0,
                    created=AuditStampClass(time=int(now.timestamp() * 1000), actor=OWNER_URN),
                    lastModified=AuditStampClass(time=int(now.timestamp() * 1000), actor=OWNER_URN),
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                    fields=field_schemas,
                ))
                
                # Add ownership if owner is specified
                if table_info.get('owner'):
                    try:
                        owner_urn = f"urn:li:corpuser:{table_info['owner'].lower().replace(' ', '_')}"
                        aspects.append(OwnershipClass(
                            owners=[
                                OwnerClass(
                                    owner=owner_urn,
                                    type=OwnershipTypeClass.DATAOWNER,
                                    source=None
                                )
                            ],
                            lastModified=AuditStampClass(time=int(now.timestamp() * 1000), actor=OWNER_URN)
                        ))
                        logger.info(f"Added owner {table_info['owner']} for table {table_name}")
                    except Exception as e:
                        logger.warning(f"Failed to add owner for {table_name}: {str(e)}")
                
                # Add proper DataHub domain if specified
                if table_info.get('domain'):
                    try:
                        clean_domain = table_info['domain'].lower().replace(' ', '_').replace('-', '_')
                        domain_urn = f"urn:li:domain:{clean_domain}"
                        
                        # Validate domain URN
                        if clean_domain and len(clean_domain) > 0:
                            aspects.append(DomainsClass(domains=[domain_urn]))
                            logger.info(f"Added domain {table_info['domain']} ({domain_urn}) for table {table_name}")
                        else:
                            logger.warning(f"Invalid domain name for {table_name}: {table_info['domain']}")
                    except Exception as e:
                        logger.warning(f"Failed to add domain for {table_name}: {str(e)}")
                
                # Add proper DataHub tags if specified
                tags_to_add = []
                
                # Add table tag
                if table_info.get('tag'):
                    table_tag = table_info['tag'].lower().replace(' ', '_').replace('-', '_')
                    table_tag_urn = f"urn:li:tag:{table_tag}"
                    tags_to_add.append(TagAssociationClass(tag=table_tag_urn))
                
                # Add column tags (collect all unique column tags)
                if 'columns' in table_metadata:
                    column_tags = set()
                    for col_name, col_data in table_metadata['columns'].items():
                        if col_data.get('tag'):
                            column_tags.add(col_data['tag'])
                    
                    for tag in column_tags:
                        clean_tag = tag.lower().replace(' ', '_').replace('-', '_')
                        tag_urn = f"urn:li:tag:{clean_tag}"
                        tags_to_add.append(TagAssociationClass(tag=tag_urn))
                
                if tags_to_add:
                    try:
                        # Validate tag URNs before creating GlobalTagsClass
                        valid_tags = []
                        for tag_assoc in tags_to_add:
                            if tag_assoc.tag and tag_assoc.tag.startswith('urn:li:tag:'):
                                valid_tags.append(tag_assoc)
                            else:
                                logger.warning(f"Invalid tag URN: {tag_assoc.tag}")
                        
                        if valid_tags:
                            aspects.append(GlobalTagsClass(tags=valid_tags))
                            tag_names = [tag.tag.split(':')[-1] for tag in valid_tags]
                            logger.info(f"Added tags {tag_names} for table {table_name}")
                    except Exception as e:
                        logger.warning(f"Failed to add tags for {table_name}: {str(e)}")
                
                snapshot = DatasetSnapshotClass(
                    urn=dataset_urn,
                    aspects=aspects
                )
                
                mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
                
                # Debug: Log the MCE structure
                logger.info(f"Emitting MCE for {table_name} with {len(aspects)} aspects")
                
                # Emit to DataHub
                try:
                    emitter.emit_mce(mce)
                    successful_emissions.append(table_name)
                    logger.info(f"Successfully emitted metadata for {table_name}")
                except Exception as emit_error:
                    error_msg = f"{table_name}: DataHub emission failed - {str(emit_error)}"
                    failed_emissions.append(error_msg)
                    logger.error(f"DataHub emission failed for {table_name}: {str(emit_error)}")
                    logger.error(f"MCE structure: {type(mce)}")
                    continue
                
            except Exception as e:
                error_msg = f"{table_name}: Metadata preparation failed - {str(e)}"
                failed_emissions.append(error_msg)
                logger.error(f"Failed to prepare metadata for {table_name}: {str(e)}")
                logger.error(f"Exception type: {type(e)}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
        
        return jsonify({
            'success': len(successful_emissions) > 0,
            'message': f'Emitted {len(successful_emissions)} tables successfully',
            'successful': successful_emissions,
            'failed': failed_emissions
        })
    
    except Exception as e:
        logger.error(f"Error emitting to DataHub: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

if __name__ == '__main__':
    app.run(debug=FLASK_DEBUG, host=FLASK_HOST, port=FLASK_PORT)