# backend/api_routes.py
from flask import Blueprint, jsonify, request
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logger = logging.getLogger('data_api')

# Create blueprint
api_bp = Blueprint('api', __name__, url_prefix='/api')

# Database connection function - duplicate this here to avoid circular imports
def get_db_connection(cursor_factory=None):
    """Establish connection to PostgreSQL database"""
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        database=os.environ.get('DB_NAME', 'nyc_data'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', '')
    )
    if cursor_factory:
        conn.cursor_factory = cursor_factory
    return conn

def get_dict_cursor_connection():
    """Get a connection with a RealDictCursor for returning dictionary results"""
    return get_db_connection(cursor_factory=RealDictCursor)

# Define task functions here since we're not importing from pipeline.py
def fetch_311_task(start_date, end_date):
    """Task to fetch 311 data"""
    source = '311'
    try:
        logger.info(f"Fetching {source} data from {start_date} to {end_date}")
        update_task_status(source, 'running')
        
        # In a real implementation, you would call your _pycache_.fetch_311 function
        # For now, we'll just update the status
        
        update_task_status(source, 'completed', "Task would fetch data from 311 API")
        logger.info(f"Completed {source} task")
    
    except Exception as e:
        logger.error(f"Error in {source} task: {str(e)}")
        update_task_status(source, 'failed', str(e))

def fetch_mta_task(start_date, end_date):
    """Task to fetch MTA data"""
    source = 'mta'
    try:
        logger.info(f"Fetching {source} data from {start_date} to {end_date}")
        update_task_status(source, 'running')
        
        # In a real implementation, you would call your _pycache_.fetch_mta function
        
        update_task_status(source, 'completed', "Task would fetch data from MTA API")
        logger.info(f"Completed {source} task")
    
    except Exception as e:
        logger.error(f"Error in {source} task: {str(e)}")
        update_task_status(source, 'failed', str(e))

def fetch_tlc_task(start_date, end_date):
    """Task to fetch TLC data"""
    source = 'tlc'
    try:
        logger.info(f"Fetching {source} data from {start_date} to {end_date}")
        update_task_status(source, 'running')
        
        # In a real implementation, you would call your _pycache_.fetch_tlc function
        
        update_task_status(source, 'completed', "Task would fetch data from TLC API")
        logger.info(f"Completed {source} task")
    
    except Exception as e:
        logger.error(f"Error in {source} task: {str(e)}")
        update_task_status(source, 'failed', str(e))

def fetch_weather_task(start_date, end_date):
    """Task to fetch Weather data"""
    source = 'weather'
    try:
        logger.info(f"Fetching {source} data from {start_date} to {end_date}")
        update_task_status(source, 'running')
        
        # In a real implementation, you would call your _pycache_.fetch_weather function
        
        update_task_status(source, 'completed', "Task would fetch data from Weather API")
        logger.info(f"Completed {source} task")
    
    except Exception as e:
        logger.error(f"Error in {source} task: {str(e)}")
        update_task_status(source, 'failed', str(e))

def fetch_events_task(start_date, end_date):
    """Task to fetch Events data"""
    source = 'events'
    try:
        logger.info(f"Fetching {source} data from {start_date} to {end_date}")
        update_task_status(source, 'running')
        
        # In a real implementation, you would call your _pycache_.fetch_events function
        
        update_task_status(source, 'completed', "Task would fetch data from Events API")
        logger.info(f"Completed {source} task")
    
    except Exception as e:
        logger.error(f"Error in {source} task: {str(e)}")
        update_task_status(source, 'failed', str(e))

def update_task_status(source, status, error=None):
    """Update status of data fetching tasks"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if task_status table exists, create if not
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS task_status (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            status VARCHAR(50) NOT NULL,
            error TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating task_status table: {str(e)}")
    
    # Store task status
    try:
        query = """
        INSERT INTO task_status (source, status, error, updated_at)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (source, status, error, datetime.now()))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating task status: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@api_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@api_bp.route('/fetch/<source>', methods=['POST'])
def fetch_source(source):
    """Endpoint to trigger fetching a specific data source"""
    valid_sources = ['311', 'mta', 'tlc', 'weather', 'events', 'all']
    if source not in valid_sources:
        return jsonify({'error': f'Invalid source. Valid sources: {", ".join(valid_sources)}'}), 400
    
    try:
        request_data = request.get_json() or {}
        start_date = request_data.get('start_date')
        end_date = request_data.get('end_date')
        
        # Validate dates
        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d') if start_date else None
            end_date = datetime.strptime(end_date, '%Y-%m-%d') if end_date else None
        except (ValueError, TypeError):
            # Default to last 30 days if dates are invalid
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
        
        # Initialize response
        response = {
            'status': 'processing',
            'source': source,
            'start_date': start_date.isoformat() if start_date else None,
            'end_date': end_date.isoformat() if end_date else None
        }
        
        # Trigger appropriate task based on source
        if source == 'all':
            fetch_311_task(start_date, end_date)
            fetch_mta_task(start_date, end_date)
            fetch_tlc_task(start_date, end_date)
            fetch_weather_task(start_date, end_date)
            fetch_events_task(start_date, end_date)
        elif source == '311':
            fetch_311_task(start_date, end_date)
        elif source == 'mta':
            fetch_mta_task(start_date, end_date)
        elif source == 'tlc':
            fetch_tlc_task(start_date, end_date)
        elif source == 'weather':
            fetch_weather_task(start_date, end_date)
        elif source == 'events':
            fetch_events_task(start_date, end_date)
        
        return jsonify(response)
    
    except Exception as e:
        logger.error(f"Error in fetch_source: {str(e)}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/status/<source>', methods=['GET'])
def get_source_status(source):
    """Get status of a specific data source's fetching task"""
    valid_sources = ['311', 'mta', 'tlc', 'weather', 'events', 'all']
    if source not in valid_sources:
        return jsonify({'error': f'Invalid source. Valid sources: {", ".join(valid_sources)}'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if source == 'all':
            # Get latest status for all sources
            cursor.execute("""
            SELECT DISTINCT ON (source) source, status, error, updated_at
            FROM task_status
            ORDER BY source, updated_at DESC
            """)
        else:
            # Get latest status for specific source
            cursor.execute("""
            SELECT source, status, error, updated_at
            FROM task_status
            WHERE source = %s
            ORDER BY updated_at DESC
            LIMIT 1
            """, (source,))
        
        results = cursor.fetchall()
        
        status = []
        for row in results:
            status.append({
                'source': row[0],
                'status': row[1],
                'error': row[2],
                'updated_at': row[3].isoformat() if row[3] else None
            })
        
        if source != 'all' and not status:
            return jsonify({'error': f'No status found for source: {source}'}), 404
        
        return jsonify(status if source == 'all' else status[0])
    
    except Exception as e:
        logger.error(f"Error in get_source_status: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
    finally:
        cursor.close()
        conn.close()

@api_bp.route('/urban-rhythm', methods=['GET'])
def get_urban_rhythm():
    # Get query parameters
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    temporal_resolution = request.args.get('resolution', 'hourly')
    data_types = request.args.get('data_types', '311,mta,tlc,weather,events').split(',')
    neighborhood = request.args.get('neighborhood', None)
    
    # Connect to database
    conn = get_db_connection(cursor_factory=RealDictCursor)
    cursor = conn.cursor()
    
    result = {}
    
    try:
        # Process each data type requested
        for data_type in data_types:
            if data_type == '311':
                # 311 data with temporal aggregation
                query = """
                SELECT 
                    date_trunc(%s, created_date) as time_bucket,
                    COUNT(*) as count,
                    complaint_type,
                    neighborhood_id
                FROM nyc_311_calls
                WHERE created_date BETWEEN %s AND %s
                """
                
                if neighborhood:
                    query += " AND neighborhood_id = %s"
                    query += " GROUP BY time_bucket, complaint_type, neighborhood_id"
                    cursor.execute(query, (temporal_resolution, start_date, end_date, neighborhood))
                else:
                    query += " GROUP BY time_bucket, complaint_type, neighborhood_id"
                    cursor.execute(query, (temporal_resolution, start_date, end_date))
                
                result['311'] = cursor.fetchall()
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error in get_urban_rhythm: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
    finally:
        cursor.close()
        conn.close()

@api_bp.route('/correlations', methods=['GET'])
def get_correlations():
    """Get correlations between different data types"""
    # Parameters for correlation analysis
    data_type1 = request.args.get('data_type1', '311')
    data_type2 = request.args.get('data_type2', 'weather')
    temporal_window = request.args.get('window', 'day')
    
    # Placeholder implementation
    return jsonify({"correlation_data": "sample_data"})

@api_bp.route('/clusters', methods=['GET'])
def get_spatiotemporal_clusters():
    """Get spatiotemporal clusters based on the data"""
    # Parameters for clustering
    data_types = request.args.get('data_types', '311,mta,tlc')
    temporal_window = request.args.get('window', 'day')
    epsilon = request.args.get('epsilon', 0.01)
    min_samples = request.args.get('min_samples', 5)
    
    # Placeholder implementation
    return jsonify({"cluster_data": "sample_data"})

@api_bp.route('/sample/<table_name>', methods=['GET'])
def get_sample_data(table_name):
    """Get sample data from a specific table"""
    # Validate table name to prevent SQL injection
    valid_tables = [
        'neighborhoods', 'nyc_311_calls', 'mta_turnstile', 
        'tlc_trips', 'weather', 'events'
    ]
    
    if table_name not in valid_tables:
        return jsonify({"error": "Invalid table name"}), 400
    
    limit = request.args.get('limit', 5, type=int)
    
    try:
        conn = get_dict_cursor_connection()
        cur = conn.cursor()
        
        # Special handling for tables with geometry fields
        if table_name == 'neighborhoods':
            cur.execute(
                f"SELECT id, name, borough, ST_AsText(geometry) as geom_text "
                f"FROM {table_name} LIMIT %s", (limit,)
            )
        elif table_name == 'tlc_trips':
            cur.execute(
                f"SELECT id, pickup_datetime, dropoff_datetime, passenger_count, "
                f"trip_distance, ST_AsText(pickup_location) as pickup_geom, "
                f"ST_AsText(dropoff_location) as dropoff_geom "
                f"FROM {table_name} LIMIT %s", (limit,)
            )
        elif table_name in ['nyc_311_calls', 'mta_turnstile', 'events']:
            # Try to select common fields for these tables without using EXCEPT
            try:
                cur.execute(
                    f"SELECT id, ST_AsText(geometry) as geom_text "
                    f"FROM {table_name} LIMIT %s", (limit,)
                )
            except:
                # Fallback to simple SELECT if the query fails
                cur.execute(f"SELECT * FROM {table_name} LIMIT %s", (limit,))
        else:
            cur.execute(f"SELECT * FROM {table_name} LIMIT %s", (limit,))
            
        rows = cur.fetchall()
        return jsonify(rows)
        
    except Exception as e:
        logger.error(f"Error in get_sample_data: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
    finally:
        cur.close()
        conn.close()

def get_date_column_for_source(source):
    """Get the date column name for a specific source"""
    date_columns = {
        '311': 'created_date',
        'mta': 'datetime',
        'tlc': 'pickup_datetime',
        'weather': 'datetime',
        'events': 'start_datetime'
    }
    return date_columns.get(source)