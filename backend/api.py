# backend/api.py
from flask import Flask, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor
from flask_cors import CORS
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database connection
def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="urban_rhythm",
        user="postgres",
        password="pratham"
    )
    conn.cursor_factory = RealDictCursor
    return conn

@app.route('/api/test')
def test_api():
    """Simple test endpoint to verify API is running"""
    return jsonify({"status": "success", "message": "API is running"})

@app.route('/api/test-db')
def test_db():
    """Test database connection"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT 1 as test')
        result = cur.fetchone()
        cur.close()
        conn.close()
        return jsonify({"status": "success", "message": "Database connection successful", "data": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/tables')
def list_tables():
    """List all tables in the database"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Query to get all tables in the public schema
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        
        tables = [row['table_name'] for row in cur.fetchall()]
        cur.close()
        conn.close()
        
        return jsonify({"status": "success", "tables": tables})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/columns/<table_name>')
def get_table_columns(table_name):
    """Get all columns for a specific table"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Query to get all columns in the specified table
        cur.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = [dict(row) for row in cur.fetchall()]
        cur.close()
        conn.close()
        
        return jsonify({"status": "success", "table": table_name, "columns": columns})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/debug/<table_name>')
def debug_table(table_name):
    """Get table structure and a few sample rows with better error handling"""
    valid_tables = [
        'neighborhoods', 'nyc_311_calls', 'mta_turnstile', 
        'tlc_trips', 'weather', 'events'
    ]
    
    if table_name not in valid_tables:
        return jsonify({"error": "Invalid table name"}), 400
    
    result = {
        "table": table_name,
        "structure": [],
        "sample_rows": []
    }
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get table structure
        try:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            columns = cur.fetchall()
            result["structure"] = [dict(col) for col in columns]
        except Exception as e:
            result["structure_error"] = str(e)
        
        # Get sample rows with minimal fields
        try:
            # Get only the first 3 columns to avoid geometry fields
            if result["structure"]:
                column_names = [col["column_name"] for col in result["structure"][:3]]
                columns_str = ", ".join(column_names)
                cur.execute(f"SELECT {columns_str} FROM {table_name} LIMIT 5")
                rows = cur.fetchall()
                result["sample_rows"] = [dict(row) for row in rows]
            else:
                result["sample_error"] = "No columns found in structure"
        except Exception as e:
            result["sample_error"] = str(e)
        
        cur.close()
        conn.close()
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/sample/<table_name>')
def get_sample_data(table_name):
    """Get sample data from specified table with improved error handling"""
    # Validate table name to prevent SQL injection
    valid_tables = [
        'neighborhoods', 'nyc_311_calls', 'mta_turnstile', 
        'tlc_trips', 'weather', 'events'
    ]
    
    if table_name not in valid_tables:
        return jsonify({"error": "Invalid table name"}), 400
    
    limit = request.args.get('limit', 10, type=int)
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # First, get column names to avoid issues with missing columns
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = [row['column_name'] for row in cur.fetchall()]
        
        # Check if geometry column exists
        has_geometry = 'geometry' in columns
        
        # Handle tables differently based on available columns
        if table_name == 'neighborhoods':
            select_cols = ['id']
            if 'name' in columns: select_cols.append('name')
            if 'borough' in columns: select_cols.append('borough')
            if has_geometry: select_cols.append('ST_AsText(geometry) as geom_text')
            
            query = f"SELECT {', '.join(select_cols)} FROM {table_name} LIMIT %s"
            
        elif table_name == 'tlc_trips':
            select_cols = ['id']
            if 'pickup_datetime' in columns: select_cols.append('pickup_datetime')
            if 'dropoff_datetime' in columns: select_cols.append('dropoff_datetime')
            if 'passenger_count' in columns: select_cols.append('passenger_count')
            if 'trip_distance' in columns: select_cols.append('trip_distance')
            if 'pickup_location' in columns: select_cols.append('ST_AsText(pickup_location) as pickup_geom')
            if 'dropoff_location' in columns: select_cols.append('ST_AsText(dropoff_location) as dropoff_geom')
            
            query = f"SELECT {', '.join(select_cols)} FROM {table_name} LIMIT %s"
            
        elif table_name == 'nyc_311_calls':
            select_cols = ['id']
            if 'created_date' in columns: select_cols.append('created_date')
            if 'complaint_type' in columns: select_cols.append('complaint_type')
            if 'descriptor' in columns: select_cols.append('descriptor')
            if 'incident_zip' in columns: select_cols.append('incident_zip')
            if has_geometry: select_cols.append('ST_AsText(geometry) as geom_text')
            
            query = f"SELECT {', '.join(select_cols)} FROM {table_name} LIMIT %s"
            
        elif table_name == 'mta_turnstile':
            select_cols = []
            # Add columns that exist in the table
            for col in ['id', 'station_name', 'datetime', 'entries', 'exits']:
                if col in columns: select_cols.append(col)
            if has_geometry: select_cols.append('ST_AsText(geometry) as geom_text')
            
            if not select_cols:
                return jsonify({"error": "No valid columns found in mta_turnstile table"}), 500
                
            query = f"SELECT {', '.join(select_cols)} FROM {table_name} LIMIT %s"
            
        elif table_name == 'events':
            select_cols = ['id']
            # Add columns that exist in the table
            for col in ['event_name', 'start_date_time', 'end_date_time', 'event_type', 'event_borough']:
                if col in columns: select_cols.append(col)
            if has_geometry: select_cols.append('ST_AsText(geometry) as geom_text')
            
            query = f"SELECT {', '.join(select_cols)} FROM {table_name} LIMIT %s"
            
        elif table_name == 'weather':
            select_cols = ['id']
            for col in ['datetime', 'temperature', 'precipitation', 'wind_speed', 'weather_condition']:
                if col in columns: select_cols.append(col)
                
            query = f"SELECT {', '.join(select_cols)} FROM {table_name} LIMIT %s"
            
        else:
            # Generic fallback query
            query = f"SELECT * FROM {table_name} LIMIT %s"
        
        # Execute the query
        print(f"Executing query: {query} with limit {limit}")
        cur.execute(query, (limit,))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        # Convert rows to a list of dictionaries
        data = [dict(row) for row in rows]
        return jsonify(data)
        
    except Exception as e:
        # Provide detailed error information
        return jsonify({
            "error": str(e),
            "table": table_name,
            "message": "Error fetching data from table"
        }), 500

# Route to get aggregated data for visualizations
@app.route('/api/visualization/311_by_borough')
def get_311_by_borough():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # First check if the required columns exist
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'nyc_311_calls'
        """)
        
        columns = [row['column_name'] for row in cur.fetchall()]
        
        # Check if all required columns exist
        required_columns = ['borough', 'complaint_type', 'geometry']
        missing_columns = [col for col in required_columns if col not in columns]
        
        if missing_columns:
            return jsonify({
                "error": f"Missing required columns: {', '.join(missing_columns)}",
                "available_columns": columns
            }), 500
            
        cur.execute("""
            SELECT 
                borough, 
                COUNT(*) as count,
                complaint_type,
                AVG(ST_Y(geometry)) as avg_lat,
                AVG(ST_X(geometry)) as avg_lng
            FROM nyc_311_calls
            GROUP BY borough, complaint_type
            ORDER BY count DESC
        """)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        data = [dict(row) for row in rows]
        return jsonify(data)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/visualization/mta_by_borough')
def get_mta_by_borough():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # First check if the required columns exist
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'mta_turnstile'
        """)
        
        columns = [row['column_name'] for row in cur.fetchall()]
        
        # Check if all required columns exist
        required_columns = ['borough', 'station_name', 'entries', 'exits', 'geometry']
        missing_columns = [col for col in required_columns if col not in columns]
        
        if missing_columns:
            return jsonify({
                "error": f"Missing required columns: {', '.join(missing_columns)}",
                "available_columns": columns
            }), 500
            
        cur.execute("""
            SELECT 
                borough, 
                SUM(entries) as total_entries,
                SUM(exits) as total_exits,
                COUNT(DISTINCT station_name) as station_count,
                AVG(ST_Y(geometry)) as avg_lat,
                AVG(ST_X(geometry)) as avg_lng
            FROM mta_turnstile
            GROUP BY borough
            ORDER BY total_entries DESC
        """)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        data = [dict(row) for row in rows]
        return jsonify(data)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)