#!/usr/bin/env python3
"""
ETL script for fetching NYC events data from Permitted Events dataset
"""

import os
import argparse
import logging
from datetime import datetime, timedelta
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from . import get_default_date_range, save_to_csv

# Load environment variables
load_dotenv()

# Set up logger
logger = logging.getLogger(__name__)

def check_api_schema(api_endpoint):
    """
    Check the schema of the API endpoint by fetching a sample
    
    Args:
        api_endpoint: URL of the API endpoint
        
    Returns:
        List of column names if available, None otherwise
    """
    try:
        logger.info(f"Checking schema for {api_endpoint}")
        response = requests.get(api_endpoint, params={"$limit": 1})
        response.raise_for_status()
        data = response.json()
        if data:
            columns = list(data[0].keys())
            logger.info(f"Available fields: {columns}")
            return columns
        else:
            logger.info(f"No data available for {api_endpoint}")
            return None
    except Exception as e:
        logger.error(f"Error checking schema for {api_endpoint}: {e}")
        return None

def get_endpoint_schema(endpoint_url):
    """
    Get the schema for an endpoint using columns.json
    
    Args:
        endpoint_url: URL of the API endpoint
        
    Returns:
        Schema dictionary if available, None otherwise
    """
    try:
        schema_url = endpoint_url.replace('.json', '/columns.json')
        logger.info(f"Fetching schema from {schema_url}")
        response = requests.get(schema_url)
        if response.status_code == 200:
            schema = response.json()
            logger.info(f"Schema contains {len(schema)} columns")
            return schema
        else:
            logger.error(f"Failed to get schema: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        return None

def build_date_query(columns):
    """
    Build a date query based on available fields
    
    Args:
        columns: List of available column names
        
    Returns:
        Query string with appropriate date field(s)
    """
    date_fields = []
    
    # Check for known date field names
    if 'start_date_time' in columns:
        date_fields.append('start_date_time')
    if 'event_date' in columns:
        date_fields.append('event_date')
    if 'startdate' in columns:
        date_fields.append('startdate')
    if 'start_date' in columns:
        date_fields.append('start_date')
    
    if not date_fields:
        logger.warning("No date fields found in schema")
        # Look for any field that might contain 'date'
        date_fields = [col for col in columns if 'date' in col.lower()]
        
    if not date_fields:
        logger.error("Could not find any date fields")
        return None
    
    logger.info(f"Using date fields: {date_fields}")
    return date_fields

def fetch_events_data(start_date, end_date):
    """
    Fetch NYC events data using NYC Open Data API
    
    Args:
        start_date: Start date for data fetching
        end_date: End date for data fetching
        
    Returns:
        Pandas DataFrame with events data
    """
    logger.info(f"Fetching events data from {start_date} to {end_date}")
    
    # API endpoints to try
    api_endpoints = [
        "https://data.cityofnewyork.us/resource/bkfu-528j.json",  # Historical
        "https://data.cityofnewyork.us/resource/tvpp-9vvx.json"   # Current
    ]
    
    # Format dates for query
    start_str = start_date.strftime('%Y-%m-%dT00:00:00')
    end_str = end_date.strftime('%Y-%m-%dT23:59:59')
    
    # App token for NYC Open Data
    app_token = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
    
    # Set up headers with app token
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token
    
    all_data = []
    
    # Try each endpoint until we get data
    for api_endpoint in api_endpoints:
        logger.info(f"Trying endpoint: {api_endpoint}")
        
        # Check schema first
        columns = check_api_schema(api_endpoint)
        if not columns:
            logger.warning(f"Could not get schema for {api_endpoint}, skipping")
            continue
        
        # Build query based on available date fields
        date_fields = build_date_query(columns)
        if not date_fields:
            logger.warning(f"Could not build date query for {api_endpoint}, skipping")
            continue
        
        # Construct the WHERE clause dynamically
        where_clauses = []
        for field in date_fields:
            where_clauses.append(f"{field} between '{start_str}' and '{end_str}'")
        
        where_str = " OR ".join(where_clauses)
        
        # Parameters for API query
        params = {
            "$where": where_str,
            "$limit": 50000
        }
        
        offset = 0
        page_size = 10000  # Smaller page size for better reliability
        
        # Fetch data with pagination
        while True:
            current_params = params.copy()
            current_params["$offset"] = offset
            current_params["$limit"] = page_size
            
            try:
                logger.info(f"Fetching page with offset {offset}")
                response = requests.get(api_endpoint, headers=headers, params=current_params)
                response.raise_for_status()
                
                data = response.json()
                
                # If no more results, break the loop
                if not data:
                    logger.info("No more data to fetch")
                    break
                    
                all_data.extend(data)
                logger.info(f"Fetched {len(data)} records (total: {len(all_data)})")
                
                # If fewer results than page_size, we've reached the end
                if len(data) < page_size:
                    break
                    
                # Increment offset for next page
                offset += page_size
                    
            except Exception as e:
                logger.error(f"Error fetching events data: {e}")
                logger.error(f"Response code: {response.status_code if 'response' in locals() else 'N/A'}")
                logger.error(f"Response text: {response.text if 'response' in locals() else 'N/A'}")
                break
        
        # If we got data from this endpoint, no need to try others
        if all_data:
            logger.info(f"Successfully fetched data from {api_endpoint}")
            break
    
    # Convert to DataFrame
    if all_data:
        df = pd.DataFrame(all_data)
        
        # Convert date strings to datetime objects
        # Try different column names as they might vary between datasets
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Standardize column names
        # Detect and standardize start date/time
        for col in ['start_date_time', 'startdate', 'start_date', 'event_date']:
            if col in df.columns:
                df['start_datetime'] = df[col]
                break
                
        # Detect and standardize end date/time
        for col in ['end_date_time', 'enddate', 'end_date']:
            if col in df.columns:
                df['end_datetime'] = df[col]
                break
        
        logger.info(f"Total of {len(df)} events records fetched")
        return df
    else:
        logger.warning("No events data fetched from any endpoint")
        return pd.DataFrame()

def process_events_data(df):
    """
    Process events data: clean, add coordinates, etc.
    
    Args:
        df: Raw events data DataFrame
        
    Returns:
        GeoDataFrame with processed events data
    """
    logger.info("Processing events data")
    
    # Check if DataFrame is empty
    if df.empty:
        logger.warning("Empty dataframe, no data to process")
        return gpd.GeoDataFrame()
    
    # Generate a unique event_id if it doesn't exist
    if 'event_id' not in df.columns:
        if 'eventid' in df.columns:
            df['event_id'] = df['eventid']
        else:
            df['event_id'] = [f"evt_{i}" for i in range(len(df))]
    
    # Check for event_name column
    if 'event_name' not in df.columns:
        if 'name' in df.columns:
            df['event_name'] = df['name']
        else:
            # Try to find any column that might contain event names
            name_candidates = [col for col in df.columns if 'name' in col.lower() or 'title' in col.lower()]
            if name_candidates:
                df['event_name'] = df[name_candidates[0]]
            else:
                df['event_name'] = "Unnamed Event"
    
    # Various approaches to get coordinates
    coordinate_methods = [
        # Method 1: Direct longitude/latitude columns
        lambda df: extract_coordinates_from_direct(df),
        # Method 2: location_point field
        lambda df: extract_coordinates_from_location_point(df),
        # Method 3: the_geom field
        lambda df: extract_coordinates_from_geom(df)
    ]
    
    # Try each method until one works
    for method in coordinate_methods:
        gdf = method(df)
        if not gdf.empty:
            break
    
    if gdf.empty:
        logger.error("Could not extract coordinates using any method")
        return gpd.GeoDataFrame()
    
    # Add time components for multi-resolution analysis
    if 'start_datetime' in gdf.columns:
        gdf['hour'] = gdf['start_datetime'].dt.hour
        gdf['day'] = gdf['start_datetime'].dt.day
        gdf['weekday'] = gdf['start_datetime'].dt.weekday
        gdf['month'] = gdf['start_datetime'].dt.month
        gdf['year'] = gdf['start_datetime'].dt.year
    
    logger.info(f"Processed {len(gdf)} events records")
    return gdf

def extract_coordinates_from_direct(df):
    """Extract coordinates from direct longitude/latitude columns"""
    # Look for various possible column names
    lat_candidates = [col for col in df.columns if 'lat' in col.lower()]
    lon_candidates = [col for col in df.columns if 'lon' in col.lower() or 'lng' in col.lower()]
    
    if lat_candidates and lon_candidates:
        lat_col = lat_candidates[0]
        lon_col = lon_candidates[0]
        
        logger.info(f"Using coordinate columns: {lat_col}, {lon_col}")
        
        # Drop rows with missing coordinates
        df_coords = df.dropna(subset=[lat_col, lon_col])
        
        # Convert to numeric
        df_coords[lat_col] = pd.to_numeric(df_coords[lat_col], errors='coerce')
        df_coords[lon_col] = pd.to_numeric(df_coords[lon_col], errors='coerce')
        
        # Filter out invalid coordinates (basic check for NYC area)
        df_coords = df_coords[
            (df_coords[lat_col] > 40.4) & (df_coords[lat_col] < 41.0) & 
            (df_coords[lon_col] > -74.3) & (df_coords[lon_col] < -73.7)
        ]
        
        if df_coords.empty:
            logger.warning("No valid coordinates after filtering")
            return gpd.GeoDataFrame()
        
        # Create geometry column
        geometry = [Point(xy) for xy in zip(df_coords[lon_col], df_coords[lat_col])]
        
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df_coords, geometry=geometry, crs="EPSG:4326")
        return gdf
    
    return gpd.GeoDataFrame()

def extract_coordinates_from_location_point(df):
    """Extract coordinates from location_point field"""
    # Check for location_point or similar columns
    point_candidates = [col for col in df.columns if 'location' in col.lower() and 'point' in col.lower()]
    
    if not point_candidates:
        return gpd.GeoDataFrame()
    
    point_col = point_candidates[0]
    logger.info(f"Using location point column: {point_col}")
    
    # Parse location_point field which might be in format "POINT (lon lat)"
    df_loc = df.copy()
    
    # Try different regex patterns
    try:
        df_loc['coords'] = df_loc[point_col].str.extract(r'POINT \(([-\d.]+) ([-\d.]+)\)')
        if df_loc['coords'].notna().any():
            df_loc['longitude'] = pd.to_numeric(df_loc['coords'][0], errors='coerce')
            df_loc['latitude'] = pd.to_numeric(df_loc['coords'][1], errors='coerce')
        else:
            # Try another pattern
            df_loc['coords'] = df_loc[point_col].str.extract(r'\(([-\d.]+), ([-\d.]+)\)')
            if df_loc['coords'].notna().any():
                df_loc['longitude'] = pd.to_numeric(df_loc['coords'][0], errors='coerce')
                df_loc['latitude'] = pd.to_numeric(df_loc['coords'][1], errors='coerce')
    except Exception as e:
        logger.error(f"Error parsing location point: {e}")
        return gpd.GeoDataFrame()
    
    df_loc = df_loc.dropna(subset=['longitude', 'latitude'])
    
    # Filter out invalid coordinates
    df_loc = df_loc[
        (df_loc['latitude'] > 40.4) & (df_loc['latitude'] < 41.0) & 
        (df_loc['longitude'] > -74.3) & (df_loc['longitude'] < -73.7)
    ]
    
    if df_loc.empty:
        logger.warning("No valid coordinates after filtering")
        return gpd.GeoDataFrame()
        
    # Create geometry column
    geometry = [Point(xy) for xy in zip(df_loc['longitude'], df_loc['latitude'])]
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(df_loc, geometry=geometry, crs="EPSG:4326")
    return gdf

def extract_coordinates_from_geom(df):
    """Extract coordinates from the_geom field"""
    if 'the_geom' not in df.columns:
        return gpd.GeoDataFrame()
    
    logger.info("Using the_geom column")
    
    try:
        # Convert WKT to geometry objects
        gdf = gpd.GeoDataFrame.from_features(df['the_geom'].tolist(), crs="EPSG:4326")
        # Copy over all other columns
        for col in df.columns:
            if col != 'the_geom':
                gdf[col] = df[col].values
        return gdf
    except Exception as e:
        logger.error(f"Error converting the_geom to geometry: {e}")
        return gpd.GeoDataFrame()

def load_to_database(gdf, db_conn_string):
    """
    Load events data to PostgreSQL database
    
    Args:
        gdf: GeoDataFrame with events data
        db_conn_string: Database connection string
    """
    logger.info("Loading events data to database")
    
    # If dataframe is empty, don't try to load
    if gdf.empty:
        logger.warning("No data to load to database")
        return
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(db_conn_string)
        cursor = conn.cursor()
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return
    
    try:
        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            event_id TEXT,
            name TEXT,
            category TEXT,
            start_datetime TIMESTAMP,
            end_datetime TIMESTAMP,
            location GEOMETRY(Point, 4326),
            neighborhood_id INTEGER,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
        """)
        
        # Create temporary table for data import
        cursor.execute("""
        CREATE TEMP TABLE temp_events (
            event_id TEXT,
            name TEXT,
            category TEXT,
            start_datetime TIMESTAMP,
            end_datetime TIMESTAMP,
            geom GEOMETRY(Point, 4326)
        ) ON COMMIT DROP
        """)
        
        # Find category column if it exists
        category_col = None
        category_candidates = ['category', 'event_type', 'eventtype', 'type']
        for col in category_candidates:
            if col in gdf.columns:
                category_col = col
                break
        
        # Prepare data for insert
        data = []
        for idx, row in gdf.iterrows():
            data.append((
                row.get('event_id', str(idx)),
                row.get('event_name', '').strip() if pd.notnull(row.get('event_name', '')) else '',
                row.get(category_col, '').strip() if category_col and pd.notnull(row.get(category_col, '')) else '',
                row.get('start_datetime'),
                row.get('end_datetime'),
                f"SRID=4326;{row.geometry.wkt}"
            ))
        
        # Insert data into temp table
        insert_query = """
        INSERT INTO temp_events (
            event_id, name, category, start_datetime, end_datetime, geom
        ) VALUES %s
        """
        execute_values(cursor, insert_query, data)
        
        # For each event_id, delete existing records before inserting
        # This avoids the need for a unique constraint
        for event_id in set(d[0] for d in data):
            cursor.execute("DELETE FROM events WHERE event_id = %s", (event_id,))
        
        # Try to create the neighborhoods join if it exists
        try:
            # Insert from temp table to main table with neighborhood join
            cursor.execute("""
            INSERT INTO events (
                event_id, name, category, start_datetime, end_datetime, location, neighborhood_id
            )
            SELECT 
                t.event_id, t.name, t.category, t.start_datetime, t.end_datetime, t.geom, n.id
            FROM temp_events t
            LEFT JOIN neighborhoods n ON ST_Within(t.geom, n.geometry)
            """)
        except Exception as e:
            logger.warning(f"Could not join with neighborhoods: {e}")
            # Fall back to simple insert without neighborhood join
            cursor.execute("""
            INSERT INTO events (
                event_id, name, category, start_datetime, end_datetime, location
            )
            SELECT 
                t.event_id, t.name, t.category, t.start_datetime, t.end_datetime, t.geom
            FROM temp_events t
            """)
        
        # Commit transaction
        conn.commit()
        logger.info(f"Loaded {len(data)} events records to database")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading events data to database: {e}")
        raise
        
    finally:
        cursor.close()
        conn.close()

def main():
    """Main ETL function"""
    parser = argparse.ArgumentParser(description='Fetch NYC events data')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--save-csv', action='store_true', help='Save data to CSV')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get date range
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    else:
        start_date, end_date = get_default_date_range()
    
    # Fetch and process data
    df = fetch_events_data(start_date, end_date)
    gdf = process_events_data(df)
    
    # Save to CSV if requested
    if args.save_csv and not df.empty:
        date_str = start_date.strftime('%Y%m%d') + '_' + end_date.strftime('%Y%m%d')
        save_to_csv(df, 'events', date_str)
    
    # Load to database if not empty
    if not gdf.empty:
        db_conn_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        load_to_database(gdf, db_conn_string)
    
    logger.info("Events ETL process completed successfully")

if __name__ == "__main__":
    main()