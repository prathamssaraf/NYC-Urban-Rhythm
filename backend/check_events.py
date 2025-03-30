import psycopg2
from psycopg2.extras import RealDictCursor

def check_events_table():
    # Connect to database
    conn = psycopg2.connect(
        host="localhost",
        database="urban_rhythm",
        user="postgres",
        password="pratham"
    )
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    
    # Check if events table exists
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'events'
        )
    """)
    table_exists = cur.fetchone()['exists']
    print(f"Events table exists: {table_exists}")
    
    if table_exists:
        # Check table structure
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'events'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        print("\nTable structure:")
        for col in columns:
            print(f"  {col['column_name']} ({col['data_type']})")
        
        # Check row count
        cur.execute("SELECT COUNT(*) FROM events")
        count = cur.fetchone()['count']
        print(f"\nTotal rows: {count}")
        
        # Try to fetch a single row with minimal columns
        if count > 0:
            # Get first few column names
            column_names = [col['column_name'] for col in columns[:3]]
            columns_str = ", ".join(column_names)
            
            cur.execute(f"SELECT {columns_str} FROM events LIMIT 1")
            sample = cur.fetchone()
            print(f"\nSample row: {sample}")
        else:
            print("\nNo rows in events table!")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_events_table()