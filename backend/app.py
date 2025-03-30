# backend/app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
import logging
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import sys

# Add the current directory to the path so Python can find your modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('nyc_data_app')

# Database connection function
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

def create_app():
    """Create and configure the Flask application"""
    app = Flask(__name__)
    
    # Enable CORS
    CORS(app)
    
    # Import and register blueprints
    # Make sure the file api_routes.py is in the same directory
    from api_routes import api_bp
    app.register_blueprint(api_bp)
    
    # Root route
    @app.route('/')
    def index():
        return jsonify({
            'status': 'running',
            'api_version': '1.0',
            'documentation': '/api/docs'
        })
    
    # API documentation route
    @app.route('/api/docs')
    def api_docs():
        return jsonify({
            'endpoints': [
                {
                    'path': '/api/fetch/<source>',
                    'method': 'POST',
                    'description': 'Trigger data fetching for a specific source',
                    'parameters': {
                        'source': 'Path parameter: 311, mta, tlc, weather, events, or all',
                        'start_date': 'Body parameter (optional): Start date in YYYY-MM-DD format',
                        'end_date': 'Body parameter (optional): End date in YYYY-MM-DD format'
                    }
                },
                # Other endpoints documentation...
            ]
        })
    
    # Error handler for 404
    @app.errorhandler(404)
    def not_found(error):
        return jsonify({'error': 'Resource not found'}), 404
    
    # Error handler for 500
    @app.errorhandler(500)
    def server_error(error):
        logger.error(f"Server error: {error}")
        return jsonify({'error': 'Internal server error'}), 500
    
    return app

if __name__ == '__main__':
    app = create_app()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)