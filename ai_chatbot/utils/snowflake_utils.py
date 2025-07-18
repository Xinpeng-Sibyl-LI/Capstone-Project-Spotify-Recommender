# ai_chatbot/utils/snowflake_utils.py
"""
Snowflake connection utilities
"""

import snowflake.connector as sf
from dotenv import load_dotenv
import os
import logging
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)
load_dotenv()

# Connection pool for reuse
_connection_pool = []

def get_snowflake_connection():
    """Get a Snowflake connection from pool or create new one"""
    # Try to reuse connection from pool
    if _connection_pool:
        conn = _connection_pool.pop()
        try:
            # Test if connection is still valid
            conn.cursor().execute("SELECT 1")
            return conn
        except:
            # Connection is dead, create new one
            pass
    
    # Create new connection
    try:
        conn = sf.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW")
        )
        logger.info("Created new Snowflake connection")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise

def return_connection(conn):
    """Return connection to pool for reuse"""
    if len(_connection_pool) < 3:  # Keep max 3 connections
        _connection_pool.append(conn)
    else:
        conn.close()

def test_connection() -> bool:
    """Test if Snowflake connection works"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP()")
        result = cursor.fetchone()
        cursor.close()
        return_connection(conn)
        logger.info(f"Snowflake connection test successful: {result[0]}")
        return True
    except Exception as e:
        logger.error(f"Snowflake connection test failed: {e}")
        return False

def execute_query(sql: str) -> List[Dict[str, Any]]:
    """Execute a SQL query and return results as list of dictionaries"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        cursor.execute(sql)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch all results
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        
        cursor.close()
        return_connection(conn)
        
        return results
        
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise

def get_table_info(table_name: str) -> Optional[Dict]:
    """Get information about a table"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Get columns
        cursor.execute(f"DESCRIBE TABLE {table_name}")
        columns = cursor.fetchall()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        cursor.close()
        return_connection(conn)
        
        return {
            'table_name': table_name,
            'columns': [{'name': col[0], 'type': col[1]} for col in columns],
            'row_count': row_count
        }
    except Exception as e:
        logger.error(f"Error getting table info: {e}")
        return None