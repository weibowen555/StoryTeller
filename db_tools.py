# db_tools.py

from openai import function_tool
from sql_connect import SQLServerExplorer

DB_CONFIG = {
    'server': 'VMHOST1001.hq.cleverex.com',
    'database': 'JoyHS_TestDrive',
    'uid': 'agent_test',
    'pwd': 'qxYdGTaR71V3JyQ04oUd'
}

@function_tool
def connect_database() -> str:
    """Connect to the SQL Server database and verify connection."""
    explorer = SQLServerExplorer(**DB_CONFIG)
    try:
        if explorer.connect():
            info = explorer.get_database_info()
            explorer.disconnect()
            return (
                f"✅ Successfully connected to database: {info.get('database_name')} "
                f"on {info.get('server_name')}. Database size: {info.get('size_mb', 0):.2f} MB"
            )
        else:
            return "❌ Failed to connect to database"
    except Exception as e:
        return f"❌ Database connection error: {str(e)}"
    finally:
        explorer.disconnect()

@function_tool
def get_database_schema(include_sample_data: bool = False) -> str:
    """Get a summary of the database schema, with optional sample data."""
    explorer = SQLServerExplorer(**DB_CONFIG)
    try:
        if not explorer.connect():
            return "❌ Failed to connect to database"
        tables = explorer.get_all_tables()
        schema_summary = []
        for table in tables[:10]:
            entry = {
                "schema": table['schema'],
                "name": table['name'],
                "row_count": explorer.get_table_row_count(table['schema'], table['name']),
                "columns": explorer.get_table_columns(table['schema'], table['name'])
            }
            if include_sample_data:
                entry["sample_data"] = explorer.get_sample_data(table['schema'], table['name'], 3)
            schema_summary.append(entry)
        return f"📊 Database Schema Summary: {schema_summary}"
    except Exception as e:
        return f"❌ Error getting schema: {str(e)}"
    finally:
        explorer.disconnect()
