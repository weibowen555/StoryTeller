import json
from agents import function_tool
from db_connector import DBConnector, DB_SERVER, DB_NAME, DB_USER, DB_PASS

@function_tool
def connect_database() -> str:
    """Connect to the SQL Server database and verify connection."""
    connector = DBConnector(DB_SERVER, DB_NAME, DB_USER, DB_PASS)
    try:
        if connector.connect():
            info = connector.get_database_info()
            connector.disconnect()
            # Compose a success message with database name, server and size
            db_name = info.get("database_name", "(unknown)")
            server_name = info.get("server_name", "(unknown server)")
            size_mb = info.get("size_mb", 0.0)
            return f"✅ Successfully connected to database: {db_name} on {server_name}. Database size: {size_mb:.2f} MB."
        else:
            return "❌ Failed to connect to database."
    except Exception as e:
        return f"❌ Database connection error: {str(e)}"
    finally:
        try:
            connector.disconnect()
        except Exception:
            pass

@function_tool
def get_database_schema(include_sample_data: bool = False) -> str:
    """Get complete database schema including tables, columns, and relationships.
    
    Args:
        include_sample_data: Whether to include sample data from tables.
    """
    connector = DBConnector(DB_SERVER, DB_NAME, DB_USER, DB_PASS)
    try:
        if not connector.connect():
            return "❌ Failed to connect to database to retrieve schema."
        # Gather basic database info and table list
        schema_info = {
            "database_info": connector.get_database_info(),
            "tables_count": 0,
            "tables": [],
            "relationships": []
        }
        tables = connector.get_all_tables()
        schema_info["tables_count"] = len(tables)
        # Limit to first 10 tables for brevity in output
        for table in tables[:10]:
            table_detail = {
                "schema": table["schema"],
                "name": table["name"],
                "comment": table.get("comment", "")
            }
            # Get columns and row count for this table
            table_detail["columns"] = connector.get_table_columns(table["schema"], table["name"])
            table_detail["row_count"] = connector.get_table_row_count(table["schema"], table["name"])
            if include_sample_data:
                table_detail["sample_data"] = connector.get_sample_data(table["schema"], table["name"], limit=3)
            schema_info["tables"].append(table_detail)
        # Get all foreign key relationships (across all tables)
        schema_info["relationships"] = connector.get_foreign_keys()
        return json.dumps(schema_info, indent=2, default=str)
    except Exception as e:
        return f"❌ Error retrieving schema: {str(e)}"
    finally:
        try:
            connector.disconnect()
        except Exception:
            pass
