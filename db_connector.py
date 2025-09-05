import pyodbc
from datetime import datetime
from typing import List, Dict, Any

# Database configuration (using provided credentials)
DB_SERVER = "VMHOST1001.hq.cleverex.com"
DB_NAME   = "JoyHS_TestDrive"
DB_USER   = "agent_test"
DB_PASS   = "qxYdGTaR71V3JyQ04oUd"

class DBConnector:
    """Utility class to connect to SQL Server and retrieve schema information."""
    def __init__(self, server: str, database: str, uid: str, pwd: str):
        self.server = server
        self.database = database
        self.uid = uid
        self.pwd = pwd
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """Establish connection to the SQL Server database."""
        try:
            self.connection = pyodbc.connect(
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.server};DATABASE={self.database};"
                f"UID={self.uid};PWD={self.pwd};"
                "Encrypt=yes;TrustServerCertificate=yes;"
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception:
            return False

    def disconnect(self):
        """Close the database connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except Exception:
            pass

    def execute_query(self, query: str, params: tuple = ()) -> List[tuple]:
        """Execute a SQL query and return all results (as list of tuples)."""
        try:
            if params:
                self.cursor.execute(query, *params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception:
            return []  # return empty list on failure

    def execute_query_one(self, query: str) -> tuple:
        """Execute a SQL query and return a single row (tuple) or None."""
        try:
            self.cursor.execute(query)
            return self.cursor.fetchone()
        except Exception:
            return None

    def get_database_info(self) -> Dict[str, Any]:
        """Get basic database information (name, server, version, size)."""
        info = {}
        # Query database name, server name, and version
        result = self.execute_query_one(
            "SELECT @@VERSION as version, DB_NAME() as database_name, @@SERVERNAME as server_name"
        )
        if result:
            info["version"] = result[0]
            info["database_name"] = result[1]
            info["server_name"] = result[2]
        # Query database size in MB
        size_query = """
        SELECT CAST(SUM(size) * 8.0 / 1024 AS DECIMAL(10,2))
        FROM sys.database_files;
        """
        size_result = self.execute_query_one(size_query)
        if size_result and size_result[0] is not None:
            info["size_mb"] = float(size_result[0])
        else:
            info["size_mb"] = 0.0
        return info

    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Retrieve all user tables with schema and optional description."""
        query = """
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            ISNULL(ep.value, 'No description') AS TABLE_COMMENT
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME AND SCHEMA_NAME(st.schema_id) = t.TABLE_SCHEMA
        LEFT JOIN sys.extended_properties ep ON ep.major_id = st.object_id 
            AND ep.minor_id = 0 AND ep.name = 'MS_Description'
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
        """
        results = self.execute_query(query)
        tables = []
        for row in results:
            tables.append({
                "schema": row[0],
                "name": row[1],
                "comment": row[2]
            })
        return tables

    def get_table_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get column details for a specific table (name, type, nullable, default, etc.)."""
        query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            ISNULL(ep.value, 'No description') AS COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN sys.tables st ON st.name = c.TABLE_NAME AND SCHEMA_NAME(st.schema_id) = c.TABLE_SCHEMA
        LEFT JOIN sys.columns sc ON sc.object_id = st.object_id AND sc.name = c.COLUMN_NAME
        LEFT JOIN sys.extended_properties ep ON ep.major_id = st.object_id 
            AND ep.minor_id = sc.column_id AND ep.name = 'MS_Description'
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION;
        """
        results = []
        try:
            # Use parameterized query to avoid issues with identifiers
            self.cursor.execute(query, schema, table)
            results = self.cursor.fetchall()
        except Exception:
            return []
        columns = []
        for row in results:
            columns.append({
                "name": row[0],
                "data_type": row[1],
                "nullable": (row[2] == "YES"),
                "default": row[3],
                "max_length": row[4],
                "precision": row[5],
                "scale": row[6],
                "comment": row[7]
            })
        return columns

    def get_table_row_count(self, schema: str, table: str) -> int:
        """Get the number of rows in a given table."""
        try:
            result = self.execute_query_one(f"SELECT COUNT(*) FROM [{schema}].[{table}];")
            return int(result[0]) if result else 0
        except Exception:
            return 0

    def get_sample_data(self, schema: str, table: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Fetch up to `limit` sample rows from the specified table."""
        sample_data = []
        try:
            results = self.execute_query(f"SELECT TOP {limit} * FROM [{schema}].[{table}];")
            if not results:
                return sample_data
            # Get column names to form dict keys
            columns = [desc[0] for desc in self.cursor.description]  # cursor.description after executing query
            for row in results:
                row_dict = {}
                for col_name, value in zip(columns, row):
                    # Convert datetime to ISO string, otherwise use str() for JSON serialization
                    if isinstance(value, datetime):
                        row_dict[col_name] = value.isoformat()
                    else:
                        row_dict[col_name] = None if value is None else str(value)
                sample_data.append(row_dict)
        except Exception:
            # On error, return what we have (possibly empty list)
            pass
        return sample_data

    def get_foreign_keys(self) -> List[Dict[str, Any]]:
        """Retrieve all foreign key relationships in the database."""
        query = """
        SELECT 
            fk.name AS foreign_key_name,
            SCHEMA_NAME(t1.schema_id) AS from_schema,
            t1.name AS from_table,
            c1.name AS from_column,
            SCHEMA_NAME(t2.schema_id) AS to_schema,
            t2.name AS to_table,
            c2.name AS to_column
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables t1 ON fkc.parent_object_id = t1.object_id
        INNER JOIN sys.columns c1 ON fkc.parent_object_id = c1.object_id AND fkc.parent_column_id = c1.column_id
        INNER JOIN sys.tables t2 ON fkc.referenced_object_id = t2.object_id
        INNER JOIN sys.columns c2 ON fkc.referenced_object_id = c2.object_id AND fkc.referenced_column_id = c2.column_id
        ORDER BY from_schema, from_table, foreign_key_name;
        """
        relationships = []
        try:
            results = self.execute_query(query)
            for row in results:
                relationships.append({
                    "foreign_key": row[0],
                    "from_schema": row[1],
                    "from_table": row[2],
                    "from_column": row[3],
                    "to_schema": row[4],
                    "to_table": row[5],
                    "to_column": row[6]
                })
        except Exception:
            return relationships
        return relationships
