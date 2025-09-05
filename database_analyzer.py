"""
Database Analyzer Module
Handles database schema analysis, filtering, and metadata extraction
"""

import pyodbc
import pandas as pd
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import os


class DatabaseAnalyzer:
    """Handles database connection, schema analysis, and metadata extraction"""
    
    def __init__(self, server: str, database: str, uid: str, pwd: str):
        self.server = server
        self.database = database
        self.uid = uid
        self.pwd = pwd
        self.connection = None
        self.cursor = None
        self.schema_info = {}
        self.table_columns = {}
        self.table_stats = {}
        self.filtered_tables = {}
        
    def connect(self) -> bool:
        """Establish connection to SQL Server"""
        try:
            self.connection = pyodbc.connect(
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.uid};PWD={self.pwd};"
                "Encrypt=yes;TrustServerCertificate=yes;"
            )
            self.cursor = self.connection.cursor()
            print(f"✅ Connected to {self.database}")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("🔌 Disconnected from database")
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a specific table"""
        try:
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            self.cursor.execute(count_query)
            result = self.cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            print(f"⚠️ Error getting row count for {table_name}: {e}")
            return 0
    
    def get_table_sample_data(self, table_name: str, sample_size: int = 5) -> List[Dict]:
        """Get sample data from table for analysis"""
        try:
            sample_query = f"SELECT TOP {sample_size} * FROM {table_name}"
            df = pd.read_sql(sample_query, self.connection)
            return df.to_dict('records')
        except Exception as e:
            print(f"⚠️ Error getting sample data from {table_name}: {e}")
            return []
    
    def analyze_column_data(self, table_name: str, column_info: List[Dict]) -> Dict:
        """Analyze column data patterns and statistics"""
        analysis = {}
        
        try:
            for col in column_info:
                col_name = col['name']
                col_type = col['type']
                
                # Get basic statistics
                stats_query = f"""
                SELECT 
                    COUNT(*) as total_count,
                    COUNT({col_name}) as non_null_count,
                    COUNT(DISTINCT {col_name}) as unique_count
                FROM {table_name}
                """
                
                self.cursor.execute(stats_query)
                result = self.cursor.fetchone()
                
                if result:
                    total, non_null, unique = result
                    analysis[col_name] = {
                        'type': col_type,
                        'total_count': total,
                        'non_null_count': non_null,
                        'unique_count': unique,
                        'null_percentage': round(((total - non_null) / total * 100), 2) if total > 0 else 0,
                        'uniqueness_ratio': round((unique / non_null), 4) if non_null > 0 else 0
                    }
                    
                    # Additional analysis for specific data types
                    self._analyze_column_by_type(table_name, col_name, col_type, analysis[col_name])
                            
        except Exception as e:
            print(f"⚠️ Error analyzing columns for {table_name}: {e}")
        
        return analysis
    
    def _analyze_column_by_type(self, table_name: str, col_name: str, col_type: str, analysis: Dict):
        """Analyze column based on its data type"""
        try:
            if col_type in ['varchar', 'nvarchar', 'text', 'ntext'] and analysis['non_null_count'] > 0:
                # String analysis
                length_query = f"""
                SELECT 
                    MIN(LEN({col_name})) as min_length,
                    MAX(LEN({col_name})) as max_length,
                    AVG(CAST(LEN({col_name}) AS FLOAT)) as avg_length
                FROM {table_name} 
                WHERE {col_name} IS NOT NULL
                """
                self.cursor.execute(length_query)
                length_result = self.cursor.fetchone()
                if length_result:
                    analysis.update({
                        'min_length': length_result[0],
                        'max_length': length_result[1],
                        'avg_length': round(length_result[2], 2) if length_result[2] else 0
                    })
            
            elif col_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real'] and analysis['non_null_count'] > 0:
                # Numeric analysis
                numeric_query = f"""
                SELECT 
                    MIN({col_name}) as min_value,
                    MAX({col_name}) as max_value,
                    AVG(CAST({col_name} AS FLOAT)) as avg_value
                FROM {table_name} 
                WHERE {col_name} IS NOT NULL
                """
                self.cursor.execute(numeric_query)
                numeric_result = self.cursor.fetchone()
                if numeric_result:
                    analysis.update({
                        'min_value': numeric_result[0],
                        'max_value': numeric_result[1],
                        'avg_value': round(numeric_result[2], 2) if numeric_result[2] else 0
                    })
            
            elif col_type in ['datetime', 'date', 'timestamp'] and analysis['non_null_count'] > 0:
                # Date analysis
                date_query = f"""
                SELECT 
                    MIN({col_name}) as earliest_date,
                    MAX({col_name}) as latest_date
                FROM {table_name} 
                WHERE {col_name} IS NOT NULL
                """
                self.cursor.execute(date_query)
                date_result = self.cursor.fetchone()
                if date_result:
                    analysis.update({
                        'earliest_date': str(date_result[0]) if date_result[0] else None,
                        'latest_date': str(date_result[1]) if date_result[1] else None
                    })
        except Exception as e:
            print(f"⚠️ Error in type-specific analysis for {col_name}: {e}")
    
    def load_and_filter_schema(self, min_rows: int = 1):
        """Load database schema and filter out empty tables"""
        print("📋 Loading and filtering database schema...")
        
        # Get all tables and their columns
        schema_query = """
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.TABLES t
        INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME 
            AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        self.cursor.execute(schema_query)
        results = self.cursor.fetchall()
        
        # Group by table
        table_data = {}
        for row in results:
            schema, table, column, data_type, nullable, default, position = row
            table_key = f"{schema}.{table}"
            
            if table_key not in table_data:
                table_data[table_key] = []
                self.schema_info[table.lower()] = table_key
            
            table_data[table_key].append({
                'name': column,
                'type': data_type,
                'nullable': nullable == 'YES',
                'default': default,
                'position': position
            })
        
        print(f"📊 Found {len(table_data)} tables. Filtering for tables with data...")
        
        # Filter tables with data and analyze them
        self._filter_and_analyze_tables(table_data, min_rows)
    
    def _filter_and_analyze_tables(self, table_data: Dict, min_rows: int):
        """Filter tables with data and perform analysis"""
        tables_processed = 0
        tables_with_data = 0
        
        for table_key, columns in table_data.items():
            tables_processed += 1
            print(f"🔍 Processing table {tables_processed}/{len(table_data)}: {table_key}", end="")
            
            # Get row count
            row_count = self.get_table_row_count(table_key)
            
            if row_count >= min_rows:
                tables_with_data += 1
                print(f" ✅ ({row_count:,} rows)")
                
                # Store table information
                self.table_columns[table_key] = columns
                self.filtered_tables[table_key] = {
                    'row_count': row_count,
                    'column_count': len(columns),
                    'columns': columns
                }
                
                # Get sample data
                sample_data = self.get_table_sample_data(table_key, 3)
                
                # Analyze columns (for smaller tables or sample analysis)
                if row_count <= 10000:  # Full analysis for smaller tables
                    column_analysis = self.analyze_column_data(table_key, columns)
                else:  # Basic analysis for larger tables
                    column_analysis = {}
                
                self.table_stats[table_key] = {
                    'row_count': row_count,
                    'column_count': len(columns),
                    'sample_data': sample_data,
                    'column_analysis': column_analysis,
                    'last_analyzed': datetime.now().isoformat()
                }
                
            else:
                print(f" ⚠️ Empty ({row_count} rows) - Skipped")
        
        print(f"\n✅ Schema filtering complete:")
        print(f"   📊 Total tables found: {len(table_data)}")
        print(f"   ✅ Tables with data: {tables_with_data}")
        print(f"   ❌ Empty tables skipped: {len(table_data) - tables_with_data}")
    
    def get_available_tables(self) -> List[str]:
        """Get list of available tables with data"""
        return list(self.filtered_tables.keys())
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a specific table"""
        if table_name in self.table_columns:
            return self.table_columns[table_name]
        
        # Try to find by partial name
        for table_key in self.table_columns:
            if table_name.lower() in table_key.lower():
                return self.table_columns[table_key]
        
        return []
    
    def get_table_statistics(self, table_name: str = None) -> Dict[str, Any]:
        """Get comprehensive statistics for tables"""
        if table_name:
            return self.table_stats.get(table_name, {})
        else:
            return {
                'total_tables': len(self.filtered_tables),
                'total_rows': sum(stats['row_count'] for stats in self.table_stats.values()),
                'total_columns': sum(stats['column_count'] for stats in self.table_stats.values()),
                'largest_tables': sorted([(table, stats['row_count']) 
                                        for table, stats in self.table_stats.items()],
                                       key=lambda x: x[1], reverse=True)[:5]
            }
    
    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        try:
            print(f"🔍 Executing query:\n{sql}\n")
            df = pd.read_sql(sql, self.connection)
            print(f"✅ Query executed successfully. Retrieved {len(df)} rows.")
            return df
        except Exception as e:
            print(f"❌ Query execution failed: {e}")
            return pd.DataFrame()