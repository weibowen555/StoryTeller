"""
SQL Server Database Explorer
Comprehensive script to extract all information from a SQL Server database
"""

import pyodbc
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Any
import os

class SQLServerExplorer:
    def __init__(self, server: str, database: str, uid: str, pwd: str):
        self.server = server
        self.database = database
        self.uid = uid
        self.pwd = pwd
        self.connection = None
        self.cursor = None
        
    def connect(self):
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
            print(f"✅ Connected to {self.database} on {self.server}")
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
    
    def execute_query(self, query: str, fetch_all: bool = True) -> List[tuple]:
        """Execute a query and return results"""
        try:
            self.cursor.execute(query)
            if fetch_all:
                return self.cursor.fetchall()
            else:
                return self.cursor.fetchone()
        except Exception as e:
            print(f"❌ Query failed: {e}")
            return []
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get basic database information"""
        print("📊 Getting database information...")
        
        info = {}
        
        # Database version and info
        version_query = "SELECT @@VERSION as Version, DB_NAME() as DatabaseName, @@SERVERNAME as ServerName"
        result = self.execute_query(version_query, fetch_all=False)
        if result:
            info['version'] = result[0]
            info['database_name'] = result[1]
            info['server_name'] = result[2]
        
        # Database size
        size_query = """
        SELECT 
            DB_NAME() as DatabaseName,
            CAST(SUM(size) * 8.0 / 1024 AS DECIMAL(10,2)) as SizeMB
        FROM sys.database_files
        """
        result = self.execute_query(size_query, fetch_all=False)
        if result:
            info['size_mb'] = float(result[1]) if result[1] else 0
        
        return info
    
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Get all tables with their properties"""
        print("📋 Getting all tables...")
        
        query = """
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            t.TABLE_TYPE,
            ISNULL(ep.value, 'No description') as TABLE_COMMENT
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME
        LEFT JOIN sys.extended_properties ep ON ep.major_id = st.object_id 
            AND ep.minor_id = 0 AND ep.name = 'MS_Description'
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
        """
        
        results = self.execute_query(query)
        tables = []
        
        for row in results:
            tables.append({
                'schema': row[0],
                'name': row[1],
                'type': row[2],
                'comment': row[3]
            })
        
        return tables
    
    def get_table_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all columns for a specific table"""
        query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            ISNULL(ep.value, 'No description') as COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN sys.tables t ON t.name = c.TABLE_NAME
        LEFT JOIN sys.columns sc ON sc.object_id = t.object_id AND sc.name = c.COLUMN_NAME
        LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id 
            AND ep.minor_id = sc.column_id AND ep.name = 'MS_Description'
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """
        
        self.cursor.execute(query, schema, table)
        results = self.cursor.fetchall()
        
        columns = []
        for row in results:
            columns.append({
                'name': row[0],
                'data_type': row[1],
                'nullable': row[2] == 'YES',
                'default': row[3],
                'max_length': row[4],
                'precision': row[5],
                'scale': row[6],
                'comment': row[7]
            })
        
        return columns
    
    def get_table_row_count(self, schema: str, table: str) -> int:
        """Get row count for a table"""
        try:
            query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
            result = self.execute_query(query, fetch_all=False)
            return result[0] if result else 0
        except:
            return 0
    
    def get_sample_data(self, schema: str, table: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get sample data from a table"""
        try:
            query = f"SELECT TOP {limit} * FROM [{schema}].[{table}]"
            results = self.execute_query(query)
            
            # Get column names
            columns = self.get_table_columns(schema, table)
            column_names = [col['name'] for col in columns]
            
            sample_data = []
            for row in results:
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(column_names):
                        # Convert datetime and other types to string for JSON serialization
                        if isinstance(value, datetime):
                            row_dict[column_names[i]] = value.isoformat()
                        else:
                            row_dict[column_names[i]] = str(value) if value is not None else None
                sample_data.append(row_dict)
            
            return sample_data
        except Exception as e:
            print(f"❌ Error getting sample data for {schema}.{table}: {e}")
            return []
    
    def get_views(self) -> List[Dict[str, Any]]:
        """Get all views"""
        print("👁️ Getting all views...")
        
        query = """
        SELECT 
            TABLE_SCHEMA,
            TABLE_NAME,
            VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        
        results = self.execute_query(query)
        views = []
        
        for row in results:
            views.append({
                'schema': row[0],
                'name': row[1],
                'definition': row[2][:500] + '...' if len(row[2]) > 500 else row[2]  # Truncate long definitions
            })
        
        return views
    
    def get_stored_procedures(self) -> List[Dict[str, Any]]:
        """Get all stored procedures"""
        print("⚙️ Getting stored procedures...")
        
        query = """
        SELECT 
            ROUTINE_SCHEMA,
            ROUTINE_NAME,
            ROUTINE_TYPE,
            CREATED,
            LAST_ALTERED
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'PROCEDURE'
        ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
        """
        
        results = self.execute_query(query)
        procedures = []
        
        for row in results:
            procedures.append({
                'schema': row[0],
                'name': row[1],
                'type': row[2],
                'created': row[3].isoformat() if row[3] else None,
                'last_altered': row[4].isoformat() if row[4] else None
            })
        
        return procedures
    
    def get_functions(self) -> List[Dict[str, Any]]:
        """Get all functions"""
        print("🔧 Getting functions...")
        
        query = """
        SELECT 
            ROUTINE_SCHEMA,
            ROUTINE_NAME,
            ROUTINE_TYPE,
            DATA_TYPE,
            CREATED,
            LAST_ALTERED
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'FUNCTION'
        ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
        """
        
        results = self.execute_query(query)
        functions = []
        
        for row in results:
            functions.append({
                'schema': row[0],
                'name': row[1],
                'type': row[2],
                'return_type': row[3],
                'created': row[4].isoformat() if row[4] else None,
                'last_altered': row[5].isoformat() if row[5] else None
            })
        
        return functions
    
    def get_indexes(self) -> List[Dict[str, Any]]:
        """Get all indexes"""
        print("🔍 Getting indexes...")
        
        query = """
        SELECT 
            s.name as schema_name,
            t.name as table_name,
            i.name as index_name,
            i.type_desc as index_type,
            i.is_unique,
            i.is_primary_key,
            STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) as columns
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.name IS NOT NULL
        GROUP BY s.name, t.name, i.name, i.type_desc, i.is_unique, i.is_primary_key
        ORDER BY s.name, t.name, i.name
        """
        
        results = self.execute_query(query)
        indexes = []
        
        for row in results:
            indexes.append({
                'schema': row[0],
                'table': row[1],
                'name': row[2],
                'type': row[3],
                'is_unique': bool(row[4]),
                'is_primary_key': bool(row[5]),
                'columns': row[6]
            })
        
        return indexes
    
    def get_foreign_keys(self) -> List[Dict[str, Any]]:
        """Get all foreign key relationships"""
        print("🔗 Getting foreign keys...")
        
        query = """
        SELECT 
            fk.name as foreign_key_name,
            SCHEMA_NAME(t1.schema_id) as from_schema,
            t1.name as from_table,
            c1.name as from_column,
            SCHEMA_NAME(t2.schema_id) as to_schema,
            t2.name as to_table,
            c2.name as to_column
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables t1 ON fkc.parent_object_id = t1.object_id
        INNER JOIN sys.columns c1 ON fkc.parent_object_id = c1.object_id AND fkc.parent_column_id = c1.column_id
        INNER JOIN sys.tables t2 ON fkc.referenced_object_id = t2.object_id
        INNER JOIN sys.columns c2 ON fkc.referenced_object_id = c2.object_id AND fkc.referenced_column_id = c2.column_id
        ORDER BY from_schema, from_table, foreign_key_name
        """
        
        results = self.execute_query(query)
        foreign_keys = []
        
        for row in results:
            foreign_keys.append({
                'name': row[0],
                'from_schema': row[1],
                'from_table': row[2],
                'from_column': row[3],
                'to_schema': row[4],
                'to_table': row[5],
                'to_column': row[6]
            })
        
        return foreign_keys
    
    def export_complete_analysis(self, output_dir: str = "database_analysis"):
        """Export complete database analysis"""
        print(f"📁 Creating complete database analysis in '{output_dir}' directory...")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Get all information
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'database_info': self.get_database_info(),
            'tables': [],
            'views': self.get_views(),
            'stored_procedures': self.get_stored_procedures(),
            'functions': self.get_functions(),
            'indexes': self.get_indexes(),
            'foreign_keys': self.get_foreign_keys()
        }
        
        # Get detailed table information
        tables = self.get_all_tables()
        print(f"📊 Analyzing {len(tables)} tables...")
        
        for i, table in enumerate(tables, 1):
            print(f"  Processing table {i}/{len(tables)}: {table['schema']}.{table['name']}")
            
            table_detail = {
                'schema': table['schema'],
                'name': table['name'],
                'type': table['type'],
                'comment': table['comment'],
                'columns': self.get_table_columns(table['schema'], table['name']),
                'row_count': self.get_table_row_count(table['schema'], table['name']),
                'sample_data': self.get_sample_data(table['schema'], table['name'], 3)
            }
            
            analysis['tables'].append(table_detail)
        
        # Save main analysis file
        with open(f"{output_dir}/complete_analysis.json", 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)
        
        # Create summary report
        self.create_summary_report(analysis, output_dir)
        
        # Create individual table files
        self.create_table_files(analysis['tables'], output_dir)
        
        print(f"✅ Analysis complete! Files saved in '{output_dir}' directory")
        return analysis
    
    def create_summary_report(self, analysis: Dict[str, Any], output_dir: str):
        """Create a human-readable summary report"""
        with open(f"{output_dir}/summary_report.txt", 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("DATABASE ANALYSIS SUMMARY REPORT\n")
            f.write("=" * 80 + "\n\n")
            
            # Database info
            db_info = analysis['database_info']
            f.write(f"Database: {db_info.get('database_name', 'Unknown')}\n")
            f.write(f"Server: {db_info.get('server_name', 'Unknown')}\n")
            f.write(f"Size: {db_info.get('size_mb', 0):.2f} MB\n")
            f.write(f"Analysis Date: {analysis['timestamp']}\n\n")
            
            # Tables summary
            tables = analysis['tables']
            f.write(f"TABLES ({len(tables)} total):\n")
            f.write("-" * 40 + "\n")
            total_rows = 0
            for table in tables:
                row_count = table['row_count']
                total_rows += row_count
                f.write(f"{table['schema']}.{table['name']}: {row_count:,} rows, {len(table['columns'])} columns\n")
            f.write(f"\nTotal rows across all tables: {total_rows:,}\n\n")
            
            # Views summary
            views = analysis['views']
            f.write(f"VIEWS ({len(views)} total):\n")
            f.write("-" * 40 + "\n")
            for view in views:
                f.write(f"{view['schema']}.{view['name']}\n")
            f.write("\n")
            
            # Stored procedures summary
            procedures = analysis['stored_procedures']
            f.write(f"STORED PROCEDURES ({len(procedures)} total):\n")
            f.write("-" * 40 + "\n")
            for proc in procedures:
                f.write(f"{proc['schema']}.{proc['name']}\n")
            f.write("\n")
            
            # Functions summary
            functions = analysis['functions']
            f.write(f"FUNCTIONS ({len(functions)} total):\n")
            f.write("-" * 40 + "\n")
            for func in functions:
                f.write(f"{func['schema']}.{func['name']} -> {func['return_type']}\n")
            f.write("\n")
            
            # Foreign keys summary
            fks = analysis['foreign_keys']
            f.write(f"FOREIGN KEY RELATIONSHIPS ({len(fks)} total):\n")
            f.write("-" * 40 + "\n")
            for fk in fks:
                f.write(f"{fk['from_schema']}.{fk['from_table']}.{fk['from_column']} -> {fk['to_schema']}.{fk['to_table']}.{fk['to_column']}\n")
    
    def create_table_files(self, tables: List[Dict[str, Any]], output_dir: str):
        """Create individual CSV files for each table's sample data"""
        tables_dir = f"{output_dir}/sample_data"
        os.makedirs(tables_dir, exist_ok=True)
        
        for table in tables:
            if table['sample_data']:
                filename = f"{table['schema']}_{table['name']}_sample.csv"
                filepath = f"{tables_dir}/{filename}"
                
                df = pd.DataFrame(table['sample_data'])
                df.to_csv(filepath, index=False, encoding='utf-8')


def main():
    """Main function to run the database exploration"""
    # Database connection parameters
    SERVER = 'VMHOST1001.hq.cleverex.com'
    DATABASE = 'JoyHS_TestDrive'
    UID = 'agent_test'
    PWD = 'qxYdGTaR71V3JyQ04oUd'
    
    # Create explorer instance
    explorer = SQLServerExplorer(SERVER, DATABASE, UID, PWD)
    
    try:
        # Connect to database
        if explorer.connect():
            # Export complete analysis
            analysis = explorer.export_complete_analysis()
            
            # Print quick summary
            print("\n" + "=" * 60)
            print("QUICK SUMMARY")
            print("=" * 60)
            print(f"Database: {analysis['database_info'].get('database_name')}")
            print(f"Tables: {len(analysis['tables'])}")
            print(f"Views: {len(analysis['views'])}")
            print(f"Stored Procedures: {len(analysis['stored_procedures'])}")
            print(f"Functions: {len(analysis['functions'])}")
            print(f"Indexes: {len(analysis['indexes'])}")
            print(f"Foreign Keys: {len(analysis['foreign_keys'])}")
            
            total_rows = sum(table['row_count'] for table in analysis['tables'])
            print(f"Total rows: {total_rows:,}")
            
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
    finally:
        explorer.disconnect()


if __name__ == "__main__":
    main()