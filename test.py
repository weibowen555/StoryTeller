"""
Enhanced Natural Language to SQL Converter
Converts natural language queries to SQL statements with database filtering and JSON export
"""

import pyodbc
import re
import json
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pandas as pd
import os
from pathlib import Path

class EnhancedNaturalLanguageToSQL:
    def __init__(self, server: str, database: str, uid: str, pwd: str):
        self.server = server
        self.database = database
        self.uid = uid
        self.pwd = pwd
        self.connection = None
        self.cursor = None
        self.schema_info = {}
        self.table_columns = {}
        self.table_stats = {}  # Store table statistics
        self.filtered_tables = {}  # Only tables with data
        self.database_metadata = {}  # Complete database information
        
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
                    if col_type in ['varchar', 'nvarchar', 'text', 'ntext'] and non_null > 0:
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
                            analysis[col_name].update({
                                'min_length': length_result[0],
                                'max_length': length_result[1],
                                'avg_length': round(length_result[2], 2) if length_result[2] else 0
                            })
                    
                    elif col_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real'] and non_null > 0:
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
                            analysis[col_name].update({
                                'min_value': numeric_result[0],
                                'max_value': numeric_result[1],
                                'avg_value': round(numeric_result[2], 2) if numeric_result[2] else 0
                            })
                    
                    elif col_type in ['datetime', 'date', 'timestamp'] and non_null > 0:
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
                            analysis[col_name].update({
                                'earliest_date': str(date_result[0]) if date_result[0] else None,
                                'latest_date': str(date_result[1]) if date_result[1] else None
                            })
                            
        except Exception as e:
            print(f"⚠️ Error analyzing columns for {table_name}: {e}")
        
        return analysis
    
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
    
    def export_database_metadata(self, filename: str = None) -> str:
        """Export all database metadata to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"database_metadata_{self.database}_{timestamp}.json"
        
        # Prepare comprehensive metadata
        metadata = {
            'database_info': {
                'server': self.server,
                'database': self.database,
                'export_timestamp': datetime.now().isoformat(),
                'total_tables': len(self.filtered_tables),
                'total_rows': sum(stats['row_count'] for stats in self.table_stats.values())
            },
            'tables': {}
        }
        
        # Add detailed table information
        for table_key, table_info in self.filtered_tables.items():
            table_name = table_key.split('.')[-1]
            
            metadata['tables'][table_key] = {
                'table_name': table_name,
                'schema': table_key.split('.')[0],
                'row_count': table_info['row_count'],
                'column_count': table_info['column_count'],
                'columns': [
                    {
                        'name': col['name'],
                        'type': col['type'],
                        'nullable': col['nullable'],
                        'position': col['position']
                    }
                    for col in table_info['columns']
                ],
                'sample_data': self.table_stats.get(table_key, {}).get('sample_data', []),
                'column_analysis': self.table_stats.get(table_key, {}).get('column_analysis', {}),
                'statistics': {
                    'data_quality_score': self.calculate_data_quality_score(table_key),
                    'primary_key_candidates': self.identify_primary_key_candidates(table_key),
                    'foreign_key_candidates': self.identify_foreign_key_candidates(table_key)
                }
            }
        
        # Save to file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"📁 Database metadata exported to: {filename}")
            print(f"   📊 File size: {os.path.getsize(filename):,} bytes")
            return filename
            
        except Exception as e:
            print(f"❌ Error exporting metadata: {e}")
            return None
    
    def calculate_data_quality_score(self, table_key: str) -> float:
        """Calculate a data quality score for the table"""
        if table_key not in self.table_stats:
            return 0.0
        
        stats = self.table_stats[table_key]
        column_analysis = stats.get('column_analysis', {})
        
        if not column_analysis:
            return 0.5  # Default score for tables without detailed analysis
        
        scores = []
        for col_name, col_stats in column_analysis.items():
            # Factors: low null percentage, reasonable uniqueness, data presence
            null_score = max(0, (100 - col_stats.get('null_percentage', 100)) / 100)
            uniqueness_score = min(1, col_stats.get('uniqueness_ratio', 0))
            data_presence_score = 1 if col_stats.get('non_null_count', 0) > 0 else 0
            
            col_score = (null_score * 0.5 + uniqueness_score * 0.3 + data_presence_score * 0.2)
            scores.append(col_score)
        
        return round(sum(scores) / len(scores), 3) if scores else 0.0
    
    def identify_primary_key_candidates(self, table_key: str) -> List[str]:
        """Identify potential primary key columns"""
        if table_key not in self.table_stats:
            return []
        
        column_analysis = self.table_stats[table_key].get('column_analysis', {})
        candidates = []
        
        for col_name, col_stats in column_analysis.items():
            # Primary key candidates: unique, non-null, reasonable data type
            uniqueness_ratio = col_stats.get('uniqueness_ratio', 0)
            null_percentage = col_stats.get('null_percentage', 100)
            col_type = col_stats.get('type', '').lower()
            
            if (uniqueness_ratio >= 0.95 and 
                null_percentage < 5 and 
                col_type in ['int', 'bigint', 'uniqueidentifier', 'varchar', 'nvarchar']):
                candidates.append(col_name)
        
        return candidates
    
    def identify_foreign_key_candidates(self, table_key: str) -> List[Dict]:
        """Identify potential foreign key relationships"""
        if table_key not in self.table_stats:
            return []
        
        column_analysis = self.table_stats[table_key].get('column_analysis', {})
        candidates = []
        
        for col_name, col_stats in column_analysis.items():
            col_type = col_stats.get('type', '').lower()
            
            # Look for columns that might reference other tables
            if (col_type in ['int', 'bigint'] and 
                col_name.lower().endswith('id') and 
                not col_name.lower() in ['id', 'rowid']):
                
                # Try to find matching tables
                potential_table = col_name.lower().replace('_id', '').replace('id', '')
                matching_tables = [t for t in self.filtered_tables.keys() 
                                 if potential_table in t.lower().split('.')[-1]]
                
                if matching_tables:
                    candidates.append({
                        'column': col_name,
                        'potential_references': matching_tables[:3]  # Top 3 matches
                    })
        
        return candidates
    
    def load_metadata_from_json(self, filename: str) -> bool:
        """Load database metadata from JSON file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            self.database_metadata = metadata
            
            # Reconstruct internal structures for querying
            for table_key, table_info in metadata['tables'].items():
                self.filtered_tables[table_key] = {
                    'row_count': table_info['row_count'],
                    'column_count': table_info['column_count'],
                    'columns': table_info['columns']
                }
                
                self.table_columns[table_key] = table_info['columns']
                table_name = table_info['table_name'].lower()
                self.schema_info[table_name] = table_key
            
            print(f"✅ Loaded metadata for {len(metadata['tables'])} tables from {filename}")
            return True
            
        except Exception as e:
            print(f"❌ Error loading metadata from {filename}: {e}")
            return False
    
    def analyze_user_query_context(self, natural_query: str) -> Dict[str, Any]:
        """Analyze user query to provide better context and suggestions"""
        query_lower = natural_query.lower()
        
        analysis = {
            'query_type': 'unknown',
            'complexity': 'simple',
            'suggested_tables': [],
            'suggested_columns': [],
            'query_intent': [],
            'data_requirements': []
        }
        
        # Determine query type
        if any(word in query_lower for word in ['count', 'how many', 'total number']):
            analysis['query_type'] = 'aggregation'
            analysis['query_intent'].append('count_records')
        elif any(word in query_lower for word in ['average', 'mean', 'avg']):
            analysis['query_type'] = 'aggregation'
            analysis['query_intent'].append('calculate_average')
        elif any(word in query_lower for word in ['sum', 'total']):
            analysis['query_type'] = 'aggregation'
            analysis['query_intent'].append('sum_values')
        elif any(word in query_lower for word in ['show', 'list', 'display', 'get']):
            analysis['query_type'] = 'retrieval'
            analysis['query_intent'].append('retrieve_data')
        elif any(word in query_lower for word in ['find', 'search', 'where']):
            analysis['query_type'] = 'filtered_retrieval'
            analysis['query_intent'].append('search_data')
        
        # Determine complexity
        complexity_indicators = len([word for word in ['where', 'and', 'or', 'join', 'group', 'order', 'having'] 
                                   if word in query_lower])
        if complexity_indicators >= 3:
            analysis['complexity'] = 'complex'
        elif complexity_indicators >= 1:
            analysis['complexity'] = 'moderate'
        
        # Suggest relevant tables based on keywords
        keywords = query_lower.split()
        for keyword in keywords:
            # Find tables with similar names
            matching_tables = [table_key for table_key in self.filtered_tables.keys()
                             if keyword in table_key.lower().split('.')[-1]]
            analysis['suggested_tables'].extend(matching_tables)
        
        # Remove duplicates and limit suggestions
        analysis['suggested_tables'] = list(set(analysis['suggested_tables']))[:5]
        
        return analysis
    
    def find_table_by_name(self, table_hint: str) -> Optional[str]:
        """Find table name from natural language hint"""
        table_hint = table_hint.lower().strip()
        
        # Direct match
        if table_hint in self.schema_info:
            return self.schema_info[table_hint]
        
        # Partial match
        for table_name, full_name in self.schema_info.items():
            if table_hint in table_name or table_name in table_hint:
                return full_name
        
        # Keywords matching
        for table_name, full_name in self.schema_info.items():
            if any(word in table_name for word in table_hint.split()):
                return full_name
        
        return None
    
    def find_columns_by_hint(self, table_name: str, column_hints: List[str]) -> List[str]:
        """Find column names from natural language hints"""
        if table_name not in self.table_columns:
            return []
        
        columns = self.table_columns[table_name]
        matched_columns = []
        
        for hint in column_hints:
            hint = hint.lower().strip()
            
            # Direct match
            for col in columns:
                if hint == col['name'].lower():
                    matched_columns.append(col['name'])
                    break
            else:
                # Partial match
                for col in columns:
                    if hint in col['name'].lower() or col['name'].lower() in hint:
                        matched_columns.append(col['name'])
                        break
        
        return matched_columns
    
    def parse_natural_language(self, query: str) -> Dict[str, Any]:
        """Parse natural language query into structured components with enhanced analysis"""
        query = query.lower().strip()
        
        # Get query context analysis
        context = self.analyze_user_query_context(query)
        
        parsed = {
            'action': 'SELECT',
            'table': None,
            'columns': [],
            'conditions': [],
            'limit': None,
            'order_by': [],
            'group_by': [],
            'aggregations': [],
            'context': context
        }
        
        # Use context to improve parsing
        if context['query_type'] == 'aggregation':
            if 'count_records' in context['query_intent']:
                parsed['aggregations'].append('COUNT')
            elif 'calculate_average' in context['query_intent']:
                parsed['aggregations'].append('AVG')
            elif 'sum_values' in context['query_intent']:
                parsed['aggregations'].append('SUM')
        
        # Enhanced table detection using context suggestions
        table_patterns = [
            r'from\s+(\w+)',
            r'in\s+(\w+)\s+table',
            r'(\w+)\s+table',
            r'all\s+(\w+)',
        ]
        
        for pattern in table_patterns:
            match = re.search(pattern, query)
            if match:
                table_hint = match.group(1)
                parsed['table'] = self.find_table_by_name(table_hint)
                break
        
        # If no table found, use context suggestions
        if not parsed['table'] and context['suggested_tables']:
            parsed['table'] = context['suggested_tables'][0]
        
        # If still no table found, try to extract from context
        if not parsed['table']:
            words = query.split()
            for word in words:
                table_match = self.find_table_by_name(word)
                if table_match:
                    parsed['table'] = table_match
                    break
        
        # Extract column hints
        column_patterns = [
            r'show\s+([\w\s,]+?)(?:\s+from|\s+where|$)',
            r'get\s+([\w\s,]+?)(?:\s+from|\s+where|$)',
            r'select\s+([\w\s,]+?)(?:\s+from|\s+where|$)',
        ]
        
        for pattern in column_patterns:
            match = re.search(pattern, query)
            if match:
                column_text = match.group(1).strip()
                if column_text not in ['all', '*', 'everything']:
                    column_hints = [col.strip() for col in re.split(r'[,\s]+', column_text) if col.strip()]
                    if parsed['table']:
                        parsed['columns'] = self.find_columns_by_hint(parsed['table'], column_hints)
                break
        
        # Extract conditions (WHERE clauses)
        condition_patterns = [
            r'where\s+(.+?)(?:\s+order|\s+group|\s+limit|$)',
            r'with\s+(.+?)(?:\s+order|\s+group|\s+limit|$)',
            r'that\s+(.+?)(?:\s+order|\s+group|\s+limit|$)',
        ]
        
        for pattern in condition_patterns:
            match = re.search(pattern, query)
            if match:
                condition_text = match.group(1).strip()
                parsed['conditions'] = self.parse_conditions(condition_text, parsed['table'])
                break
        
        # Extract LIMIT
        limit_patterns = [
            r'(?:top|first|limit)\s+(\d+)',
            r'(\d+)\s+(?:rows?|records?)',
        ]
        
        for pattern in limit_patterns:
            match = re.search(pattern, query)
            if match:
                parsed['limit'] = int(match.group(1))
                break
        
        return parsed
    
    def parse_conditions(self, condition_text: str, table_name: str) -> List[str]:
        """Parse condition text into SQL WHERE clauses"""
        if not table_name or table_name not in self.table_columns:
            return []
        
        conditions = []
        
        # Simple condition patterns
        patterns = [
            (r'(\w+)\s*=\s*["\']([^"\']+)["\']', lambda m: f"{m.group(1)} = '{m.group(2)}'"),
            (r'(\w+)\s*=\s*(\d+)', lambda m: f"{m.group(1)} = {m.group(2)}"),
            (r'(\w+)\s*>\s*(\d+)', lambda m: f"{m.group(1)} > {m.group(2)}"),
            (r'(\w+)\s*<\s*(\d+)', lambda m: f"{m.group(1)} < {m.group(2)}"),
            (r'(\w+)\s*like\s*["\']([^"\']+)["\']', lambda m: f"{m.group(1)} LIKE '%{m.group(2)}%'"),
            (r'(\w+)\s*contains\s*["\']([^"\']+)["\']', lambda m: f"{m.group(1)} LIKE '%{m.group(2)}%'"),
        ]
        
        for pattern, formatter in patterns:
            matches = re.finditer(pattern, condition_text, re.IGNORECASE)
            for match in matches:
                column_hint = match.group(1)
                matched_columns = self.find_columns_by_hint(table_name, [column_hint])
                if matched_columns:
                    condition = formatter(match).replace(column_hint, matched_columns[0])
                    conditions.append(condition)
        
        return conditions
    
    def build_sql_query(self, parsed: Dict[str, Any]) -> str:
        """Build SQL query from parsed natural language"""
        if not parsed['table']:
            return "-- Error: No table identified in the query"
        
        # SELECT clause
        if parsed['aggregations']:
            if 'COUNT' in parsed['aggregations']:
                select_clause = "SELECT COUNT(*)"
            elif parsed['columns']:
                agg = parsed['aggregations'][0]
                col = parsed['columns'][0] if parsed['columns'] else '*'
                select_clause = f"SELECT {agg}({col})"
            else:
                select_clause = f"SELECT {parsed['aggregations'][0]}(*)"
        elif parsed['columns']:
            select_clause = f"SELECT {', '.join(parsed['columns'])}"
        else:
            select_clause = "SELECT *"
        
        # Add TOP clause if limit specified
        if parsed['limit'] and 'COUNT' not in parsed.get('aggregations', []):
            select_clause = select_clause.replace('SELECT', f'SELECT TOP {parsed["limit"]}')
        
        # FROM clause
        from_clause = f"FROM {parsed['table']}"
        
        # WHERE clause
        where_clause = ""
        if parsed['conditions']:
            where_clause = f"WHERE {' AND '.join(parsed['conditions'])}"
        
        # ORDER BY clause
        order_clause = ""
        if parsed['order_by']:
            order_items = [f"{col} {direction}" for col, direction in parsed['order_by']]
            order_clause = f"ORDER BY {', '.join(order_items)}"
        
        # Combine all clauses
        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if order_clause:
            sql_parts.append(order_clause)
        
        return '\n'.join(sql_parts)
    
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
    
    def natural_query_to_dataframe(self, natural_query: str) -> Tuple[str, pd.DataFrame, Dict[str, Any]]:
        """Complete pipeline: natural language -> SQL -> DataFrame with context"""
        print(f"🤖 Processing: '{natural_query}'")
        
        # Parse natural language with enhanced context
        parsed = self.parse_natural_language(natural_query)
        context = parsed.get('context', {})
        
        print(f"📝 Query Analysis:")
        print(f"   Type: {context.get('query_type', 'unknown')}")
        print(f"   Complexity: {context.get('complexity', 'simple')}")
        print(f"   Intent: {', '.join(context.get('query_intent', []))}")
        print(f"   Table: {parsed['table']}")
        print(f"   Columns: {parsed['columns']}")
        print(f"   Conditions: {parsed['conditions']}")
        
        # Build SQL
        sql = self.build_sql_query(parsed)
        
        # Execute and return results
        df = self.execute_query(sql)
        
        return sql, df, context
    
    def get_available_tables(self) -> List[str]:
        """Get list of available tables with data for reference"""
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
                'avg_data_quality': round(sum(self.calculate_data_quality_score(table) 
                                            for table in self.filtered_tables.keys()) / 
                                        len(self.filtered_tables), 3) if self.filtered_tables else 0,
                'largest_tables': sorted([(table, stats['row_count']) 
                                        for table, stats in self.table_stats.items()],
                                       key=lambda x: x[1], reverse=True)[:5]
            }
    
    def suggest_queries_for_table(self, table_name: str) -> List[str]:
        """Generate suggested queries for a specific table"""
        if table_name not in self.filtered_tables:
            return []
        
        table_info = self.filtered_tables[table_name]
        columns = table_info['columns']
        row_count = table_info['row_count']
        
        suggestions = []
        table_simple_name = table_name.split('.')[-1]
        
        # Basic queries
        suggestions.append(f"Show all data from {table_simple_name}")
        suggestions.append(f"Count rows in {table_simple_name}")
        suggestions.append(f"Get top 10 records from {table_simple_name}")
        
        # Column-specific queries
        if columns:
            # Find potential name/title columns
            name_columns = [col['name'] for col in columns 
                          if any(term in col['name'].lower() for term in ['name', 'title', 'description'])]
            if name_columns:
                suggestions.append(f"Show {name_columns[0]} from {table_simple_name}")
            
            # Find date columns
            date_columns = [col['name'] for col in columns 
                          if col['type'].lower() in ['datetime', 'date', 'timestamp']]
            if date_columns:
                suggestions.append(f"Show {table_simple_name} ordered by {date_columns[0]}")
            
            # Find numeric columns for aggregations
            numeric_columns = [col['name'] for col in columns 
                             if col['type'].lower() in ['int', 'bigint', 'decimal', 'numeric', 'float']]
            if numeric_columns:
                suggestions.append(f"Calculate average {numeric_columns[0]} from {table_simple_name}")
                suggestions.append(f"Find maximum {numeric_columns[0]} in {table_simple_name}")
        
        return suggestions[:6]  # Limit to 6 suggestions
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get comprehensive database summary"""
        stats = self.get_table_statistics()
        
        # Find interesting patterns
        high_quality_tables = [table for table in self.filtered_tables.keys()
                             if self.calculate_data_quality_score(table) > 0.8]
        
        # Find tables with foreign key relationships
        tables_with_fk = []
        for table in self.filtered_tables.keys():
            fk_candidates = self.identify_foreign_key_candidates(table)
            if fk_candidates:
                tables_with_fk.append((table, len(fk_candidates)))
        
        return {
            'overview': stats,
            'data_quality': {
                'high_quality_tables': high_quality_tables,
                'avg_quality_score': stats['avg_data_quality']
            },
            'relationships': {
                'tables_with_foreign_keys': tables_with_fk,
                'potential_relationships': sum(len(self.identify_foreign_key_candidates(table)) 
                                             for table in self.filtered_tables.keys())
            },
            'recommendations': {
                'start_with_tables': high_quality_tables[:3],
                'explore_relationships': [table for table, _ in sorted(tables_with_fk, 
                                                                      key=lambda x: x[1], 
                                                                      reverse=True)[:3]]
            }
        }


def demonstrate_enhanced_nl_to_sql():
    """Demonstrate the Enhanced Natural Language to SQL converter"""
    
    # Database connection parameters
    SERVER = 'VMHOST1001.hq.cleverex.com'
    DATABASE = 'JoyHS_TestDrive'
    UID = 'agent_test'
    PWD = 'qxYdGTaR71V3JyQ04oUd'
    
    converter = EnhancedNaturalLanguageToSQL(SERVER, DATABASE, UID, PWD)
    
    try:
        if converter.connect():
            print("\n" + "="*80)
            print("ENHANCED NATURAL LANGUAGE TO SQL CONVERTER - DEMONSTRATION")
            print("="*80)
            
            # Step 1: Load and filter schema
            print(f"\n🔍 STEP 1: DATABASE ANALYSIS AND FILTERING")
            print("-" * 50)
            converter.load_and_filter_schema(min_rows=1)
            
            # Step 2: Export metadata
            print(f"\n📁 STEP 2: METADATA EXPORT")
            print("-" * 30)
            metadata_file = converter.export_database_metadata()
            
            # Step 3: Database summary
            print(f"\n📊 STEP 3: DATABASE SUMMARY")
            print("-" * 30)
            summary = converter.get_database_summary()
            
            print(f"Database Overview:")
            print(f"   • Total tables with data: {summary['overview']['total_tables']}")
            print(f"   • Total rows: {summary['overview']['total_rows']:,}")
            print(f"   • Total columns: {summary['overview']['total_columns']:,}")
            print(f"   • Average data quality: {summary['overview']['avg_data_quality']:.3f}")
            
            print(f"\nData Quality Analysis:")
            print(f"   • High quality tables: {len(summary['data_quality']['high_quality_tables'])}")
            if summary['data_quality']['high_quality_tables']:
                print(f"   • Best tables: {', '.join(summary['data_quality']['high_quality_tables'][:3])}")
            
            print(f"\nRelationship Analysis:")
            print(f"   • Tables with foreign keys: {len(summary['relationships']['tables_with_foreign_keys'])}")
            print(f"   • Potential relationships: {summary['relationships']['potential_relationships']}")
            
            print(f"\nRecommendations:")
            if summary['recommendations']['start_with_tables']:
                print(f"   • Start exploring: {', '.join(summary['recommendations']['start_with_tables'])}")
            
            # Step 4: Enhanced query examples
            print(f"\n🤖 STEP 4: ENHANCED NATURAL LANGUAGE QUERIES")
            print("-" * 50)
            
            available_tables = converter.get_available_tables()
            if available_tables:
                # Use the first few high-quality tables for examples
                demo_tables = summary['data_quality']['high_quality_tables'][:2]
                if not demo_tables:
                    demo_tables = available_tables[:2]
                
                example_queries = []
                for table in demo_tables:
                    table_name = table.split('.')[-1]
                    suggestions = converter.suggest_queries_for_table(table)
                    example_queries.extend(suggestions[:2])  # 2 per table
                
                # Add some generic advanced queries
                example_queries.extend([
                    "Find tables with the most data",
                    "Show me data quality statistics",
                    "List all tables with their row counts"
                ])
                
                for i, query in enumerate(example_queries[:6], 1):
                    print(f"\n{i}️⃣ Example Query: '{query}'")
                    print("-" * 40)
                    
                    try:
                        sql, df, context = converter.natural_query_to_dataframe(query)
                        
                        print(f"📄 Generated SQL:\n{sql}")
                        
                        if not df.empty:
                            print(f"\n📊 Results ({len(df)} rows, {len(df.columns)} columns):")
                            pd.set_option('display.max_columns', 8)
                            pd.set_option('display.width', 1000)
                            pd.set_option('display.max_colwidth', 30)
                            
                            print(df.head(3).to_string(index=False))
                            if len(df) > 3:
                                print(f"... showing first 3 of {len(df)} rows")
                        else:
                            print("ℹ️ No results returned")
                            
                    except Exception as e:
                        print(f"❌ Error: {e}")
                    
                    print()
            
            # Step 5: Show metadata file info
            if metadata_file:
                print(f"\n📋 STEP 5: EXPORTED METADATA ANALYSIS")
                print("-" * 40)
                print(f"✅ Metadata exported to: {metadata_file}")
                print(f"📁 File size: {os.path.getsize(metadata_file):,} bytes")
                
                # Show what's in the metadata
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                print(f"\n📊 Metadata Contents:")
                print(f"   • Database: {metadata['database_info']['database']}")
                print(f"   • Export time: {metadata['database_info']['export_timestamp']}")
                print(f"   • Tables included: {len(metadata['tables'])}")
                
                # Show sample table analysis
                if metadata['tables']:
                    sample_table = list(metadata['tables'].values())[0]
                    print(f"\n🔍 Sample Table Analysis:")
                    print(f"   • Table: {sample_table['table_name']}")
                    print(f"   • Rows: {sample_table['row_count']:,}")
                    print(f"   • Columns: {sample_table['column_count']}")
                    if sample_table['statistics']['primary_key_candidates']:
                        print(f"   • Primary key candidates: {', '.join(sample_table['statistics']['primary_key_candidates'])}")
                    if sample_table['statistics']['foreign_key_candidates']:
                        print(f"   • Foreign key candidates: {len(sample_table['statistics']['foreign_key_candidates'])}")
            
            print(f"\n💡 NEXT STEPS:")
            print("=" * 40)
            print("1. Use the exported JSON metadata for offline analysis")
            print("2. Run the interactive mode to test your own queries")
            print("3. Explore the high-quality tables identified in the analysis")
            print("4. Investigate the foreign key relationships for data modeling")
            
    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()
    finally:
        converter.disconnect()


def interactive_enhanced_nl_to_sql():
    """Interactive mode for enhanced natural language to SQL conversion"""
    
    # Database connection parameters
    SERVER = 'VMHOST1001.hq.cleverex.com'
    DATABASE = 'JoyHS_TestDrive'
    UID = 'agent_test'
    PWD = 'qxYdGTaR71V3JyQ04oUd'
    
    converter = EnhancedNaturalLanguageToSQL(SERVER, DATABASE, UID, PWD)
    
    try:
        if converter.connect():
            print("\n" + "="*70)
            print("🤖 ENHANCED INTERACTIVE NATURAL LANGUAGE TO SQL")
            print("="*70)
            print("Enhanced features: Smart filtering, metadata export, query analysis")
            print("Commands: 'tables', 'summary', 'export', 'help', 'suggest [table]', 'quit'")
            print("-" * 70)
            
            # Load schema with filtering
            print("🔍 Loading and analyzing database schema...")
            converter.load_and_filter_schema(min_rows=1)
            
            # Get summary for context
            summary = converter.get_database_summary()
            print(f"✅ Ready! Found {summary['overview']['total_tables']} tables with data")
            
            while True:
                query = input("\n💬 Your query: ").strip()
                
                if query.lower() in ['quit', 'exit', 'q']:
                    print("👋 Goodbye!")
                    break
                
                elif query.lower() == 'tables':
                    tables = converter.get_available_tables()
                    print(f"\n📋 Available tables with data ({len(tables)}):")
                    for i, table in enumerate(tables, 1):
                        stats = converter.get_table_statistics(table)
                        row_count = stats.get('row_count', 0)
                        quality = converter.calculate_data_quality_score(table)
                        print(f"   {i:2d}. {table} ({row_count:,} rows, quality: {quality:.2f})")
                
                elif query.lower() == 'summary':
                    summary = converter.get_database_summary()
                    print(f"\n📊 Database Summary:")
                    print(f"   • Tables: {summary['overview']['total_tables']}")
                    print(f"   • Total rows: {summary['overview']['total_rows']:,}")
                    print(f"   • Average quality: {summary['overview']['avg_data_quality']:.3f}")
                    print(f"   • High quality tables: {len(summary['data_quality']['high_quality_tables'])}")
                    
                    if summary['overview']['largest_tables']:
                        print(f"\n📈 Largest tables:")
                        for table, rows in summary['overview']['largest_tables']:
                            print(f"      {table}: {rows:,} rows")
                
                elif query.lower() == 'export':
                    filename = converter.export_database_metadata()
                    if filename:
                        print(f"✅ Database metadata exported to: {filename}")
                    else:
                        print("❌ Failed to export metadata")
                
                elif query.lower().startswith('suggest '):
                    table_hint = query[8:].strip()
                    table_name = converter.find_table_by_name(table_hint)
                    if table_name:
                        suggestions = converter.suggest_queries_for_table(table_name)
                        print(f"\n💡 Query suggestions for {table_name}:")
                        for i, suggestion in enumerate(suggestions, 1):
                            print(f"   {i}. {suggestion}")
                    else:
                        print(f"❌ Table '{table_hint}' not found")
                
                elif query.lower() == 'help':
                    print(f"\n💡 Commands and examples:")
                    print("   📋 'tables' - Show all available tables")
                    print("   📊 'summary' - Show database summary")
                    print("   📁 'export' - Export metadata to JSON")
                    print("   💡 'suggest [table]' - Get query suggestions for table")
                    print("\n🗣️ Natural language examples:")
                    print("   • 'Show me all data from customers'")
                    print("   • 'Count rows in orders table'")
                    print("   • 'Find users where status = active'")
                    print("   • 'Get top 10 products by price'")
                    print("   • 'Calculate average salary from employees'")
                
                elif query:
                    try:
                        sql, df, context = converter.natural_query_to_dataframe(query)
                        
                        print(f"\n📄 Generated SQL:\n{sql}")
                        
                        # Show context analysis
                        print(f"\n🔍 Query Analysis:")
                        print(f"   Type: {context.get('query_type', 'unknown')}")
                        print(f"   Complexity: {context.get('complexity', 'simple')}")
                        if context.get('suggested_tables'):
                            print(f"   Suggested tables: {', '.join(context['suggested_tables'][:3])}")
                        
                        if not df.empty:
                            print(f"\n📊 Results ({len(df)} rows, {len(df.columns)} columns):")
                            pd.set_option('display.max_columns', 10)
                            pd.set_option('display.width', 1000)
                            pd.set_option('display.max_colwidth', 40)
                            
                            print(df.head(10).to_string(index=False))
                            
                            if len(df) > 10:
                                print(f"\n... showing first 10 of {len(df)} rows")
                                
                            # Show basic statistics for numeric columns
                            numeric_cols = df.select_dtypes(include=['number']).columns
                            if len(numeric_cols) > 0:
                                print(f"\n📈 Numeric Summary:")
                                for col in numeric_cols[:3]:  # Show first 3 numeric columns
                                    print(f"   {col}: min={df[col].min()}, max={df[col].max()}, avg={df[col].mean():.2f}")
                        else:
                            print("ℹ️ No results returned")
                            
                    except Exception as e:
                        print(f"❌ Error: {e}")
                
    except Exception as e:
        print(f"❌ Error during interactive session: {e}")
        import traceback
        traceback.print_exc()
    finally:
        converter.disconnect()


def analyze_from_metadata(metadata_file: str):
    """Analyze database using exported metadata JSON file"""
    try:
        converter = EnhancedNaturalLanguageToSQL("", "", "", "")
        
        if converter.load_metadata_from_json(metadata_file):
            print(f"\n📊 OFFLINE METADATA ANALYSIS")
            print("=" * 50)
            
            summary = converter.get_database_summary()
            
            print(f"Database Analysis from {metadata_file}:")
            print(f"   • Total tables: {summary['overview']['total_tables']}")
            print(f"   • Total rows: {summary['overview']['total_rows']:,}")
            print(f"   • Average data quality: {summary['overview']['avg_data_quality']:.3f}")
            
            # Show table recommendations
            if summary['recommendations']['start_with_tables']:
                print(f"\n🎯 Recommended tables to explore:")
                for table in summary['recommendations']['start_with_tables']:
                    stats = converter.get_table_statistics(table)
                    print(f"   • {table}: {stats.get('row_count', 0):,} rows")
                    
                    # Show suggested queries
                    suggestions = converter.suggest_queries_for_table(table)
                    if suggestions:
                        print(f"     Suggested queries: {suggestions[0]}")
            
            return True
        else:
            return False
            
    except Exception as e:
        print(f"❌ Error analyzing metadata: {e}")
        return False


if __name__ == "__main__":
    print("Enhanced Natural Language to SQL Converter")
    print("=" * 50)
    print("Choose mode:")
    print("1. Full demonstration (connect to DB, analyze, export)")
    print("2. Interactive mode (connect to DB)")
    print("3. Offline analysis (analyze from exported JSON)")
    
    choice = input("\nEnter your choice (1, 2, or 3): ").strip()
    
    if choice == "1":
        demonstrate_enhanced_nl_to_sql()
    elif choice == "2":
        interactive_enhanced_nl_to_sql()
    elif choice == "3":
        metadata_file = input("Enter metadata JSON file path: ").strip()
        if os.path.exists(metadata_file):
            analyze_from_metadata(metadata_file)
        else:
            print(f"❌ File not found: {metadata_file}")
    else:
        print("Invalid choice. Running demonstration mode...")
        demonstrate_enhanced_nl_to_sql()