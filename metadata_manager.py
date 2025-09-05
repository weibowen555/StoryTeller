"""
Metadata Manager Module
Handles metadata export/import, data quality scoring, and relationship analysis
"""

import json
import os
from typing import Dict, List, Any
from datetime import datetime


class MetadataManager:
    """Handles metadata operations, quality scoring, and relationship analysis"""
    
    def __init__(self, database_analyzer=None):
        self.analyzer = database_analyzer
        self.database_metadata = {}
    
    def calculate_data_quality_score(self, table_key: str) -> float:
        """Calculate a data quality score for the table"""
        if not self.analyzer or table_key not in self.analyzer.table_stats:
            return 0.0
        
        stats = self.analyzer.table_stats[table_key]
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
        if not self.analyzer or table_key not in self.analyzer.table_stats:
            return []
        
        column_analysis = self.analyzer.table_stats[table_key].get('column_analysis', {})
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
        if not self.analyzer or table_key not in self.analyzer.table_stats:
            return []
        
        column_analysis = self.analyzer.table_stats[table_key].get('column_analysis', {})
        candidates = []
        
        for col_name, col_stats in column_analysis.items():
            col_type = col_stats.get('type', '').lower()
            
            # Look for columns that might reference other tables
            if (col_type in ['int', 'bigint'] and 
                col_name.lower().endswith('id') and 
                not col_name.lower() in ['id', 'rowid']):
                
                # Try to find matching tables
                potential_table = col_name.lower().replace('_id', '').replace('id', '')
                matching_tables = [t for t in self.analyzer.filtered_tables.keys() 
                                 if potential_table in t.lower().split('.')[-1]]
                
                if matching_tables:
                    candidates.append({
                        'column': col_name,
                        'potential_references': matching_tables[:3]  # Top 3 matches
                    })
        
        return candidates
    
    def export_database_metadata(self, filename: str = None) -> str:
        """Export all database metadata to JSON file"""
        if not self.analyzer:
            print("❌ No database analyzer available")
            return None
            
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"database_metadata_{self.analyzer.database}_{timestamp}.json"
        
        # Prepare comprehensive metadata
        metadata = {
            'database_info': {
                'server': self.analyzer.server,
                'database': self.analyzer.database,
                'export_timestamp': datetime.now().isoformat(),
                'total_tables': len(self.analyzer.filtered_tables),
                'total_rows': sum(stats['row_count'] for stats in self.analyzer.table_stats.values())
            },
            'tables': {}
        }
        
        # Add detailed table information
        for table_key, table_info in self.analyzer.filtered_tables.items():
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
                'sample_data': self.analyzer.table_stats.get(table_key, {}).get('sample_data', []),
                'column_analysis': self.analyzer.table_stats.get(table_key, {}).get('column_analysis', {}),
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
    
    def load_metadata_from_json(self, filename: str) -> bool:
        """Load database metadata from JSON file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            self.database_metadata = metadata
            
            # If we have an analyzer, reconstruct internal structures
            if self.analyzer:
                for table_key, table_info in metadata['tables'].items():
                    self.analyzer.filtered_tables[table_key] = {
                        'row_count': table_info['row_count'],
                        'column_count': table_info['column_count'],
                        'columns': table_info['columns']
                    }
                    
                    self.analyzer.table_columns[table_key] = table_info['columns']
                    table_name = table_info['table_name'].lower()
                    self.analyzer.schema_info[table_name] = table_key
            
            print(f"✅ Loaded metadata for {len(metadata['tables'])} tables from {filename}")
            return True
            
        except Exception as e:
            print(f"❌ Error loading metadata from {filename}: {e}")
            return False
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get comprehensive database summary"""
        if not self.analyzer:
            return {}
            
        stats = self.analyzer.get_table_statistics()
        
        # Calculate average data quality
        if self.analyzer.filtered_tables:
            avg_quality = sum(self.calculate_data_quality_score(table) 
                            for table in self.analyzer.filtered_tables.keys()) / len(self.analyzer.filtered_tables)
        else:
            avg_quality = 0
        
        # Find interesting patterns
        high_quality_tables = [table for table in self.analyzer.filtered_tables.keys()
                             if self.calculate_data_quality_score(table) > 0.8]
        
        # Find tables with foreign key relationships
        tables_with_fk = []
        for table in self.analyzer.filtered_tables.keys():
            fk_candidates = self.identify_foreign_key_candidates(table)
            if fk_candidates:
                tables_with_fk.append((table, len(fk_candidates)))
        
        return {
            'overview': {
                **stats,
                'avg_data_quality': round(avg_quality, 3)
            },
            'data_quality': {
                'high_quality_tables': high_quality_tables,
                'avg_quality_score': round(avg_quality, 3)
            },
            'relationships': {
                'tables_with_foreign_keys': tables_with_fk,
                'potential_relationships': sum(len(self.identify_foreign_key_candidates(table)) 
                                             for table in self.analyzer.filtered_tables.keys())
            },
            'recommendations': {
                'start_with_tables': high_quality_tables[:3],
                'explore_relationships': [table for table, _ in sorted(tables_with_fk, 
                                                                      key=lambda x: x[1], 
                                                                      reverse=True)[:3]]
            }
        }
    
    def suggest_queries_for_table(self, table_name: str) -> List[str]:
        """Generate suggested queries for a specific table"""
        if not self.analyzer or table_name not in self.analyzer.filtered_tables:
            return []
        
        table_info = self.analyzer.filtered_tables[table_name]
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
    
    def analyze_from_metadata_only(self, metadata_file: str) -> Dict[str, Any]:
        """Analyze database using only exported metadata JSON file"""
        if not self.load_metadata_from_json(metadata_file):
            return {}
        
        metadata = self.database_metadata
        analysis = {
            'database_info': metadata.get('database_info', {}),
            'table_count': len(metadata.get('tables', {})),
            'high_quality_tables': [],
            'table_relationships': {},
            'recommendations': []
        }
        
        # Analyze tables from metadata
        for table_key, table_info in metadata.get('tables', {}).items():
            stats = table_info.get('statistics', {})
            quality_score = stats.get('data_quality_score', 0)
            
            if quality_score > 0.8:
                analysis['high_quality_tables'].append({
                    'table': table_key,
                    'quality_score': quality_score,
                    'row_count': table_info.get('row_count', 0)
                })
            
            # Foreign key relationships
            fk_candidates = stats.get('foreign_key_candidates', [])
            if fk_candidates:
                analysis['table_relationships'][table_key] = fk_candidates
        
        # Generate recommendations
        if analysis['high_quality_tables']:
            analysis['recommendations'].append("Start with high-quality tables for reliable data analysis")
            
        if analysis['table_relationships']:
            analysis['recommendations'].append("Explore foreign key relationships for data modeling")
        
        return analysis