"""
Enhanced Natural Language to SQL Converter - Main Module
Combines all components into a unified interface
"""

import pandas as pd
from typing import Dict, List, Any, Tuple
from database_analyzer import DatabaseAnalyzer
from metadata_manager import MetadataManager
from query_parser import QueryParser


class EnhancedNaturalLanguageToSQL:
    """Main class that combines all components for natural language to SQL conversion"""
    
    def __init__(self, server: str, database: str, uid: str, pwd: str):
        self.analyzer = DatabaseAnalyzer(server, database, uid, pwd)
        self.metadata_manager = MetadataManager(self.analyzer)
        self.query_parser = QueryParser(self.analyzer)
    
    def connect(self) -> bool:
        """Establish connection to SQL Server"""
        return self.analyzer.connect()
    
    def disconnect(self):
        """Close database connection"""
        self.analyzer.disconnect()
    
    def load_and_filter_schema(self, min_rows: int = 1):
        """Load database schema and filter out empty tables"""
        self.analyzer.load_and_filter_schema(min_rows)
    
    def export_database_metadata(self, filename: str = None) -> str:
        """Export all database metadata to JSON file"""
        return self.metadata_manager.export_database_metadata(filename)
    
    def load_metadata_from_json(self, filename: str) -> bool:
        """Load database metadata from JSON file"""
        return self.metadata_manager.load_metadata_from_json(filename)
    
    def natural_query_to_dataframe(self, natural_query: str) -> Tuple[str, pd.DataFrame, Dict[str, Any]]:
        """Complete pipeline: natural language -> SQL -> DataFrame with context"""
        # Parse natural language to SQL
        sql, context = self.query_parser.natural_query_to_sql(natural_query)
        
        # Execute query and return results
        df = self.analyzer.execute_query(sql)
        
        return sql, df, context
    
    def get_available_tables(self) -> List[str]:
        """Get list of available tables with data"""
        return self.analyzer.get_available_tables()
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a specific table"""
        return self.analyzer.get_table_info(table_name)
    
    def get_table_statistics(self, table_name: str = None) -> Dict[str, Any]:
        """Get comprehensive statistics for tables"""
        return self.analyzer.get_table_statistics(table_name)
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get comprehensive database summary"""
        return self.metadata_manager.get_database_summary()
    
    def suggest_queries_for_table(self, table_name: str) -> List[str]:
        """Generate suggested queries for a specific table"""
        return self.metadata_manager.suggest_queries_for_table(table_name)
    
    def calculate_data_quality_score(self, table_name: str) -> float:
        """Calculate data quality score for a table"""
        return self.metadata_manager.calculate_data_quality_score(table_name)
    
    def identify_primary_key_candidates(self, table_name: str) -> List[str]:
        """Identify potential primary key columns"""
        return self.metadata_manager.identify_primary_key_candidates(table_name)
    
    def identify_foreign_key_candidates(self, table_name: str) -> List[Dict]:
        """Identify potential foreign key relationships"""
        return self.metadata_manager.identify_foreign_key_candidates(table_name)
    
    def analyze_from_metadata_only(self, metadata_file: str) -> Dict[str, Any]:
        """Analyze database using only exported metadata JSON file"""
        return self.metadata_manager.analyze_from_metadata_only(metadata_file)


# Utility functions for backwards compatibility
def find_table_by_name(analyzer, table_hint: str):
    """Find table name from natural language hint"""
    return QueryParser(analyzer).find_table_by_name(table_hint)


def find_columns_by_hint(analyzer, table_name: str, column_hints: List[str]):
    """Find column names from natural language hints"""
    return QueryParser(analyzer).find_columns_by_hint(table_name, column_hints)