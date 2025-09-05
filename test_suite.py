#!/usr/bin/env python3
"""
Automated Test Suite for Enhanced Natural Language to SQL Converter
Run with: python test_suite.py
"""

import unittest
import tempfile
import json
import os
import sys
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config, DatabaseConfig, AnalysisConfig
from database_analyzer import DatabaseAnalyzer
from metadata_manager import MetadataManager
from query_parser import QueryParser
from enhanced_nl_sql import EnhancedNaturalLanguageToSQL


class TestConfiguration(unittest.TestCase):
    """Test configuration management"""
    
    def test_database_config_defaults(self):
        """Test default database configuration"""
        config = DatabaseConfig.get_connection_params()
        
        self.assertIn('server', config)
        self.assertIn('database', config)
        self.assertIn('uid', config)
        self.assertIn('pwd', config)
        
        # Test connection string generation
        conn_str = DatabaseConfig.get_connection_string()
        self.assertIn('DRIVER=', conn_str)
        self.assertIn('SERVER=', conn_str)
        self.assertIn('DATABASE=', conn_str)
    
    def test_analysis_config(self):
        """Test analysis configuration settings"""
        settings = AnalysisConfig.get_analysis_settings()
        
        self.assertIn('min_rows', settings)
        self.assertIn('max_analysis_rows', settings)
        self.assertIn('quality_weights', settings)
        
        # Verify quality weights sum to 1.0
        weights = settings['quality_weights']
        total_weight = sum(weights.values())
        self.assertAlmostEqual(total_weight, 1.0, places=1)


class TestDatabaseAnalyzer(unittest.TestCase):
    """Test database analyzer functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.analyzer = DatabaseAnalyzer("test_server", "test_db", "test_user", "test_pwd")
    
    @patch('pyodbc.connect')
    def test_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        result = self.analyzer.connect()
        
        self.assertTrue(result)
        self.assertIsNotNone(self.analyzer.connection)
        self.assertIsNotNone(self.analyzer.cursor)
    
    @patch('pyodbc.connect')
    def test_connection_failure(self, mock_connect):
        """Test database connection failure"""
        mock_connect.side_effect = Exception("Connection failed")
        
        result = self.analyzer.connect()
        
        self.assertFalse(result)
        self.assertIsNone(self.analyzer.connection)
    
    def test_get_table_row_count_mock(self):
        """Test row count retrieval with mock data"""
        # Mock cursor and connection
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [1000]
        self.analyzer.cursor = mock_cursor
        
        count = self.analyzer.get_table_row_count("test_table")
        
        self.assertEqual(count, 1000)
        mock_cursor.execute.assert_called_once()
    
    def test_sample_data_mock(self):
        """Test sample data retrieval with mock"""
        mock_connection = Mock()
        sample_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        
        with patch('pandas.read_sql', return_value=sample_df):
            self.analyzer.connection = mock_connection
            result = self.analyzer.get_table_sample_data("test_table", 3)
            
            self.assertEqual(len(result), 3)
            self.assertIn('id', result[0])
            self.assertIn('name', result[0])


class TestMetadataManager(unittest.TestCase):
    """Test metadata management functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_analyzer = Mock()
        self.metadata_manager = MetadataManager(self.mock_analyzer)
        
        # Mock table data
        self.mock_analyzer.filtered_tables = {
            'dbo.users': {
                'row_count': 1000,
                'column_count': 5,
                'columns': [
                    {'name': 'id', 'type': 'int', 'nullable': False, 'position': 1},
                    {'name': 'name', 'type': 'varchar', 'nullable': True, 'position': 2}
                ]
            }
        }
        
        self.mock_analyzer.table_stats = {
            'dbo.users': {
                'row_count': 1000,
                'column_count': 5,
                'sample_data': [{'id': 1, 'name': 'Alice'}],
                'column_analysis': {
                    'id': {
                        'type': 'int',
                        'total_count': 1000,
                        'non_null_count': 1000,
                        'unique_count': 1000,
                        'null_percentage': 0.0,
                        'uniqueness_ratio': 1.0
                    }
                }
            }
        }
        
        self.mock_analyzer.server = "test_server"
        self.mock_analyzer.database = "test_db"
    
    def test_data_quality_score_calculation(self):
        """Test data quality score calculation"""
        score = self.metadata_manager.calculate_data_quality_score('dbo.users')
        
        # Should be high quality (low nulls, high uniqueness)
        self.assertGreater(score, 0.8)
        self.assertLessEqual(score, 1.0)
    
    def test_primary_key_identification(self):
        """Test primary key candidate identification"""
        candidates = self.metadata_manager.identify_primary_key_candidates('dbo.users')
        
        # 'id' should be identified as PK candidate
        self.assertIn('id', candidates)
    
    def test_metadata_export_import(self):
        """Test metadata export and import"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_filename = f.name
        
        try:
            # Test export
            filename = self.metadata_manager.export_database_metadata(temp_filename)
            self.assertEqual(filename, temp_filename)
            self.assertTrue(os.path.exists(temp_filename))
            
            # Verify export content
            with open(temp_filename, 'r') as f:
                metadata = json.load(f)
            
            self.assertIn('database_info', metadata)
            self.assertIn('tables', metadata)
            self.assertIn('dbo.users', metadata['tables'])
            
            # Test import
            manager2 = MetadataManager()
            result = manager2.load_metadata_from_json(temp_filename)
            self.assertTrue(result)
            self.assertIn('dbo.users', manager2.database_metadata['tables'])
            
        finally:
            # Cleanup
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)


class TestQueryParser(unittest.TestCase):
    """Test natural language query parsing"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_analyzer = Mock()
        self.query_parser = QueryParser(self.mock_analyzer)
        
        # Mock schema info
        self.mock_analyzer.schema_info = {
            'users': 'dbo.users',
            'orders': 'dbo.orders',
            'products': 'dbo.products'
        }
        
        self.mock_analyzer.filtered_tables = {
            'dbo.users': {'row_count': 1000},
            'dbo.orders': {'row_count': 5000},
            'dbo.products': {'row_count': 200}
        }
        
        self.mock_analyzer.table_columns = {
            'dbo.users': [
                {'name': 'id', 'type': 'int'},
                {'name': 'name', 'type': 'varchar'},
                {'name': 'email', 'type': 'varchar'},
                {'name': 'created_at', 'type': 'datetime'}
            ]
        }
    
    def test_query_context_analysis(self):
        """Test query context analysis"""
        # Test aggregation query
        context = self.query_parser.analyze_user_query_context("count all users")
        self.assertEqual(context['query_type'], 'aggregation')
        self.assertIn('count_records', context['query_intent'])
        
        # Test retrieval query
        context = self.query_parser.analyze_user_query_context("show me all users")
        self.assertEqual(context['query_type'], 'retrieval')
        self.assertIn('retrieve_data', context['query_intent'])
        
        # Test filtered query
        context = self.query_parser.analyze_user_query_context("find users where name = john")
        self.assertEqual(context['query_type'], 'filtered_retrieval')
        self.assertIn('search_data', context['query_intent'])
    
    def test_table_name_finding(self):
        """Test table name finding from hints"""
        # Direct match
        result = self.query_parser.find_table_by_name("users")
        self.assertEqual(result, "dbo.users")
        
        # Partial match
        result = self.query_parser.find_table_by_name("user")
        self.assertEqual(result, "dbo.users")
        
        # No match
        result = self.query_parser.find_table_by_name("nonexistent")
        self.assertIsNone(result)
    
    def test_column_finding(self):
        """Test column finding from hints"""
        columns = self.query_parser.find_columns_by_hint("dbo.users", ["name", "email"])
        
        self.assertIn("name", columns)
        self.assertIn("email", columns)
        self.assertEqual(len(columns), 2)
    
    def test_natural_language_parsing(self):
        """Test complete natural language parsing"""
        # Simple query
        parsed = self.query_parser.parse_natural_language("show all users")
        self.assertEqual(parsed['table'], 'dbo.users')
        self.assertEqual(parsed['action'], 'SELECT')
        
        # Count query
        parsed = self.query_parser.parse_natural_language("count all users")
        self.assertEqual(parsed['table'], 'dbo.users')
        self.assertIn('COUNT', parsed['aggregations'])
        
        # Limited query
        parsed = self.query_parser.parse_natural_language("show top 5 users")
        self.assertEqual(parsed['table'], 'dbo.users')
        self.assertEqual(parsed['limit'], 5)
    
    def test_sql_generation(self):
        """Test SQL query generation"""
        # Simple SELECT
        parsed = {
            'table': 'dbo.users',
            'columns': [],
            'conditions': [],
            'aggregations': [],
            'limit': None,
            'order_by': []
        }
        sql = self.query_parser.build_sql_query(parsed)
        self.assertIn('SELECT *', sql)
        self.assertIn('FROM dbo.users', sql)
        
        # COUNT query
        parsed['aggregations'] = ['COUNT']
        sql = self.query_parser.build_sql_query(parsed)
        self.assertIn('SELECT COUNT(*)', sql)
        
        # Limited query
        parsed['aggregations'] = []
        parsed['limit'] = 10
        sql = self.query_parser.build_sql_query(parsed)
        self.assertIn('SELECT TOP 10', sql)


class TestEnhancedNLSQL(unittest.TestCase):
    """Test the main enhanced NL-SQL class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.converter = EnhancedNaturalLanguageToSQL(
            "test_server", "test_db", "test_user", "test_pwd"
        )
    
    @patch.object(DatabaseAnalyzer, 'connect')
    def test_connection_delegation(self, mock_connect):
        """Test that connection is properly delegated"""
        mock_connect.return_value = True
        
        result = self.converter.connect()
        
        self.assertTrue(result)
        mock_connect.assert_called_once()
    
    def test_component_integration(self):
        """Test that all components are properly integrated"""
        # Verify all components are initialized
        self.assertIsInstance(self.converter.analyzer, DatabaseAnalyzer)
        self.assertIsInstance(self.converter.metadata_manager, MetadataManager)
        self.assertIsInstance(self.converter.query_parser, QueryParser)
        
        # Verify cross-references are set up
        self.assertEqual(self.converter.metadata_manager.analyzer, self.converter.analyzer)
        self.assertEqual(self.converter.query_parser.analyzer, self.converter.analyzer)


class TestEndToEndFlow(unittest.TestCase):
    """End-to-end integration tests"""
    
    def test_full_pipeline_mock(self):
        """Test the complete pipeline with mock data"""
        converter = EnhancedNaturalLanguageToSQL("test", "test", "test", "test")
        
        # Mock the analyzer components
        converter.analyzer.filtered_tables = {
            'dbo.test_table': {
                'row_count': 100,
                'column_count': 3,
                'columns': [
                    {'name': 'id', 'type': 'int'},
                    {'name': 'name', 'type': 'varchar'},
                    {'name': 'value', 'type': 'decimal'}
                ]
            }
        }
        
        converter.analyzer.schema_info = {'test_table': 'dbo.test_table'}
        converter.analyzer.table_columns = converter.analyzer.filtered_tables
        
        # Mock execute_query to return a DataFrame
        mock_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'value': [10.5, 20.0, 30.5]
        })
        
        with patch.object(converter.analyzer, 'execute_query', return_value=mock_df):
            sql, df, context = converter.natural_query_to_dataframe("show all test_table")
            
            # Verify results
            self.assertIn('SELECT', sql)
            self.assertIn('dbo.test_table', sql)
            self.assertEqual(len(df), 3)
            self.assertIn('query_type', context)


def run_comprehensive_tests():
    """Run comprehensive test suite with detailed reporting"""
    print("🧪 Starting Enhanced NL-SQL Test Suite")
    print("=" * 60)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestConfiguration,
        TestDatabaseAnalyzer,
        TestMetadataManager,
        TestQueryParser,
        TestEnhancedNLSQL,
        TestEndToEndFlow
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 60)
    print("📊 Test Summary")
    print("-" * 30)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\n❌ Failures:")
        for test, failure in result.failures:
            print(f"  - {test}: {failure}")
    
    if result.errors:
        print("\n💥 Errors:")
        for test, error in result.errors:
            print(f"  - {test}: {error}")
    
    if result.wasSuccessful():
        print("\n✅ All tests passed successfully!")
        return True
    else:
        print("\n❌ Some tests failed.")
        return False


if __name__ == "__main__":
    success = run_comprehensive_tests()
    sys.exit(0 if success else 1)