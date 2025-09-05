"""
Demonstration and Interactive Mode Module
Provides demo and interactive functionality for the Enhanced NL to SQL converter
"""

import json
import os
import pandas as pd
from enhanced_nl_sql import EnhancedNaturalLanguageToSQL


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
                    table_name = converter.query_parser.find_table_by_name(table_hint)
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
        
        analysis = converter.analyze_from_metadata_only(metadata_file)
        
        if analysis:
            print(f"\n📊 OFFLINE METADATA ANALYSIS")
            print("=" * 50)
            
            print(f"Database Analysis from {metadata_file}:")
            db_info = analysis.get('database_info', {})
            print(f"   • Database: {db_info.get('database', 'Unknown')}")
            print(f"   • Server: {db_info.get('server', 'Unknown')}")
            print(f"   • Export time: {db_info.get('export_timestamp', 'Unknown')}")
            print(f"   • Total tables: {analysis.get('table_count', 0)}")
            
            # Show high quality tables
            high_quality = analysis.get('high_quality_tables', [])
            if high_quality:
                print(f"\n🎯 High Quality Tables ({len(high_quality)}):")
                for table_info in high_quality[:5]:  # Show top 5
                    table_name = table_info['table']
                    quality = table_info['quality_score']
                    rows = table_info['row_count']
                    print(f"   • {table_name}: Quality {quality:.3f}, {rows:,} rows")
            
            # Show relationships
            relationships = analysis.get('table_relationships', {})
            if relationships:
                print(f"\n🔗 Table Relationships:")
                for table, fk_info in list(relationships.items())[:3]:  # Show first 3
                    print(f"   • {table}: {len(fk_info)} foreign key candidates")
            
            # Show recommendations
            recommendations = analysis.get('recommendations', [])
            if recommendations:
                print(f"\n💡 Recommendations:")
                for rec in recommendations:
                    print(f"   • {rec}")
            
            return True
        else:
            print(f"❌ Failed to analyze metadata from {metadata_file}")
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