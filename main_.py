#!/usr/bin/env python3
"""
Enhanced Natural Language to SQL Converter
Main application entry point

This is the main entry point that brings together all components of the
Enhanced Natural Language to SQL converter system.

File Structure:
├── main.py                 # Main entry point (this file)
├── config.py               # Configuration settings
├── database_analyzer.py    # Database connection and analysis
├── metadata_manager.py     # Metadata export/import and quality analysis
├── query_parser.py         # Natural language query parsing
├── enhanced_nl_sql.py      # Main unified interface
└── demo_and_interactive.py # Demo and interactive modes

Usage:
    python main.py              # Interactive mode selection
    python main.py --demo       # Run demonstration
    python main.py --interactive # Run interactive mode
    python main.py --analyze metadata.json # Offline analysis
"""

import argparse
import sys
import os
from pathlib import Path

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from config import Config, DatabaseConfig, LoggingConfig
from demo_and_interactive import (
    demonstrate_enhanced_nl_to_sql,
    interactive_enhanced_nl_to_sql,
    analyze_from_metadata
)


def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                Enhanced Natural Language to SQL Converter                    ║
║                           Intelligent Database Query System                  ║
╚══════════════════════════════════════════════════════════════════════════════╝

Features:
• 🔍 Smart database schema filtering (skips empty tables)
• 📊 Comprehensive data quality analysis and scoring
• 📁 Complete metadata export to JSON for offline analysis
• 🤖 Intelligent natural language query parsing
• 💡 Smart query suggestions and context analysis
• 🔗 Foreign key relationship detection
• 📈 Real-time statistics and data insights

Modes:
• 🎭 Demonstration: Full system showcase with analysis
• 💬 Interactive: Real-time natural language queries
• 📋 Offline: Analyze exported metadata without DB connection
"""
    print(banner)


def print_file_structure():
    """Print project file structure"""
    structure = """
📁 Project Structure:
├── 🚀 main.py                 # Main entry point
├── ⚙️  config.py               # Configuration settings
├── 🔍 database_analyzer.py    # Database connection and analysis
├── 📊 metadata_manager.py     # Metadata export/import and quality analysis
├── 🤖 query_parser.py         # Natural language query parsing
├── 🎯 enhanced_nl_sql.py      # Main unified interface
└── 🎭 demo_and_interactive.py # Demo and interactive modes
"""
    print(structure)


def print_usage_examples():
    """Print usage examples"""
    examples = """
💡 Usage Examples:

1. 🎭 Full Demonstration:
   python main.py --demo
   
2. 💬 Interactive Mode:
   python main.py --interactive
   
3. 📋 Offline Analysis:
   python main.py --analyze metadata.json
   
4. 🔧 Custom Database:
   python main.py --server "your-server" --database "your-db"
   
5. 📈 Environment Variables:
   export DB_SERVER="your-server"
   export DB_DATABASE="your-database"
   python main.py

🗣️ Natural Language Query Examples:
• "Show me all data from customers table"
• "Count rows in orders"
• "Find users where status = active"
• "Get top 10 products by price"
• "Calculate average salary from employees"
• "Show tables with more than 1000 rows"
"""
    print(examples)


def handle_command_line_args():
    """Handle command line arguments"""
    parser = argparse.ArgumentParser(
        description="Enhanced Natural Language to SQL Converter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --demo                    # Run full demonstration
  python main.py --interactive             # Interactive mode
  python main.py --analyze metadata.json   # Offline analysis
  python main.py --help-all               # Show detailed help
        """
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--demo', action='store_true',
                           help='Run full demonstration mode')
    mode_group.add_argument('--interactive', action='store_true',
                           help='Run interactive mode')
    mode_group.add_argument('--analyze', metavar='FILE',
                           help='Analyze from metadata JSON file')
    
    # Database connection options
    db_group = parser.add_argument_group('database options')
    db_group.add_argument('--server', help='Database server')
    db_group.add_argument('--database', help='Database name')
    db_group.add_argument('--uid', help='User ID')
    db_group.add_argument('--pwd', help='Password')
    
    # Additional options
    parser.add_argument('--config', action='store_true',
                       help='Show current configuration')
    parser.add_argument('--structure', action='store_true',
                       help='Show project file structure')
    parser.add_argument('--examples', action='store_true',
                       help='Show usage examples')
    parser.add_argument('--help-all', action='store_true',
                       help='Show comprehensive help')
    parser.add_argument('--version', action='version', version='Enhanced NL-SQL v2.0')
    
    return parser.parse_args()


def show_configuration():
    """Show current configuration"""
    print("\n⚙️ Current Configuration:")
    print("=" * 50)
    
    config = Config.get_all_settings()
    
    print("📊 Database Settings:")
    db_config = config['database']
    print(f"   Server: {db_config['server']}")
    print(f"   Database: {db_config['database']}")
    print(f"   User: {db_config['uid']}")
    print(f"   Password: {'*' * len(db_config['pwd'])}")
    
    print("\n🔍 Analysis Settings:")
    analysis_config = config['analysis']
    print(f"   Min rows for analysis: {analysis_config['min_rows']}")
    print(f"   Max rows for full analysis: {analysis_config['max_analysis_rows']}")
    print(f"   Sample size: {analysis_config['sample_size']}")
    
    print("\n🌍 Environment:")
    env_config = config['environment']
    print(f"   Development mode: {env_config['is_development']}")
    print(f"   Production mode: {env_config['is_production']}")


def interactive_mode_selection():
    """Interactive mode selection when no arguments provided"""
    print_banner()
    
    print("Choose operating mode:")
    print("1. 🎭 Full Demonstration (recommended for first-time users)")
    print("2. 💬 Interactive Query Mode")
    print("3. 📋 Offline Analysis (requires exported metadata JSON)")
    print("4. ⚙️  Show Configuration")
    print("5. 📁 Show Project Structure")
    print("6. 💡 Show Usage Examples")
    print("7. ❌ Exit")
    
    while True:
        try:
            choice = input("\nEnter your choice (1-7): ").strip()
            
            if choice == "1":
                print("\n🎭 Starting Full Demonstration...")
                demonstrate_enhanced_nl_to_sql()
                break
            elif choice == "2":
                print("\n💬 Starting Interactive Mode...")
                interactive_enhanced_nl_to_sql()
                break
            elif choice == "3":
                metadata_file = input("📁 Enter metadata JSON file path: ").strip()
                if os.path.exists(metadata_file):
                    print(f"\n📋 Analyzing metadata from {metadata_file}...")
                    analyze_from_metadata(metadata_file)
                else:
                    print(f"❌ File not found: {metadata_file}")
                break
            elif choice == "4":
                show_configuration()
            elif choice == "5":
                print_file_structure()
            elif choice == "6":
                print_usage_examples()
            elif choice == "7":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please enter 1-7.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except EOFError:
            print("\n\n👋 Goodbye!")
            break


def main():
    """Main application entry point"""
    args = handle_command_line_args()
    
    # Handle help and info options
    if args.help_all:
        print_banner()
        print_file_structure()
        print_usage_examples()
        show_configuration()
        return
    
    if args.structure:
        print_file_structure()
        return
    
    if args.examples:
        print_usage_examples()
        return
    
    if args.config:
        show_configuration()
        return
    
    # Update database configuration if provided
    if any([args.server, args.database, args.uid, args.pwd]):
        print("🔧 Using custom database configuration...")
        # Note: In a real implementation, you'd update the config here
        # For now, we'll just show what would be updated
        if args.server:
            print(f"   Server: {args.server}")
        if args.database:
            print(f"   Database: {args.database}")
        if args.uid:
            print(f"   User: {args.uid}")
    
    # Handle mode selection
    try:
        if args.demo:
            print_banner()
            print("🎭 Starting Full Demonstration Mode...")
            demonstrate_enhanced_nl_to_sql()
        
        elif args.interactive:
            print_banner()
            print("💬 Starting Interactive Mode...")
            interactive_enhanced_nl_to_sql()
        
        elif args.analyze:
            if os.path.exists(args.analyze):
                print_banner()
                print(f"📋 Starting Offline Analysis of {args.analyze}...")
                analyze_from_metadata(args.analyze)
            else:
                print(f"❌ Error: File not found: {args.analyze}")
                sys.exit(1)
        
        else:
            # No specific mode selected, show interactive menu
            interactive_mode_selection()
    
    except KeyboardInterrupt:
        print("\n\n👋 Application interrupted by user. Goodbye!")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        if Config.environment.is_development():
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()