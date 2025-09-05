# Enhanced Natural Language to SQL Converter

A sophisticated Python system that converts natural language queries into SQL statements with intelligent database analysis, filtering, and metadata management.

## 🌟 Key Features

### 🔍 **Smart Database Analysis**
- **Automatic empty table filtering** - Skips tables with no data
- **Comprehensive schema analysis** - Detailed column statistics and data types
- **Data quality scoring** - Automated quality assessment for each table
- **Relationship detection** - Identifies potential primary and foreign keys

### 🤖 **Intelligent Query Processing**
- **Context-aware parsing** - Understands query intent and complexity
- **Smart table/column matching** - Fuzzy matching for natural language hints
- **Query suggestions** - Generates relevant queries for each table
- **Multi-complexity support** - From simple retrievals to complex aggregations

### 📊 **Comprehensive Metadata Management**
- **JSON export/import** - Complete database metadata preservation
- **Offline analysis** - Analyze databases without active connections
- **Quality metrics** - Data completeness, uniqueness, and validity scores
- **Relationship mapping** - Foreign key candidate identification

### 💬 **User-Friendly Interfaces**
- **Interactive mode** - Real-time natural language querying
- **Demonstration mode** - Comprehensive system showcase
- **Offline mode** - Analysis from exported metadata
- **Command-line interface** - Flexible execution options

## 📁 Project Structure

```
enhanced-nl-sql/
├── 🚀 main.py                 # Main entry point
├── ⚙️  config.py               # Configuration settings
├── 🔍 database_analyzer.py    # Database connection and analysis
├── 📊 metadata_manager.py     # Metadata export/import and quality analysis
├── 🤖 query_parser.py         # Natural language query parsing
├── 🎯 enhanced_nl_sql.py      # Main unified interface
├── 🎭 demo_and_interactive.py # Demo and interactive modes
└── 📚 README.md               # This documentation
```

## 🚀 Quick Start

### Prerequisites

```bash
pip install pyodbc pandas json pathlib argparse
```

### Basic Usage

```bash
# Interactive mode selection
python main.py

# Direct demonstration
python main.py --demo

# Interactive querying
python main.py --interactive

# Offline analysis
python main.py --analyze metadata.json
```

### Environment Configuration

```bash
# Set database connection via environment variables
export DB_SERVER="your-server"
export DB_DATABASE="your-database"
export DB_UID="your-username"
export DB_PWD="your-password"
export ENVIRONMENT="development"  # or "production"

python main.py
```

## 💡 Usage Examples

### Natural Language Queries

```python
# Basic data retrieval
"Show me all data from customers table"
"Get top 10 orders"
"List employee names and salaries"

# Filtered queries
"Find users where status = active"
"Show products with price > 100"
"Get customers from New York"

# Aggregations
"Count rows in orders table"
"Calculate average salary from employees"
"Find maximum price in products"

# Complex queries
"Show customers ordered by registration date"
"Get top 5 products by sales"
"Find tables with more than 1000 rows"
```

### Programmatic Usage

```python
from enhanced_nl_sql import EnhancedNaturalLanguageToSQL

# Initialize converter
converter = EnhancedNaturalLanguageToSQL(
    server="your-server",
    database="your-database", 
    uid="username",
    pwd="password"
)

# Connect and analyze database
if converter.connect():
    # Filter and analyze schema
    converter.load_and_filter_schema(min_rows=1)
    
    # Export metadata
    metadata_file = converter.export_database_metadata()
    
    # Execute natural language query
    sql, df, context = converter.natural_query_to_dataframe(
        "Show me top 10 customers by order count"
    )
    
    # Get database summary
    summary = converter.get_database_summary()
    
    converter.disconnect()
```

## 🔧 Configuration

### Database Configuration

```python
# config.py - Modify default settings
class DatabaseConfig:
    DEFAULT_SERVER = 'your-server'
    DEFAULT_DATABASE = 'your-database'
    DEFAULT_UID = 'your-username'
    DEFAULT_PWD = 'your-password'
```

### Analysis Configuration

```python
class AnalysisConfig:
    MIN_ROWS_DEFAULT = 1           # Minimum rows to include table
    MAX_ANALYSIS_ROWS = 10000      # Full analysis threshold
    SAMPLE_SIZE_DEFAULT = 5        # Sample data size
    PK_UNIQUENESS_THRESHOLD = 0.95 # Primary key uniqueness requirement
```

## 📊 Output Examples

### Database Summary
```
Database Overview:
   • Total tables with data: 45
   • Total rows: 2,847,392
   • Total columns: 312
   • Average data quality: 0.847

Data Quality Analysis:
   • High quality tables: 12
   • Best tables: users, orders, products

Relationship Analysis:
   • Tables with foreign keys: 8
   • Potential relationships: 23
```

### Query Analysis
```
Query Analysis:
   Type: aggregation
   Complexity: moderate
   Intent: count_records
   Table: dbo.orders
   Columns: []
   Conditions: ['status = "active"']

Generated SQL:
SELECT COUNT(*)
FROM dbo.orders
WHERE status = 'active'
```

### Metadata Export
```json
{
  "database_info": {
    "server": "localhost",
    "database": "ecommerce",
    "export_timestamp": "2024-01-15T10:30:00",
    "total_tables": 45,
    "total_rows": 2847392
  },
  "tables": {
    "dbo.users": {
      "table_name": "users",
      "row_count": 15420,
      "column_count": 8,
      "columns": [...],
      "statistics": {
        "data_quality_score": 0.892,
        "primary_key_candidates": ["user_id"],
        "foreign_key_candidates": []
      }
    }
  }
}
```

## 🎯 Advanced Features

### Data Quality Scoring

The system automatically calculates quality scores based on:
- **Null percentage** (50% weight) - Lower nulls = higher quality
- **Uniqueness ratio** (30% weight) - Appropriate uniqueness levels
- **Data presence** (20% weight) - Actual data availability

### Relationship Detection

Automatically identifies:
- **Primary key candidates** - High uniqueness, low nulls, appropriate types
- **Foreign key candidates** - ID columns that reference other tables
- **Table relationships** - Potential joins between tables

### Query Context Analysis

Analyzes queries for:
- **Query type** - retrieval, aggregation, filtered_retrieval
- **Complexity level** - simple, moderate, complex
- **Intent classification** - count_records, calculate_average, search_data
- **Table suggestions** - Relevant tables based on keywords

## 🛠️ Development

### Adding New Features

1. **Database Analysis** - Extend `database_analyzer.py`
2. **Query Parsing** - Enhance `query_parser.py`
3. **Metadata Management** - Modify `metadata_manager.py`
4. **Configuration** - Update `config.py`

### Testing

```bash
# Test with demonstration mode
python main.py --demo

# Test specific queries
python main.py --interactive
> "your test query here"

# Test offline analysis
python main.py --analyze sample_metadata.json
```

## 📋 Command Reference

### Command Line Options

```bash
python main.py [OPTIONS]

Modes:
  --demo                    # Full demonstration mode
  --interactive             # Interactive query mode  
  --analyze FILE            # Offline analysis from JSON

Database Options:
  --server SERVER           # Database server
  --database DATABASE       # Database name
  --uid USERNAME           # User ID
  --pwd PASSWORD           # Password

Information:
  --config                 # Show current configuration
  --structure              # Show project file structure
  --examples               # Show usage examples
  --help-all              # Comprehensive help
  --version               # Show version
```

### Interactive Commands

```bash
# In interactive mode
tables                    # List all available tables
summary                   # Show database summary
export                    # Export metadata to JSON
suggest [table]           # Get query suggestions for table
help                      # Show help and examples
quit                      # Exit interactive mode
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.