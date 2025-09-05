"""
Configuration Module
Contains database connection settings and system configurations
"""

import os
from typing import Dict, Any


class DatabaseConfig:
    """Database connection configuration"""
    
    # Default database connection parameters
    DEFAULT_SERVER = 'VMHOST1001.hq.cleverex.com'
    DEFAULT_DATABASE = 'JoyHS_TestDrive'
    DEFAULT_UID = 'agent_test'
    DEFAULT_PWD = 'qxYdGTaR71V3JyQ04oUd'
    
    # Connection string template
    CONNECTION_TEMPLATE = (
        "DRIVER={{ODBC Driver 18 for SQL Server}};"
        "SERVER={server};"
        "DATABASE={database};"
        "UID={uid};PWD={pwd};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    
    @classmethod
    def get_connection_params(cls, server: str = None, database: str = None, 
                            uid: str = None, pwd: str = None) -> Dict[str, str]:
        """Get database connection parameters with defaults"""
        return {
            'server': server or cls.DEFAULT_SERVER,
            'database': database or cls.DEFAULT_DATABASE,
            'uid': uid or cls.DEFAULT_UID,
            'pwd': pwd or cls.DEFAULT_PWD
        }
    
    @classmethod
    def get_connection_string(cls, **kwargs) -> str:
        """Get formatted connection string"""
        params = cls.get_connection_params(**kwargs)
        return cls.CONNECTION_TEMPLATE.format(**params)


class AnalysisConfig:
    """Configuration for database analysis and processing"""
    
    # Schema filtering settings
    MIN_ROWS_DEFAULT = 1
    MAX_ANALYSIS_ROWS = 10000  # Full analysis for tables with fewer rows
    SAMPLE_SIZE_DEFAULT = 5
    
    # Data quality scoring weights
    QUALITY_WEIGHTS = {
        'null_score': 0.5,
        'uniqueness_score': 0.3,
        'data_presence_score': 0.2
    }
    
    # Primary key identification thresholds
    PK_UNIQUENESS_THRESHOLD = 0.95
    PK_NULL_THRESHOLD = 5.0  # Max null percentage
    PK_VALID_TYPES = ['int', 'bigint', 'uniqueidentifier', 'varchar', 'nvarchar']
    
    # Query parsing settings
    MAX_SUGGESTED_TABLES = 5
    MAX_QUERY_SUGGESTIONS = 6
    
    # Export settings
    METADATA_FILENAME_TEMPLATE = "database_metadata_{database}_{timestamp}.json"
    
    @classmethod
    def get_analysis_settings(cls) -> Dict[str, Any]:
        """Get analysis configuration settings"""
        return {
            'min_rows': cls.MIN_ROWS_DEFAULT,
            'max_analysis_rows': cls.MAX_ANALYSIS_ROWS,
            'sample_size': cls.SAMPLE_SIZE_DEFAULT,
            'quality_weights': cls.QUALITY_WEIGHTS,
            'pk_thresholds': {
                'uniqueness': cls.PK_UNIQUENESS_THRESHOLD,
                'null_percentage': cls.PK_NULL_THRESHOLD,
                'valid_types': cls.PK_VALID_TYPES
            }
        }


class DisplayConfig:
    """Configuration for display and output formatting"""
    
    # Pandas display settings
    PANDAS_DISPLAY_OPTIONS = {
        'display.max_columns': 10,
        'display.width': 1000,
        'display.max_colwidth': 40
    }
    
    # Console output settings
    CONSOLE_WIDTH = 80
    SECTION_SEPARATOR = "=" * CONSOLE_WIDTH
    SUBSECTION_SEPARATOR = "-" * 50
    
    # Result display limits
    MAX_ROWS_DISPLAY = 10
    MAX_COLUMNS_DISPLAY = 8
    MAX_TABLES_LIST = 15
    MAX_NUMERIC_SUMMARY_COLS = 3
    
    @classmethod
    def apply_pandas_settings(cls):
        """Apply pandas display settings"""
        import pandas as pd
        for option, value in cls.PANDAS_DISPLAY_OPTIONS.items():
            pd.set_option(option, value)


class LoggingConfig:
    """Configuration for logging and debugging"""
    
    # Log levels
    LOG_LEVELS = {
        'DEBUG': 0,
        'INFO': 1,
        'WARNING': 2,
        'ERROR': 3
    }
    
    # Default log level
    DEFAULT_LOG_LEVEL = 'INFO'
    
    # Log message formats
    LOG_FORMATS = {
        'DEBUG': "🔍 {message}",
        'INFO': "ℹ️ {message}",
        'WARNING': "⚠️ {message}",
        'ERROR': "❌ {message}",
        'SUCCESS': "✅ {message}"
    }
    
    @classmethod
    def format_message(cls, level: str, message: str) -> str:
        """Format log message with appropriate emoji"""
        format_template = cls.LOG_FORMATS.get(level.upper(), "{message}")
        return format_template.format(message=message)


class EnvironmentConfig:
    """Environment-specific configuration"""
    
    @classmethod
    def get_from_env(cls, key: str, default: str = None) -> str:
        """Get configuration value from environment variable"""
        return os.getenv(key, default)
    
    @classmethod
    def get_database_config_from_env(cls) -> Dict[str, str]:
        """Get database configuration from environment variables"""
        return {
            'server': cls.get_from_env('DB_SERVER', DatabaseConfig.DEFAULT_SERVER),
            'database': cls.get_from_env('DB_DATABASE', DatabaseConfig.DEFAULT_DATABASE),
            'uid': cls.get_from_env('DB_UID', DatabaseConfig.DEFAULT_UID),
            'pwd': cls.get_from_env('DB_PWD', DatabaseConfig.DEFAULT_PWD)
        }
    
    @classmethod
    def is_development(cls) -> bool:
        """Check if running in development environment"""
        return cls.get_from_env('ENVIRONMENT', 'production').lower() in ['dev', 'development']
    
    @classmethod
    def is_production(cls) -> bool:
        """Check if running in production environment"""
        return cls.get_from_env('ENVIRONMENT', 'production').lower() == 'production'


# Global configuration instance
class Config:
    """Main configuration class combining all configs"""
    
    database = DatabaseConfig()
    analysis = AnalysisConfig()
    display = DisplayConfig()
    logging = LoggingConfig()
    environment = EnvironmentConfig()
    
    @classmethod
    def initialize(cls):
        """Initialize configuration settings"""
        cls.display.apply_pandas_settings()
        
        # Set debug mode based on environment
        if cls.environment.is_development():
            cls.logging.DEFAULT_LOG_LEVEL = 'DEBUG'
    
    @classmethod
    def get_all_settings(cls) -> Dict[str, Any]:
        """Get all configuration settings"""
        return {
            'database': cls.database.get_connection_params(),
            'analysis': cls.analysis.get_analysis_settings(),
            'display': cls.display.PANDAS_DISPLAY_OPTIONS,
            'environment': {
                'is_development': cls.environment.is_development(),
                'is_production': cls.environment.is_production()
            }
        }


# Initialize configuration on import
Config.initialize()