"""
Configuration module for Azure Cosmos DB MongoDB Migration Tool.
"""

import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class Config:
    """Configuration class to manage environment variables and settings."""
    
    def __init__(self):
        """Initialize configuration with default values."""
        # MongoDB connection settings
        self.source_connection_string = None
        self.dest_connection_string = None
        
        # PostgreSQL connection settings
        self.pg_source_connection_string = None
        self.pg_dest_connection_string = None
        
        # General settings
        self.use_managed_identity = False
        self.batch_size = 1000
        self.timeout_ms = 30000
        
        # Enhanced retry settings
        self.retry_attempts = 3
        self.retry_delay_ms = 1000
        self.connection_retry_attempts = 5  # More retries for initial connection
        self.connection_retry_delay_ms = 2000  # Longer delay for connection retries
        self.operation_retry_attempts = 3  # Retries for database operations
        self.health_check_interval_seconds = 30  # Health check frequency
        
        # Cosmos DB target configuration
        self.target_is_vcore = True  # Default to vCore
        self.ru_throughput_mode = "manual"  # "manual" or "autoscale"
        self.ru_manual_throughput = 400  # Manual RU/s
        self.ru_autoscale_max_throughput = 4000  # Max RU/s for autoscale
        self.ru_default_partition_key = "_id"  # Default partition key for RU collections
        
    def load_config(self):
        """Load configuration from environment variables or .env file."""
        try:
            # Load environment variables from .env file if it exists
            load_dotenv()
            
            # MongoDB connection strings
            self.source_connection_string = os.getenv("SOURCE_CONNECTION_STRING")
            self.dest_connection_string = os.getenv("DEST_CONNECTION_STRING")
            
            # PostgreSQL connection strings
            self.pg_source_connection_string = os.getenv("PG_SOURCE_CONNECTION_STRING")
            self.pg_dest_connection_string = os.getenv("PG_DEST_CONNECTION_STRING")
            
            # Check if managed identity should be used
            self.use_managed_identity = os.getenv("USE_MANAGED_IDENTITY", "False").lower() == "true"
            
            # Performance settings
            self.batch_size = int(os.getenv("BATCH_SIZE", "1000"))
            self.timeout_ms = int(os.getenv("TIMEOUT_MS", "30000"))
            
            # Enhanced retry settings
            self.retry_attempts = int(os.getenv("RETRY_ATTEMPTS", "3"))
            self.retry_delay_ms = int(os.getenv("RETRY_DELAY_MS", "1000"))
            self.connection_retry_attempts = int(os.getenv("CONNECTION_RETRY_ATTEMPTS", "5"))
            self.connection_retry_delay_ms = int(os.getenv("CONNECTION_RETRY_DELAY_MS", "2000"))
            self.operation_retry_attempts = int(os.getenv("OPERATION_RETRY_ATTEMPTS", "3"))
            self.health_check_interval_seconds = int(os.getenv("HEALTH_CHECK_INTERVAL_SECONDS", "30"))
            
            # Cosmos DB target configuration
            self.target_is_vcore = os.getenv("TARGET_IS_VCORE", "true").lower() == "true"
            self.ru_throughput_mode = os.getenv("RU_THROUGHPUT_MODE", "manual").lower()
            self.ru_manual_throughput = int(os.getenv("RU_MANUAL_THROUGHPUT", "400"))
            self.ru_autoscale_max_throughput = int(os.getenv("RU_AUTOSCALE_MAX_THROUGHPUT", "4000"))
            self.ru_default_partition_key = os.getenv("RU_DEFAULT_PARTITION_KEY", "_id")
            
            logger.info("Configuration loaded successfully.")
            
            # Validate required settings
            if not self.use_managed_identity:
                if not self.source_connection_string:
                    logger.warning("SOURCE_CONNECTION_STRING not set. You will be prompted to enter it.")
                if not self.dest_connection_string:
                    logger.warning("DEST_CONNECTION_STRING not set. You will be prompted to enter it.")
                if not self.pg_source_connection_string:
                    logger.warning("PG_SOURCE_CONNECTION_STRING not set. You will be prompted to enter it.")
                if not self.pg_dest_connection_string:
                    logger.warning("PG_DEST_CONNECTION_STRING not set. You will be prompted to enter it.")
                    
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
            
    def update_connection_strings(self, source_connection_string=None, dest_connection_string=None):
        """Update connection strings if provided."""
        if source_connection_string:
            self.source_connection_string = source_connection_string
        if dest_connection_string:
            self.dest_connection_string = dest_connection_string
