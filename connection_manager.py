"""
Connection manager for Azure Cosmos DB MongoDB Migration Tool.
Implements connection retry logic with exponential backoff and health monitoring.
"""

import logging
import time
import threading
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, PyMongoError
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
import getpass

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Manages connections to source and destination MongoDB instances with retry logic and health monitoring."""
    
    def __init__(self, config):
        """Initialize the connection manager with configuration.
        
        Args:
            config: Config object containing connection settings
        """
        self.config = config
        self.source_client = None
        self.dest_client = None
        self._source_healthy = False
        self._dest_healthy = False
        self._health_check_lock = threading.Lock()
        self._shutdown_requested = False
        
        # Start health monitoring thread
        self._health_monitor_thread = threading.Thread(target=self._health_monitor, daemon=True)
        self._health_monitor_thread.start()
        
    def _health_monitor(self):
        """Background health monitoring for connections."""
        while not self._shutdown_requested:
            try:
                # Check source connection health
                if self.source_client:
                    try:
                        self.source_client.admin.command('ping')
                        with self._health_check_lock:
                            self._source_healthy = True
                    except Exception as e:
                        logger.warning(f"Source connection health check failed: {e}")
                        with self._health_check_lock:
                            self._source_healthy = False
                
                # Check destination connection health
                if self.dest_client:
                    try:
                        self.dest_client.admin.command('ping')
                        with self._health_check_lock:
                            self._dest_healthy = True
                    except Exception as e:
                        logger.warning(f"Destination connection health check failed: {e}")
                        with self._health_check_lock:
                            self._dest_healthy = False
                
                # Sleep for configured interval before next health check
                time.sleep(self.config.health_check_interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")
                time.sleep(self.config.health_check_interval_seconds)
                
    def is_source_healthy(self):
        """Check if source connection is healthy.
        
        Returns:
            bool: True if source connection is healthy
        """
        with self._health_check_lock:
            return self._source_healthy
            
    def is_dest_healthy(self):
        """Check if destination connection is healthy.
        
        Returns:
            bool: True if destination connection is healthy
        """
        with self._health_check_lock:
            return self._dest_healthy
        
    def connect_to_source(self, retry_on_failure=True):
        """Connect to the source MongoDB instance with retry logic.
        
        Args:
            retry_on_failure: Whether to retry if existing connection is unhealthy
            
        Returns:
            MongoClient: Connected MongoDB client or None if connection fails
        """
        try:
            # Check existing connection health
            if self.source_client and self.is_source_healthy():
                logger.debug("Using existing healthy source connection")
                return self.source_client
                
            # If connection exists but is unhealthy, close it
            if self.source_client and not self.is_source_healthy():
                logger.info("Existing source connection is unhealthy, reconnecting...")
                try:
                    self.source_client.close()
                except Exception as e:
                    logger.warning(f"Error closing unhealthy source connection: {e}")
                self.source_client = None
                
            connection_string = self.config.source_connection_string
            
            # Prompt for connection string if not provided
            if not connection_string and not self.config.use_managed_identity:
                connection_string = self._prompt_for_connection_string("source")
                self.config.update_connection_strings(source_connection_string=connection_string)
                
            if self.config.use_managed_identity:
                logger.info("Connecting to source using Managed Identity")
                # For managed identity, a different approach is needed
                # This is a placeholder as the actual implementation depends on the Azure Cosmos DB SDK
                raise NotImplementedError("Managed Identity authentication is not yet implemented")
            else:
                logger.info("Connecting to source using connection string")
                self.source_client = self._create_mongo_client_with_retry(connection_string, "source")
                
            # Mark as healthy after successful connection
            with self._health_check_lock:
                self._source_healthy = True
                
            logger.info("Successfully connected to source MongoDB instance")
            return self.source_client
            
        except Exception as e:
            logger.error(f"Failed to connect to source MongoDB instance: {e}")
            self.source_client = None
            with self._health_check_lock:
                self._source_healthy = False
            return None
            
    def connect_to_destination(self, retry_on_failure=True):
        """Connect to the destination MongoDB instance with retry logic.
        
        Args:
            retry_on_failure: Whether to retry if existing connection is unhealthy
            
        Returns:
            MongoClient: Connected MongoDB client or None if connection fails
        """
        try:
            # Check existing connection health
            if self.dest_client and self.is_dest_healthy():
                logger.debug("Using existing healthy destination connection")
                return self.dest_client
                
            # If connection exists but is unhealthy, close it
            if self.dest_client and not self.is_dest_healthy():
                logger.info("Existing destination connection is unhealthy, reconnecting...")
                try:
                    self.dest_client.close()
                except Exception as e:
                    logger.warning(f"Error closing unhealthy destination connection: {e}")
                self.dest_client = None
                
            connection_string = self.config.dest_connection_string
            
            # Prompt for connection string if not provided
            if not connection_string and not self.config.use_managed_identity:
                connection_string = self._prompt_for_connection_string("destination")
                self.config.update_connection_strings(dest_connection_string=connection_string)
                
            if self.config.use_managed_identity:
                logger.info("Connecting to destination using Managed Identity")
                # For managed identity, a different approach is needed
                # This is a placeholder as the actual implementation depends on the Azure Cosmos DB SDK
                raise NotImplementedError("Managed Identity authentication is not yet implemented")
            else:
                logger.info("Connecting to destination using connection string")
                self.dest_client = self._create_mongo_client_with_retry(connection_string, "destination")
                
            # Mark as healthy after successful connection
            with self._health_check_lock:
                self._dest_healthy = True
                
            logger.info("Successfully connected to destination MongoDB instance")
            return self.dest_client
            
        except Exception as e:
            logger.error(f"Failed to connect to destination MongoDB instance: {e}")
            self.dest_client = None
            with self._health_check_lock:
                self._dest_healthy = False
            return None
    
    def _create_mongo_client_with_retry(self, connection_string, client_type="unknown"):
        """Create a MongoDB client with comprehensive retry logic and exponential backoff.
        
        Args:
            connection_string: MongoDB connection string
            client_type: String identifier for logging (source/destination)
            
        Returns:
            MongoClient: Connected MongoDB client
            
        Raises:
            ConnectionFailure: If connection fails after all retries
        """
        retry_attempts = self.config.connection_retry_attempts
        base_delay_ms = self.config.connection_retry_delay_ms
        
        logger.info(f"Attempting to connect to {client_type} with {retry_attempts} retries")
        
        for attempt in range(retry_attempts):
            try:
                # Configure client with appropriate settings for Azure Cosmos DB
                client = MongoClient(
                    connection_string,
                    serverSelectionTimeoutMS=5000,  # Faster server selection for retries
                    connectTimeoutMS=self.config.timeout_ms,
                    socketTimeoutMS=self.config.timeout_ms,
                    maxPoolSize=10,
                    minPoolSize=1,
                    maxIdleTimeMS=30000,  # Close connections after 30s of inactivity
                    retryWrites=True,
                    retryReads=True,
                    heartbeatFrequencyMS=10000,  # Health check every 10 seconds
                )
                
                # Test connection with ping command
                result = client.admin.command('ping')
                logger.info(f"Successfully connected to {client_type} on attempt {attempt + 1}")
                logger.debug(f"Ping result: {result}")
                return client
                
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                if attempt < retry_attempts - 1:
                    # Calculate exponential backoff delay with jitter
                    backoff_multiplier = 2 ** attempt
                    jitter = (attempt * 0.1)  # Add small jitter to prevent thundering herd
                    delay_seconds = (base_delay_ms / 1000.0) * backoff_multiplier + jitter
                    
                    logger.warning(
                        f"{client_type.capitalize()} connection attempt {attempt + 1}/{retry_attempts} failed: {e}. "
                        f"Retrying in {delay_seconds:.2f} seconds..."
                    )
                    time.sleep(delay_seconds)
                else:
                    logger.error(f"Failed to connect to {client_type} after {retry_attempts} attempts: {e}")
                    raise ConnectionFailure(f"Could not connect to {client_type} after {retry_attempts} attempts: {e}")
                    
            except Exception as e:
                logger.error(f"Unexpected error connecting to {client_type}: {e}")
                if attempt < retry_attempts - 1:
                    delay_seconds = (base_delay_ms / 1000.0) * (2 ** attempt)
                    logger.info(f"Retrying in {delay_seconds:.2f} seconds...")
                    time.sleep(delay_seconds)
                else:
                    raise
                    
    def execute_with_retry(self, operation, client_type="unknown", *args, **kwargs):
        """Execute a database operation with automatic retry on connection failure.
        
        Args:
            operation: Function to execute
            client_type: "source" or "destination" for connection retry
            *args: Arguments to pass to the operation
            **kwargs: Keyword arguments to pass to the operation
            
        Returns:
            Result of the operation
            
        Raises:
            Exception: If operation fails after retries
        """
        max_retries = self.config.operation_retry_attempts
        for attempt in range(max_retries):
            try:
                return operation(*args, **kwargs)
                
            except (ConnectionFailure, ServerSelectionTimeoutError, PyMongoError) as e:
                logger.warning(f"Operation failed due to connection issue (attempt {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    # Try to reconnect
                    if client_type == "source":
                        logger.info("Attempting to reconnect to source...")
                        self.connect_to_source(retry_on_failure=True)
                    elif client_type == "destination":
                        logger.info("Attempting to reconnect to destination...")
                        self.connect_to_destination(retry_on_failure=True)
                    
                    # Wait before retry
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Operation failed after {max_retries} attempts: {e}")
                    raise
                    
    def _prompt_for_connection_string(self, instance_type):
        """Prompt user for connection string.
        
        Args:
            instance_type: String indicating "source" or "destination"
            
        Returns:
            str: Connection string entered by user
        """
        print(f"\nEnter {instance_type} MongoDB connection string:")
        print("Example: mongodb://account:password@account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false")
        return getpass.getpass(f"{instance_type.capitalize()} connection string: ")
        
    def close_connections(self):
        """Close all active connections and stop health monitoring."""
        logger.info("Closing all connections...")
        
        # Stop health monitoring
        self._shutdown_requested = True
        
        if self.source_client:
            try:
                logger.info("Closing source connection")
                self.source_client.close()
                with self._health_check_lock:
                    self._source_healthy = False
            except Exception as e:
                logger.warning(f"Error closing source connection: {e}")
            finally:
                self.source_client = None
            
        if self.dest_client:
            try:
                logger.info("Closing destination connection")
                self.dest_client.close()
                with self._health_check_lock:
                    self._dest_healthy = False
            except Exception as e:
                logger.warning(f"Error closing destination connection: {e}")
            finally:
                self.dest_client = None
                
        # Wait for health monitor thread to finish
        try:
            if self._health_monitor_thread.is_alive():
                self._health_monitor_thread.join(timeout=5)
        except Exception as e:
            logger.warning(f"Error stopping health monitor thread: {e}")
            
        logger.info("All connections closed successfully")
