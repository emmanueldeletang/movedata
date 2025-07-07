"""
PostgreSQL connection manager for Azure Database Migration Tool.
Implements connection management, retry logic, and health monitoring for PostgreSQL databases.
"""

import logging
import time
import threading
import psycopg2
from psycopg2 import pool, sql, OperationalError, DatabaseError
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
import getpass

logger = logging.getLogger(__name__)

class PostgreSQLConnectionManager:
    """Manages connections to source and destination PostgreSQL instances with retry logic and health monitoring."""
    
    def __init__(self, config):
        """Initialize the PostgreSQL connection manager with configuration.
        
        Args:
            config: Config object containing connection settings
        """
        self.config = config
        self.source_pool = None
        self.dest_pool = None
        self._source_healthy = False
        self._dest_healthy = False
        self._health_check_lock = threading.Lock()
        self._shutdown_requested = False
        
        # Start health monitoring thread
        self._health_monitor_thread = threading.Thread(target=self._health_monitor, daemon=True)
        self._health_monitor_thread.start()
        
    def _health_monitor(self):
        """Background health monitoring for PostgreSQL connections."""
        while not self._shutdown_requested:
            try:
                # Check source connection health
                if self.source_pool:
                    try:
                        conn = self.source_pool.getconn()
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT 1")
                            cursor.fetchone()
                        self.source_pool.putconn(conn)
                        with self._health_check_lock:
                            self._source_healthy = True
                    except Exception as e:
                        logger.warning(f"Source PostgreSQL connection health check failed: {e}")
                        with self._health_check_lock:
                            self._source_healthy = False
                
                # Check destination connection health
                if self.dest_pool:
                    try:
                        conn = self.dest_pool.getconn()
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT 1")
                            cursor.fetchone()
                        self.dest_pool.putconn(conn)
                        with self._health_check_lock:
                            self._dest_healthy = True
                    except Exception as e:
                        logger.warning(f"Destination PostgreSQL connection health check failed: {e}")
                        with self._health_check_lock:
                            self._dest_healthy = False
                
                # Sleep for configured interval before next health check
                time.sleep(self.config.health_check_interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in PostgreSQL health monitoring: {e}")
                time.sleep(self.config.health_check_interval_seconds)
                
    def is_source_healthy(self):
        """Check if source PostgreSQL connection is healthy.
        
        Returns:
            bool: True if source connection is healthy
        """
        with self._health_check_lock:
            return self._source_healthy
            
    def is_dest_healthy(self):
        """Check if destination PostgreSQL connection is healthy.
        
        Returns:
            bool: True if destination connection is healthy
        """
        with self._health_check_lock:
            return self._dest_healthy
            
    def connect_to_source(self, retry_on_failure=True):
        """Connect to the source PostgreSQL instance with retry logic.
        
        Args:
            retry_on_failure: Whether to retry if existing connection is unhealthy
            
        Returns:
            psycopg2.pool.ThreadedConnectionPool: Connected PostgreSQL connection pool or None if connection fails
        """
        try:
            # Check existing connection health
            if self.source_pool and self.is_source_healthy():
                logger.debug("Using existing healthy source PostgreSQL connection")
                return self.source_pool
                
            # If connection exists but is unhealthy, close it
            if self.source_pool and not self.is_source_healthy():
                logger.info("Existing source PostgreSQL connection is unhealthy, reconnecting...")
                try:
                    self.source_pool.closeall()
                except Exception as e:
                    logger.warning(f"Error closing unhealthy source PostgreSQL connection: {e}")
                self.source_pool = None
                
            connection_string = getattr(self.config, 'pg_source_connection_string', None)
            
            # Prompt for connection string if not provided
            if not connection_string and not getattr(self.config, 'use_managed_identity', False):
                connection_string = self._prompt_for_connection_string("source PostgreSQL")
                self.config.pg_source_connection_string = connection_string
                
            if getattr(self.config, 'use_managed_identity', False):
                logger.info("Connecting to source PostgreSQL using Managed Identity")
                # For Azure PostgreSQL with Managed Identity
                self.source_pool = self._create_pg_pool_with_managed_identity(connection_string, "source")
            else:
                logger.info("Connecting to source PostgreSQL using connection string")
                self.source_pool = self._create_pg_pool_with_retry(connection_string, "source")
                
            # Mark as healthy after successful connection
            with self._health_check_lock:
                self._source_healthy = True
                
            logger.info("Successfully connected to source PostgreSQL instance")
            return self.source_pool
            
        except Exception as e:
            logger.error(f"Failed to connect to source PostgreSQL instance: {e}")
            self.source_pool = None
            with self._health_check_lock:
                self._source_healthy = False
            return None
            
    def connect_to_destination(self, retry_on_failure=True):
        """Connect to the destination PostgreSQL instance with retry logic.
        
        Args:
            retry_on_failure: Whether to retry if existing connection is unhealthy
            
        Returns:
            psycopg2.pool.ThreadedConnectionPool: Connected PostgreSQL connection pool or None if connection fails
        """
        try:
            # Check existing connection health
            if self.dest_pool and self.is_dest_healthy():
                logger.debug("Using existing healthy destination PostgreSQL connection")
                return self.dest_pool
                
            # If connection exists but is unhealthy, close it
            if self.dest_pool and not self.is_dest_healthy():
                logger.info("Existing destination PostgreSQL connection is unhealthy, reconnecting...")
                try:
                    self.dest_pool.closeall()
                except Exception as e:
                    logger.warning(f"Error closing unhealthy destination PostgreSQL connection: {e}")
                self.dest_pool = None
                
            connection_string = getattr(self.config, 'pg_dest_connection_string', None)
            
            # Prompt for connection string if not provided
            if not connection_string and not getattr(self.config, 'use_managed_identity', False):
                connection_string = self._prompt_for_connection_string("destination PostgreSQL")
                self.config.pg_dest_connection_string = connection_string
                
            if getattr(self.config, 'use_managed_identity', False):
                logger.info("Connecting to destination PostgreSQL using Managed Identity")
                # For Azure PostgreSQL with Managed Identity
                self.dest_pool = self._create_pg_pool_with_managed_identity(connection_string, "destination")
            else:
                logger.info("Connecting to destination PostgreSQL using connection string")
                self.dest_pool = self._create_pg_pool_with_retry(connection_string, "destination")
                
            # Mark as healthy after successful connection
            with self._health_check_lock:
                self._dest_healthy = True
                
            logger.info("Successfully connected to destination PostgreSQL instance")
            return self.dest_pool
            
        except Exception as e:
            logger.error(f"Failed to connect to destination PostgreSQL instance: {e}")
            self.dest_pool = None
            with self._health_check_lock:
                self._dest_healthy = False
            return None
            
    def _create_pg_pool_with_retry(self, connection_string, client_type="unknown"):
        """Create a PostgreSQL connection pool with comprehensive retry logic and exponential backoff.
        
        Args:
            connection_string: PostgreSQL connection string
            client_type: String identifier for logging (source/destination)
            
        Returns:
            psycopg2.pool.ThreadedConnectionPool: Connected PostgreSQL connection pool
            
        Raises:
            OperationalError: If connection fails after all retries
        """
        retry_attempts = self.config.connection_retry_attempts
        base_delay_ms = self.config.connection_retry_delay_ms
        
        logger.info(f"Attempting to connect to PostgreSQL {client_type} with {retry_attempts} retries")
        
        for attempt in range(retry_attempts):
            try:
                # Create connection pool with Azure PostgreSQL optimized settings
                pool_obj = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=20,
                    dsn=connection_string,
                    # Connection parameters for Azure PostgreSQL
                    connect_timeout=30,
                    application_name=f"Azure_Migration_Tool_{client_type}",
                    # SSL settings for Azure PostgreSQL
                    sslmode='require',
                )
                
                # Test connection with a simple query
                test_conn = pool_obj.getconn()
                try:
                    with test_conn.cursor() as cursor:
                        cursor.execute("SELECT version()")
                        result = cursor.fetchone()
                        logger.info(f"Successfully connected to PostgreSQL {client_type} on attempt {attempt + 1}")
                        logger.debug(f"PostgreSQL version: {result[0] if result else 'Unknown'}")
                finally:
                    pool_obj.putconn(test_conn)
                
                return pool_obj
                
            except (OperationalError, DatabaseError) as e:
                if attempt < retry_attempts - 1:
                    # Calculate exponential backoff delay with jitter
                    backoff_multiplier = 2 ** attempt
                    jitter = (attempt * 0.1)  # Add small jitter to prevent thundering herd
                    delay_seconds = (base_delay_ms / 1000.0) * backoff_multiplier + jitter
                    
                    logger.warning(
                        f"PostgreSQL {client_type.capitalize()} connection attempt {attempt + 1}/{retry_attempts} failed: {e}. "
                        f"Retrying in {delay_seconds:.2f} seconds..."
                    )
                    time.sleep(delay_seconds)
                else:
                    logger.error(f"Failed to connect to PostgreSQL {client_type} after {retry_attempts} attempts: {e}")
                    raise OperationalError(f"Could not connect to PostgreSQL {client_type} after {retry_attempts} attempts: {e}")
                    
            except Exception as e:
                logger.error(f"Unexpected error connecting to PostgreSQL {client_type}: {e}")
                if attempt < retry_attempts - 1:
                    delay_seconds = (base_delay_ms / 1000.0) * (2 ** attempt)
                    logger.info(f"Retrying in {delay_seconds:.2f} seconds...")
                    time.sleep(delay_seconds)
                else:
                    raise
                    
    def _create_pg_pool_with_managed_identity(self, connection_string, client_type="unknown"):
        """Create a PostgreSQL connection pool using Azure Managed Identity.
        
        Args:
            connection_string: Base PostgreSQL connection string (without password)
            client_type: String identifier for logging (source/destination)
            
        Returns:
            psycopg2.pool.ThreadedConnectionPool: Connected PostgreSQL connection pool
        """
        try:
            # Get Azure access token for PostgreSQL
            credential = DefaultAzureCredential()
            token = credential.get_token("https://ossrdbms-aad.database.windows.net/.default")
            
            # Parse connection string and add token as password
            # For Azure PostgreSQL with Managed Identity, the token is used as password
            if "password=" in connection_string:
                # Remove existing password and replace with token
                import re
                connection_string = re.sub(r'password=[^;\s]*[;\s]?', '', connection_string)
            
            # Add token as password
            if connection_string.endswith(';'):
                connection_string += f"password={token.token}"
            else:
                connection_string += f";password={token.token}"
            
            logger.info(f"Using Managed Identity for PostgreSQL {client_type} connection")
            return self._create_pg_pool_with_retry(connection_string, client_type)
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Managed Identity for PostgreSQL {client_type}: {e}")
            raise
            
    def execute_with_retry(self, operation, client_type="unknown", *args, **kwargs):
        """Execute a PostgreSQL operation with automatic retry on connection failure.
        
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
                
            except (OperationalError, DatabaseError) as e:
                logger.warning(f"PostgreSQL operation failed due to connection issue (attempt {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    # Try to reconnect
                    if client_type == "source":
                        logger.info("Attempting to reconnect to source PostgreSQL...")
                        self.connect_to_source(retry_on_failure=True)
                    elif client_type == "destination":
                        logger.info("Attempting to reconnect to destination PostgreSQL...")
                        self.connect_to_destination(retry_on_failure=True)
                    
                    # Wait before retry
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"PostgreSQL operation failed after {max_retries} attempts: {e}")
                    raise
                    
    def _prompt_for_connection_string(self, instance_type):
        """Prompt user for PostgreSQL connection string.
        
        Args:
            instance_type: String indicating "source PostgreSQL" or "destination PostgreSQL"
            
        Returns:
            str: Connection string entered by user
        """
        print(f"\nEnter {instance_type} connection string:")
        print("Example: postgresql://username:password@hostname:5432/database")
        print("Azure PostgreSQL: postgresql://username%40servername:password@servername.postgres.database.azure.com:5432/database")
        return getpass.getpass(f"{instance_type.capitalize()} connection string: ")
        
    def close_connections(self):
        """Close all active PostgreSQL connections and stop health monitoring."""
        logger.info("Closing all PostgreSQL connections...")
        
        # Stop health monitoring
        self._shutdown_requested = True
        
        if self.source_pool:
            try:
                logger.info("Closing source PostgreSQL connection pool")
                self.source_pool.closeall()
                with self._health_check_lock:
                    self._source_healthy = False
            except Exception as e:
                logger.warning(f"Error closing source PostgreSQL connection pool: {e}")
            finally:
                self.source_pool = None
            
        if self.dest_pool:
            try:
                logger.info("Closing destination PostgreSQL connection pool")
                self.dest_pool.closeall()
                with self._health_check_lock:
                    self._dest_healthy = False
            except Exception as e:
                logger.warning(f"Error closing destination PostgreSQL connection pool: {e}")
            finally:
                self.dest_pool = None
                
        # Wait for health monitor thread to finish
        try:
            if self._health_monitor_thread.is_alive():
                self._health_monitor_thread.join(timeout=5)
        except Exception as e:
            logger.warning(f"Error stopping PostgreSQL health monitor thread: {e}")
            
        logger.info("All PostgreSQL connections closed successfully")
