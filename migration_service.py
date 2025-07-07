"""
Migration service for Azure Cosmos DB MongoDB Migration Tool.
Implements retry logic for database operations using connection manager.
"""

import logging
import time
import traceback
from pymongo.errors import PyMongoError, BulkWriteError, ConnectionFailure, ServerSelectionTimeoutError
from pymongo import ReplaceOne
from tqdm import tqdm

try:
    # Try relative import first (when running as module)
    from .cosmos_ru_manager import CosmosDBRUManager
except ImportError:
    # Fall back to absolute import (when running directly)
    from cosmos_ru_manager import CosmosDBRUManager

logger = logging.getLogger(__name__)

class MigrationService:
    """Service to handle migration of data between MongoDB instances with connection retry support."""
    
    def __init__(self, source_client, dest_client, config, connection_manager=None, batch_size=1000):
        """Initialize the migration service.
        
        Args:
            source_client: MongoDB client for source
            dest_client: MongoDB client for destination
            config: Configuration object
            connection_manager: Connection manager for retry functionality
            batch_size: Number of documents to process in each batch
        """
        self.source_client = source_client
        self.dest_client = dest_client
        self.config = config
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        
        # Initialize Cosmos DB RU manager
        self.ru_manager = CosmosDBRUManager(dest_client, config)
        
        # Detect target database type
        self.detected_target_type = self.ru_manager.detect_cosmos_db_type()
        logger.info(f"Detected target database type: {self.detected_target_type}")
        
        # Override config if detection is successful
        if self.detected_target_type == "ru":
            self.config.target_is_vcore = False
            logger.info("Target detected as RU-based Cosmos DB - enabling RU features")
        elif self.detected_target_type == "vcore":
            self.config.target_is_vcore = True
            logger.info("Target detected as vCore-based Cosmos DB - using standard MongoDB operations")
        
    def list_databases(self):
        """List all databases in the source with retry logic.
        
        Returns:
            list: List of database names
        """
        def _list_databases_operation():
            # Skip system databases
            system_dbs = ['admin', 'local', 'config']
            db_names = [db for db in self.source_client.list_database_names() if db not in system_dbs]
            return db_names
        
        try:
            if self.connection_manager:
                return self.connection_manager.execute_with_retry(_list_databases_operation, "source")
            else:
                return _list_databases_operation()
        except PyMongoError as e:
            logger.error(f"Error listing databases: {e}")
            return []
            
    def list_collections(self, database_name):
        """List all collections in a database with retry logic.
        
        Args:
            database_name: Name of the database
            
        Returns:
            list: List of collection names
        """
        def _list_collections_operation():
            db = self.source_client[database_name]
            collection_names = db.list_collection_names()
            return collection_names
        
        try:
            if self.connection_manager:
                return self.connection_manager.execute_with_retry(_list_collections_operation, "source")
            else:
                return _list_collections_operation()
        except PyMongoError as e:
            logger.error(f"Error listing collections for database {database_name}: {e}")
            return []
            
    def count_documents(self, database_name, collection_name):
        """Count documents in a collection with retry logic.
        
        Args:
            database_name: Name of the database
            collection_name: Name of the collection
            
        Returns:
            int: Number of documents in the collection
        """
        def _count_documents_operation():
            db = self.source_client[database_name]
            collection = db[collection_name]
            return collection.count_documents({})
        
        try:
            if self.connection_manager:
                return self.connection_manager.execute_with_retry(_count_documents_operation, "source")
            else:
                return _count_documents_operation()
        except PyMongoError as e:
            logger.error(f"Error counting documents in {database_name}.{collection_name}: {e}")
            return 0
            
    def get_collection_count(self, database_name, collection_name):
        """Get document count for a collection (alias for count_documents).
        
        Args:
            database_name: Name of the database
            collection_name: Name of the collection
            
        Returns:
            int: Number of documents in the collection
        """
        return self.count_documents(database_name, collection_name)
        
    def migrate_database(self, database_name):
        """Migrate an entire database.
        
        Args:
            database_name: Name of the database to migrate
            
        Returns:
            dict: Migration statistics
        """
        try:
            collections = self.list_collections(database_name)
            
            stats = {
                "total_collections": len(collections),
                "successful_collections": 0,
                "failed_collections": 0,
                "total_documents": 0,
                "migrated_documents": 0,
                "failed_documents": 0
            }
            
            for collection_name in collections:
                logger.info(f"Migrating collection: {database_name}.{collection_name}")
                collection_stats = self.migrate_collection(database_name, collection_name)
                
                # Update statistics
                if collection_stats["success"]:
                    stats["successful_collections"] += 1
                else:
                    stats["failed_collections"] += 1
                    
                stats["total_documents"] += collection_stats["total_documents"]
                stats["migrated_documents"] += collection_stats["migrated_documents"]
                stats["failed_documents"] += collection_stats["failed_documents"]
                
            logger.info(f"Database migration completed. Stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error migrating database {database_name}: {e}")
            logger.debug(traceback.format_exc())
            return {
                "success": False,
                "error": str(e)
            }
            
    def migrate_collection(self, database_name, collection_name):
        """Migrate a single collection.
        
        Args:
            database_name: Name of the database
            collection_name: Name of the collection
            
        Returns:
            dict: Migration statistics
        """
        try:
            source_db = self.source_client[database_name]
            source_collection = source_db[collection_name]
            
            dest_db = self.dest_client[database_name]
            
            # Ensure destination collection exists with proper settings
            if not self.config.target_is_vcore:
                # For RU-based Cosmos DB, create collection with throughput settings
                logger.info(f"Creating RU-based collection {database_name}.{collection_name}")
                
                # Get recommended partition key
                partition_key = self.ru_manager.get_recommended_partition_key(
                    database_name, collection_name, self.source_client
                )
                
                success = self.ru_manager.ensure_collection_exists(
                    database_name, collection_name, partition_key
                )
                
                if not success:
                    logger.error(f"Failed to create RU collection {database_name}.{collection_name}")
                    return {
                        "success": False,
                        "error": "Failed to create destination collection with RU settings"
                    }
                    
                # Get throughput information
                throughput_info = self.ru_manager.get_collection_throughput(database_name, collection_name)
                if throughput_info:
                    logger.info(f"Collection throughput info: {throughput_info}")
            else:
                # For vCore, ensure collection exists using standard methods
                if collection_name not in dest_db.list_collection_names():
                    dest_db.create_collection(collection_name)
                    logger.info(f"Created vCore collection: {database_name}.{collection_name}")
            
            dest_collection = dest_db[collection_name]
            
            # Count documents for progress tracking
            total_documents = self.count_documents(database_name, collection_name)
            
            if total_documents == 0:
                logger.info(f"Collection {database_name}.{collection_name} is empty. Nothing to migrate.")
                return {
                    "success": True,
                    "total_documents": 0,
                    "migrated_documents": 0,
                    "failed_documents": 0
                }
                
            # Check if destination collection already exists and has documents
            dest_count = dest_collection.count_documents({})
            if dest_count > 0:
                logger.info(f"Destination collection {database_name}.{collection_name} already has {dest_count} documents. Using upsert operations to handle duplicates.")
                
            # Initialize statistics
            stats = {
                "success": True,
                "total_documents": total_documents,
                "migrated_documents": 0,
                "failed_documents": 0,
                "inserted_documents": 0,
                "upserted_documents": 0,
                "modified_documents": 0
            }
            
            # Create progress bar
            progress_bar = tqdm(total=total_documents, desc=f"Migrating {database_name}.{collection_name}")
            
            # Process documents in batches
            cursor = source_collection.find({})
            batch = []
            
            for document in cursor:
                batch.append(document)
                
                if len(batch) >= self.batch_size:
                    batch_stats = self._process_batch(dest_collection, batch)
                    stats["migrated_documents"] += batch_stats["inserted"]
                    stats["failed_documents"] += batch_stats["failed"]
                    stats["inserted_documents"] += batch_stats.get("raw_inserted", 0)
                    stats["upserted_documents"] += batch_stats.get("upserted", 0)
                    stats["modified_documents"] += batch_stats.get("modified", 0)
                    progress_bar.update(len(batch))
                    batch = []
                    
            # Process remaining documents
            if batch:
                batch_stats = self._process_batch(dest_collection, batch)
                stats["migrated_documents"] += batch_stats["inserted"]
                stats["failed_documents"] += batch_stats["failed"]
                stats["inserted_documents"] += batch_stats.get("raw_inserted", 0)
                stats["upserted_documents"] += batch_stats.get("upserted", 0)
                stats["modified_documents"] += batch_stats.get("modified", 0)
                progress_bar.update(len(batch))
                
            progress_bar.close()
            
            # Log results with detailed statistics
            logger.info(f"Collection migration completed. {stats['migrated_documents']} of {total_documents} documents processed.")
            logger.info(f"Upsert details - Inserted: {stats['inserted_documents']}, "
                       f"Upserted: {stats['upserted_documents']}, Modified: {stats['modified_documents']}")
            
            if stats["failed_documents"] > 0:
                logger.warning(f"Failed to migrate {stats['failed_documents']} documents in {database_name}.{collection_name}")
                
            return stats
            
        except Exception as e:
            logger.error(f"Error migrating collection {database_name}.{collection_name}: {e}")
            logger.debug(traceback.format_exc())
            return {
                "success": False,
                "total_documents": 0,
                "migrated_documents": 0,
                "failed_documents": 0,
                "error": str(e)
            }
            
    def _process_batch(self, dest_collection, batch):
        """Process a batch of documents with retry logic for connection failures.
        
        Args:
            dest_collection: Destination collection
            batch: List of documents to upsert
            
        Returns:
            dict: Batch processing statistics
        """
        def _process_batch_operation():
            stats = {"inserted": 0, "failed": 0, "upserted": 0, "modified": 0}
            
            if not batch:
                return stats
                
            try:
                # Prepare bulk upsert operations
                operations = []
                for document in batch:
                    # Use _id as the filter for upsert operation
                    # If document doesn't have _id, we'll let MongoDB generate one during insert
                    if "_id" in document:
                        filter_doc = {"_id": document["_id"]}
                        # Create ReplaceOne operation with upsert=True
                        operations.append(ReplaceOne(filter_doc, document, upsert=True))
                    else:
                        # For documents without _id, we'll use insert_one to let MongoDB generate the _id
                        # But since we're in a batch operation, we'll add a temporary _id for the filter
                        # and let the upsert create a new document
                        operations.append(ReplaceOne({}, document, upsert=True))
                
                # Execute bulk write operation
                result = dest_collection.bulk_write(operations, ordered=False)
                
                # Update statistics based on the result
                stats["inserted"] = result.inserted_count + result.upserted_count + result.modified_count
                stats["upserted"] = result.upserted_count  
                stats["modified"] = result.modified_count
                
                # Store individual counts for detailed reporting
                stats["raw_inserted"] = result.inserted_count
                
                logger.debug(f"Batch processed: {result.inserted_count} inserted, "
                            f"{result.upserted_count} upserted, {result.modified_count} modified")
                
                return stats
                
            except BulkWriteError as e:
                # Some documents were processed successfully
                result = e.details.get('writeResult', {})
                write_errors = e.details.get('writeErrors', [])
                
                # Get successful operations
                inserted_count = result.get('nInserted', 0)
                upserted_count = result.get('nUpserted', 0)
                modified_count = result.get('nModified', 0)
                
                stats["inserted"] = inserted_count + upserted_count + modified_count
                stats["upserted"] = upserted_count
                stats["modified"] = modified_count
                stats["raw_inserted"] = inserted_count
                stats["failed"] = len(write_errors)
                
                for error in write_errors:
                    logger.error(f"Document upsert error: {error.get('errmsg')} - Document: {error.get('op', {}).get('_id', 'unknown')}")
                    
                return stats
                
            except PyMongoError as e:
                # Failed to process any documents
                logger.error(f"Batch upsert processing error: {e}")
                stats["failed"] = len(batch)
                return stats
        
        # Execute with retry logic if connection manager is available
        try:
            if self.connection_manager:
                return self.connection_manager.execute_with_retry(_process_batch_operation, "destination")
            else:
                return _process_batch_operation()
        except Exception as e:
            logger.error(f"Batch processing failed after retries: {e}")
            return {"inserted": 0, "failed": len(batch), "upserted": 0, "modified": 0}
