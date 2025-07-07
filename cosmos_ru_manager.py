"""
Cosmos DB RU (Request Unit) utilities for collection creation and management.
"""

import logging
from pymongo.errors import PyMongoError, OperationFailure

logger = logging.getLogger(__name__)

class CosmosDBRUManager:
    """Manages Cosmos DB RU-based operations including collection creation with throughput settings."""
    
    def __init__(self, client, config):
        """Initialize the RU manager.
        
        Args:
            client: MongoDB client connected to Cosmos DB
            config: Configuration object containing RU settings
        """
        self.client = client
        self.config = config
        
    def detect_cosmos_db_type(self):
        """Detect if the target is Cosmos DB vCore or RU-based.
        
        Returns:
            str: "vcore", "ru", or "unknown"
        """
        try:
            # Try to run a command that's specific to RU-based Cosmos DB
            db = self.client.get_database("admin")
            result = db.command("serverStatus")
            
            # Check for RU-specific indicators
            if "cosmos" in str(result).lower() or "microsoft" in str(result.get("host", "")).lower():
                # Try to create a test collection with RU settings to confirm RU-based
                try:
                    test_db = self.client.get_database("_temp_detection_test")
                    test_collection = "test_collection"
                    
                    # Try RU-specific collection creation command
                    result = test_db.command({
                        "customAction": "CreateCollection",
                        "collection": test_collection,
                        "offerThroughput": 400
                    })
                    
                    # Clean up test collection
                    try:
                        test_db.drop_collection(test_collection)
                    except:
                        pass
                        
                    return "ru"
                    
                except OperationFailure:
                    # If RU command fails, it might be vCore
                    return "vcore"
                    
            return "unknown"
            
        except Exception as e:
            logger.warning(f"Could not detect Cosmos DB type: {e}")
            return "unknown"
            
    def create_collection_with_throughput(self, database_name, collection_name, 
                                        partition_key=None, throughput_mode="manual", 
                                        manual_ru=400, autoscale_max_ru=4000):
        """Create a collection with specified throughput settings for RU-based Cosmos DB.
        
        Args:
            database_name: Name of the database
            collection_name: Name of the collection
            partition_key: Partition key field (default: "_id")
            throughput_mode: "manual" or "autoscale"
            manual_ru: Manual RU/s setting
            autoscale_max_ru: Maximum RU/s for autoscale
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            db = self.client[database_name]
            
            # Set default partition key
            if not partition_key:
                partition_key = self.config.ru_default_partition_key
                
            # Prepare the collection creation command
            create_command = {
                "customAction": "CreateCollection",
                "collection": collection_name,
                "shardKey": partition_key
            }
            
            # Add throughput settings based on mode
            if throughput_mode.lower() == "autoscale":
                create_command["autoScaleSettings"] = {
                    "maxThroughput": autoscale_max_ru
                }
                logger.info(f"Creating collection with autoscale throughput (max: {autoscale_max_ru} RU/s)")
            else:
                create_command["offerThroughput"] = manual_ru
                logger.info(f"Creating collection with manual throughput ({manual_ru} RU/s)")
                
            logger.info(f"Creating RU collection: {database_name}.{collection_name} with partition key: {partition_key}")
            
            # Execute the collection creation command
            result = db.command(create_command)
            
            if result.get("ok") == 1:
                logger.info(f"✅ Successfully created RU collection: {database_name}.{collection_name}")
                return True
            else:
                logger.error(f"❌ Failed to create RU collection: {result}")
                return False
                
        except OperationFailure as e:
            if "NamespaceExists" in str(e):
                logger.info(f"Collection {database_name}.{collection_name} already exists")
                return True
            else:
                logger.error(f"❌ Error creating RU collection: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Unexpected error creating RU collection: {e}")
            return False
            
    def get_collection_throughput(self, database_name, collection_name):
        """Get the current throughput settings for a collection.
        
        Args:
            database_name: Name of the database
            collection_name: Name of the collection
            
        Returns:
            dict: Throughput information or None if not available
        """
        try:
            db = self.client[database_name]
            
            # Get collection throughput using Cosmos DB command
            result = db.command({
                "customAction": "GetCollection",
                "collection": collection_name
            })
            
            throughput_info = {
                "manual_throughput": result.get("offerThroughput"),
                "autoscale_settings": result.get("autoScaleSettings"),
                "partition_key": result.get("shardKey", "_id")
            }
            
            return throughput_info
            
        except Exception as e:
            logger.warning(f"Could not get throughput info for {database_name}.{collection_name}: {e}")
            return None
            
    def update_collection_throughput(self, database_name, collection_name, 
                                   throughput_mode="manual", manual_ru=400, autoscale_max_ru=4000):
        """Update the throughput settings for an existing collection.
        
        Args:
            database_name: Name of the database
            collection_name: Name of the collection
            throughput_mode: "manual" or "autoscale"
            manual_ru: Manual RU/s setting
            autoscale_max_ru: Maximum RU/s for autoscale
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            db = self.client[database_name]
            
            # Prepare the throughput update command
            update_command = {
                "customAction": "UpdateCollection",
                "collection": collection_name
            }
            
            # Add throughput settings based on mode
            if throughput_mode.lower() == "autoscale":
                update_command["autoScaleSettings"] = {
                    "maxThroughput": autoscale_max_ru
                }
                logger.info(f"Updating collection to autoscale throughput (max: {autoscale_max_ru} RU/s)")
            else:
                update_command["offerThroughput"] = manual_ru
                logger.info(f"Updating collection to manual throughput ({manual_ru} RU/s)")
                
            # Execute the throughput update command
            result = db.command(update_command)
            
            if result.get("ok") == 1:
                logger.info(f"✅ Successfully updated throughput for: {database_name}.{collection_name}")
                return True
            else:
                logger.error(f"❌ Failed to update throughput: {result}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error updating collection throughput: {e}")
            return False
            
    def ensure_collection_exists(self, database_name, collection_name, partition_key=None):
        """Ensure a collection exists, creating it with appropriate settings if it doesn't.
        
        Args:
            database_name: Name of the database
            collection_name: Name of the collection
            partition_key: Partition key field (optional)
            
        Returns:
            bool: True if collection exists or was created successfully
        """
        try:
            db = self.client[database_name]
            
            # Check if collection already exists
            if collection_name in db.list_collection_names():
                logger.info(f"Collection {database_name}.{collection_name} already exists")
                return True
                
            # Create collection with configured settings
            if self.config.target_is_vcore:
                # For vCore, use standard MongoDB collection creation
                db.create_collection(collection_name)
                logger.info(f"✅ Created vCore collection: {database_name}.{collection_name}")
                return True
            else:
                # For RU-based, use custom creation with throughput
                return self.create_collection_with_throughput(
                    database_name, 
                    collection_name,
                    partition_key or self.config.ru_default_partition_key,
                    self.config.ru_throughput_mode,
                    self.config.ru_manual_throughput,
                    self.config.ru_autoscale_max_throughput
                )
                
        except Exception as e:
            logger.error(f"❌ Error ensuring collection exists: {e}")
            return False
            
    def get_recommended_partition_key(self, database_name, collection_name, source_client):
        """Analyze a source collection to recommend a partition key for RU-based Cosmos DB.
        
        Args:
            database_name: Name of the database
            collection_name: Name of the collection
            source_client: MongoDB client for the source database
            
        Returns:
            str: Recommended partition key field
        """
        try:
            source_db = source_client[database_name]
            source_collection = source_db[collection_name]
            
            # Sample a few documents to analyze structure
            sample_docs = list(source_collection.find().limit(100))
            
            if not sample_docs:
                return self.config.ru_default_partition_key
                
            # Analyze field frequency and cardinality
            field_stats = {}
            
            for doc in sample_docs:
                for field in doc.keys():
                    if field not in field_stats:
                        field_stats[field] = {"count": 0, "unique_values": set()}
                    field_stats[field]["count"] += 1
                    
                    # Store value for cardinality analysis (limit to avoid memory issues)
                    if len(field_stats[field]["unique_values"]) < 1000:
                        field_stats[field]["unique_values"].add(str(doc[field]))
                        
            # Score fields based on frequency and cardinality
            scored_fields = []
            
            for field, stats in field_stats.items():
                if field == "_id":
                    continue  # Skip _id unless it's the only option
                    
                frequency_score = stats["count"] / len(sample_docs)
                cardinality_score = len(stats["unique_values"]) / len(sample_docs)
                
                # Prefer fields that appear frequently and have good cardinality
                total_score = frequency_score * 0.6 + cardinality_score * 0.4
                scored_fields.append((field, total_score))
                
            if scored_fields:
                # Sort by score and return the best field
                scored_fields.sort(key=lambda x: x[1], reverse=True)
                recommended_key = scored_fields[0][0]
                logger.info(f"Recommended partition key for {database_name}.{collection_name}: {recommended_key}")
                return recommended_key
            else:
                return self.config.ru_default_partition_key
                
        except Exception as e:
            logger.warning(f"Could not analyze partition key for {database_name}.{collection_name}: {e}")
            return self.config.ru_default_partition_key
