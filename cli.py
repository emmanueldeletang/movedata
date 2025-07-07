"""
Command-line interface for Azure Cosmos DB MongoDB Migration Tool.
"""

import logging
import os
import sys
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class CLI:
    """Command-line interface for interactive usage."""
    
    def __init__(self):
        """Initialize the CLI."""
        pass
        
    def run_interactive_mode(self, migration_service):
        """Run the tool in interactive mode.
        
        Args:
            migration_service: MigrationService instance
        """
        print("\n======================================================")
        print("  Azure Cosmos DB MongoDB Migration Tool")
        print("======================================================\n")
        
        while True:
            self._print_main_menu()
            choice = input("Enter your choice (1-4): ").strip()
            
            if choice == "1":
                self._list_databases_menu(migration_service)
            elif choice == "2":
                self._migrate_database_menu(migration_service)
            elif choice == "3":
                self._migrate_collection_menu(migration_service)
            elif choice == "4":
                print("\nExiting the application...")
                break
            else:
                print("\nInvalid choice. Please try again.")
                
    def _print_main_menu(self):
        """Print the main menu options."""
        print("\nMain Menu:")
        print("1. List databases and collections")
        print("2. Migrate entire database")
        print("3. Migrate specific collections")
        print("4. Exit")
        
    def _list_databases_menu(self, migration_service):
        """Display databases and their collections.
        
        Args:
            migration_service: MigrationService instance
        """
        print("\n=== Databases and Collections ===")
        
        databases = migration_service.list_databases()
        if not databases:
            print("No databases found or error occurred.")
            return
            
        for db_name in databases:
            print(f"\nDatabase: {db_name}")
            collections = migration_service.list_collections(db_name)
            
            if not collections:
                print("  No collections found or error occurred.")
                continue
                
            for idx, coll_name in enumerate(collections, 1):
                doc_count = migration_service.count_documents(db_name, coll_name)
                print(f"  {idx}. {coll_name} ({doc_count} documents)")
                
        input("\nPress Enter to continue...")
        
    def _migrate_database_menu(self, migration_service):
        """Menu for migrating entire databases.
        
        Args:
            migration_service: MigrationService instance
        """
        print("\n=== Migrate Entire Database ===")
        
        databases = migration_service.list_databases()
        if not databases:
            print("No databases found or error occurred.")
            return
            
        print("\nAvailable databases:")
        for idx, db_name in enumerate(databases, 1):
            print(f"{idx}. {db_name}")
            
        choice = input("\nEnter database number to migrate (or 'b' to go back): ").strip()
        
        if choice.lower() == 'b':
            return
            
        try:
            db_idx = int(choice) - 1
            if db_idx < 0 or db_idx >= len(databases):
                print("Invalid database number.")
                return
                
            db_name = databases[db_idx]
            
            # Confirm migration
            confirm = input(f"\nConfirm migration of entire database '{db_name}'? (y/n): ").strip().lower()
            if confirm != 'y':
                print("Migration cancelled.")
                return
                
            print(f"\nMigrating database '{db_name}'...")
            start_time = time.time()
            
            stats = migration_service.migrate_database(db_name)
            
            end_time = time.time()
            duration = end_time - start_time
            
            print("\nMigration completed!")
            print(f"Total time: {duration:.2f} seconds")
            print(f"Collections: {stats['successful_collections']} successful, {stats['failed_collections']} failed")
            print(f"Documents: {stats['migrated_documents']} migrated, {stats['failed_documents']} failed")
            
            # Ask if user wants to set up change streams
            change_streams = input("\nWould you like to set up change streams for this database? (y/n): ").strip().lower()
            if change_streams == 'y':
                return db_name  # Return database name for change stream setup
                
        except ValueError:
            print("Invalid input. Please enter a number.")
        except Exception as e:
            logger.error(f"Error in database migration menu: {e}")
            print(f"An error occurred: {e}")
            
        input("\nPress Enter to continue...")
        return None
        
    def _migrate_collection_menu(self, migration_service):
        """Menu for migrating specific collections.
        
        Args:
            migration_service: MigrationService instance
        """
        print("\n=== Migrate Specific Collections ===")
        
        databases = migration_service.list_databases()
        if not databases:
            print("No databases found or error occurred.")
            return
            
        print("\nSelect a database:")
        for idx, db_name in enumerate(databases, 1):
            print(f"{idx}. {db_name}")
            
        db_choice = input("\nEnter database number (or 'b' to go back): ").strip()
        
        if db_choice.lower() == 'b':
            return
            
        try:
            db_idx = int(db_choice) - 1
            if db_idx < 0 or db_idx >= len(databases):
                print("Invalid database number.")
                return
                
            db_name = databases[db_idx]
            collections = migration_service.list_collections(db_name)
            
            if not collections:
                print(f"No collections found in database '{db_name}' or error occurred.")
                input("\nPress Enter to continue...")
                return
                
            print(f"\nCollections in database '{db_name}':")
            for idx, coll_name in enumerate(collections, 1):
                doc_count = migration_service.count_documents(db_name, coll_name)
                print(f"{idx}. {coll_name} ({doc_count} documents)")
                
            coll_choice = input("\nEnter collection numbers to migrate (comma-separated, or 'a' for all): ").strip()
            
            selected_collections = []
            
            if coll_choice.lower() == 'a':
                selected_collections = collections
            elif coll_choice.lower() == 'b':
                return
            else:
                try:
                    indices = [int(idx.strip()) - 1 for idx in coll_choice.split(',')]
                    for idx in indices:
                        if idx >= 0 and idx < len(collections):
                            selected_collections.append(collections[idx])
                        else:
                            print(f"Invalid collection index: {idx + 1}")
                except ValueError:
                    print("Invalid input. Please enter comma-separated numbers.")
                    input("\nPress Enter to continue...")
                    return
                    
            if not selected_collections:
                print("No valid collections selected.")
                input("\nPress Enter to continue...")
                return
                
            # Confirm migration
            print("\nThe following collections will be migrated:")
            for coll_name in selected_collections:
                doc_count = migration_service.count_documents(db_name, coll_name)
                print(f"- {coll_name} ({doc_count} documents)")
                
            confirm = input("\nConfirm migration? (y/n): ").strip().lower()
            if confirm != 'y':
                print("Migration cancelled.")
                input("\nPress Enter to continue...")
                return
                
            # Perform migration
            print("\nMigrating collections...")
            start_time = time.time()
            
            total_stats = {
                "total_collections": len(selected_collections),
                "successful_collections": 0,
                "failed_collections": 0,
                "total_documents": 0,
                "migrated_documents": 0,
                "failed_documents": 0
            }
            
            for coll_name in selected_collections:
                print(f"\nMigrating collection '{coll_name}'...")
                stats = migration_service.migrate_collection(db_name, coll_name)
                
                if stats.get("success", False):
                    total_stats["successful_collections"] += 1
                else:
                    total_stats["failed_collections"] += 1
                    
                total_stats["total_documents"] += stats.get("total_documents", 0)
                total_stats["migrated_documents"] += stats.get("migrated_documents", 0)
                total_stats["failed_documents"] += stats.get("failed_documents", 0)
                
            end_time = time.time()
            duration = end_time - start_time
            
            print("\nMigration completed!")
            print(f"Total time: {duration:.2f} seconds")
            print(f"Collections: {total_stats['successful_collections']} successful, {total_stats['failed_collections']} failed")
            print(f"Documents: {total_stats['migrated_documents']} migrated, {total_stats['failed_documents']} failed")
            
            # Ask if user wants to set up change streams
            change_streams = input("\nWould you like to set up change streams for these collections? (y/n): ").strip().lower()
            if change_streams == 'y':
                return {
                    "database": db_name,
                    "collections": selected_collections
                }
                
        except ValueError:
            print("Invalid input. Please enter a number.")
        except Exception as e:
            logger.error(f"Error in collection migration menu: {e}")
            print(f"An error occurred: {e}")
            
        input("\nPress Enter to continue...")
        return None
        
    def _manage_change_streams_menu(self, migration_service, change_stream_manager):
        """Menu for managing change streams.
        
        Args:
            migration_service: MigrationService instance
            change_stream_manager: ChangeStreamManager instance
        """
        while True:
            print("\n=== Manage Change Streams ===")
            print("1. Start change streams for a database")
            print("2. Start change streams for specific collections")
            print("3. Stop change streams")
            print("4. Back to main menu")
            
            choice = input("\nEnter your choice (1-4): ").strip()
            
            if choice == "1":
                self._start_database_change_streams(migration_service, change_stream_manager)
            elif choice == "2":
                self._start_collection_change_streams(migration_service, change_stream_manager)
            elif choice == "3":
                self._stop_change_streams_menu(change_stream_manager)
            elif choice == "4":
                break
            else:
                print("Invalid choice. Please try again.")
                
    def _start_database_change_streams(self, migration_service, change_stream_manager):
        """Start change streams for an entire database.
        
        Args:
            migration_service: MigrationService instance
            change_stream_manager: ChangeStreamManager instance
        """
        print("\n=== Start Change Streams for Database ===")
        
        databases = migration_service.list_databases()
        if not databases:
            print("No databases found or error occurred.")
            input("\nPress Enter to continue...")
            return
            
        print("\nSelect a database:")
        for idx, db_name in enumerate(databases, 1):
            print(f"{idx}. {db_name}")
            
        choice = input("\nEnter database number (or 'b' to go back): ").strip()
        
        if choice.lower() == 'b':
            return
            
        try:
            db_idx = int(choice) - 1
            if db_idx < 0 or db_idx >= len(databases):
                print("Invalid database number.")
                input("\nPress Enter to continue...")
                return
                
            db_name = databases[db_idx]
            
            # Check eligible collections
            print(f"\nChecking collections in '{db_name}' for change stream support...")
            eligible_collections = change_stream_manager.list_eligible_collections(db_name)
            
            if not eligible_collections:
                print(f"No eligible collections found in database '{db_name}'.")
                input("\nPress Enter to continue...")
                return
                
            print(f"\nThe following collections support change streams:")
            for coll_name in eligible_collections:
                print(f"- {coll_name}")
                
            confirm = input("\nStart change streams for all these collections? (y/n): ").strip().lower()
            if confirm != 'y':
                print("Operation cancelled.")
                input("\nPress Enter to continue...")
                return
                
            # Start change streams
            print(f"\nStarting change streams for database '{db_name}'...")
            results = change_stream_manager.start_database_change_streams(db_name)
            
            success_count = sum(1 for status in results.values() if status)
            failure_count = len(results) - success_count
            
            print(f"\nChange streams started: {success_count} successful, {failure_count} failed")
            
            if failure_count > 0:
                print("\nFailed collections:")
                for coll_name, status in results.items():
                    if not status:
                        print(f"- {coll_name}")
                        
        except ValueError:
            print("Invalid input. Please enter a number.")
        except Exception as e:
            logger.error(f"Error starting database change streams: {e}")
            print(f"An error occurred: {e}")
            
        input("\nPress Enter to continue...")
        
    def _start_collection_change_streams(self, migration_service, change_stream_manager):
        """Start change streams for specific collections.
        
        Args:
            migration_service: MigrationService instance
            change_stream_manager: ChangeStreamManager instance
        """
        print("\n=== Start Change Streams for Collections ===")
        
        databases = migration_service.list_databases()
        if not databases:
            print("No databases found or error occurred.")
            input("\nPress Enter to continue...")
            return
            
        print("\nSelect a database:")
        for idx, db_name in enumerate(databases, 1):
            print(f"{idx}. {db_name}")
            
        db_choice = input("\nEnter database number (or 'b' to go back): ").strip()
        
        if db_choice.lower() == 'b':
            return
            
        try:
            db_idx = int(db_choice) - 1
            if db_idx < 0 or db_idx >= len(databases):
                print("Invalid database number.")
                input("\nPress Enter to continue...")
                return
                
            db_name = databases[db_idx]
            
            # Check eligible collections
            print(f"\nChecking collections in '{db_name}' for change stream support...")
            eligible_collections = change_stream_manager.list_eligible_collections(db_name)
            
            if not eligible_collections:
                print(f"No eligible collections found in database '{db_name}'.")
                input("\nPress Enter to continue...")
                return
                
            print(f"\nCollections in database '{db_name}' that support change streams:")
            for idx, coll_name in enumerate(eligible_collections, 1):
                print(f"{idx}. {coll_name}")
                
            coll_choice = input("\nEnter collection numbers to monitor (comma-separated, or 'a' for all): ").strip()
            
            selected_collections = []
            
            if coll_choice.lower() == 'a':
                selected_collections = eligible_collections
            elif coll_choice.lower() == 'b':
                return
            else:
                try:
                    indices = [int(idx.strip()) - 1 for idx in coll_choice.split(',')]
                    for idx in indices:
                        if idx >= 0 and idx < len(eligible_collections):
                            selected_collections.append(eligible_collections[idx])
                        else:
                            print(f"Invalid collection index: {idx + 1}")
                except ValueError:
                    print("Invalid input. Please enter comma-separated numbers.")
                    input("\nPress Enter to continue...")
                    return
                    
            if not selected_collections:
                print("No valid collections selected.")
                input("\nPress Enter to continue...")
                return
                
            # Confirm operation
            print("\nThe following collections will be monitored with change streams:")
            for coll_name in selected_collections:
                print(f"- {coll_name}")
                
            confirm = input("\nConfirm starting change streams? (y/n): ").strip().lower()
            if confirm != 'y':
                print("Operation cancelled.")
                input("\nPress Enter to continue...")
                return
                
            # Start change streams
            print("\nStarting change streams...")
            results = {}
            
            for coll_name in selected_collections:
                print(f"Starting change stream for {db_name}.{coll_name}...")
                success = change_stream_manager.start_change_stream(db_name, coll_name)
                results[coll_name] = success
                
            success_count = sum(1 for status in results.values() if status)
            failure_count = len(results) - success_count
            
            print(f"\nChange streams started: {success_count} successful, {failure_count} failed")
            
            if failure_count > 0:
                print("\nFailed collections:")
                for coll_name, status in results.items():
                    if not status:
                        print(f"- {coll_name}")
                        
        except ValueError:
            print("Invalid input. Please enter a number.")
        except Exception as e:
            logger.error(f"Error starting collection change streams: {e}")
            print(f"An error occurred: {e}")
            
        input("\nPress Enter to continue...")
        
    def _stop_change_streams_menu(self, change_stream_manager):
        """Menu for stopping change streams.
        
        Args:
            change_stream_manager: ChangeStreamManager instance
        """
        print("\n=== Stop Change Streams ===")
        
        active_streams = change_stream_manager.list_active_streams()
        
        if not active_streams:
            print("No active change streams found.")
            input("\nPress Enter to continue...")
            return
            
        print("\nActive change streams:")
        stream_keys = list(active_streams.keys())
        
        for idx, stream_key in enumerate(stream_keys, 1):
            info = active_streams[stream_key]
            print(f"{idx}. {stream_key} (Status: {info['status']}, Started: {info['started_at']})")
            
        choice = input("\nEnter stream numbers to stop (comma-separated, 'a' for all, or 'b' to go back): ").strip()
        
        if choice.lower() == 'b':
            return
            
        selected_streams = []
        
        if choice.lower() == 'a':
            selected_streams = stream_keys
        else:
            try:
                indices = [int(idx.strip()) - 1 for idx in choice.split(',')]
                for idx in indices:
                    if idx >= 0 and idx < len(stream_keys):
                        selected_streams.append(stream_keys[idx])
                    else:
                        print(f"Invalid stream index: {idx + 1}")
            except ValueError:
                print("Invalid input. Please enter comma-separated numbers.")
                input("\nPress Enter to continue...")
                return
                
        if not selected_streams:
            print("No valid streams selected.")
            input("\nPress Enter to continue...")
            return
            
        # Confirm operation
        print("\nThe following change streams will be stopped:")
        for stream_key in selected_streams:
            print(f"- {stream_key}")
            
        confirm = input("\nConfirm stopping change streams? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Operation cancelled.")
            input("\nPress Enter to continue...")
            return
            
        # Stop change streams
        print("\nStopping change streams...")
        results = {}
        
        for stream_key in selected_streams:
            parts = stream_key.split(".")
            db_name = parts[0]
            coll_name = ".".join(parts[1:])  # Handle collection names with dots
            
            print(f"Stopping change stream for {stream_key}...")
            success = change_stream_manager.stop_change_stream(db_name, coll_name)
            results[stream_key] = success
            
        success_count = sum(1 for status in results.values() if status)
        failure_count = len(results) - success_count
        
        print(f"\nChange streams stopped: {success_count} successful, {failure_count} failed")
        
        if failure_count > 0:
            print("\nFailed to stop:")
            for stream_key, status in results.items():
                if not status:
                    print(f"- {stream_key}")
                    
        input("\nPress Enter to continue...")
        
    def _show_active_change_streams(self, change_stream_manager):
        """Display active change streams.
        
        Args:
            change_stream_manager: ChangeStreamManager instance
        """
        print("\n=== Active Change Streams ===")
        
        active_streams = change_stream_manager.list_active_streams()
        
        if not active_streams:
            print("No active change streams found.")
            input("\nPress Enter to continue...")
            return
            
        print("\nThe following change streams are active:")
        
        for stream_key, info in active_streams.items():
            print(f"\n- {stream_key}")
            print(f"  Status: {info['status']}")
            print(f"  Started: {info['started_at']}")
            
        input("\nPress Enter to continue...")
