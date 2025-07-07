#!/usr/bin/env python3
"""
Main entry point for the Azure Cosmos DB MongoDB Migration Tool.
"""

import sys
import os
import logging
from src.connection_manager import ConnectionManager
from src.migration_service import MigrationService
from src.config import Config
from src.cli import CLI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("migration.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Main function to run the migration tool."""
    # Show help message if requested
    if len(sys.argv) > 1 and sys.argv[1].lower() in ['--help', '-h', 'help']:
        print("Azure Cosmos DB MongoDB Migration Tool")
        print("=====================================")
        print("")
        print("Usage:")
        print("  python -m src.main           - Run in CLI mode (default)")
        print("  python -m src.main --gui     - Run in GUI mode")
        print("  python gui_launcher.py       - Run GUI directly")
        print("  python -m src.main --help    - Show this help message")
        print("")
        print("Alternatively, use the batch file on Windows:")
        print("  launch_gui.bat               - Launch GUI with dependency check")
        print("")
        return 0
    
    # Check command line arguments for GUI mode
    if len(sys.argv) > 1 and sys.argv[1].lower() in ['--gui', '-g', 'gui']:
        try:
            # Try relative import first (when running as module)
            from .gui import main as gui_main
        except ImportError:
            # Fall back to absolute import (when running directly)
            from gui import main as gui_main
        
        try:
            gui_main()
            return 0
        except ImportError as e:
            logger.error(f"Failed to import GUI module: {e}")
            print("Error: Could not start GUI. Make sure all dependencies are installed.")
            return 1
            
    try:
        # Load configuration
        config = Config()
        config.load_config()

        # Initialize CLI
        cli = CLI()
        
        # Initialize connection manager
        connection_manager = ConnectionManager(config)
        
        # Connect to source and destination
        source_client = connection_manager.connect_to_source()
        if not source_client:
            logger.error("Failed to connect to source. Exiting.")
            return 1
            
        destination_client = connection_manager.connect_to_destination()
        if not destination_client:
            logger.error("Failed to connect to destination. Exiting.")
            return 1
        
        logger.info("Successfully connected to both source and destination.")
        
        # Initialize migration service with connection manager for retry support
        migration_service = MigrationService(
            source_client, 
            destination_client, 
            config, 
            connection_manager=connection_manager
        )
        
        # Run the CLI interface
        cli.run_interactive_mode(migration_service)
        
        return 0
    
    except KeyboardInterrupt:
        logger.info("Migration tool stopped by user.")
        return 0
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
