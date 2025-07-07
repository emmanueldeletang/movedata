"""
Windows GUI for Azure Cosmos DB MongoDB Migration Tool.
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import threading
import queue
import logging
import json
import os
from datetime import datetime
from typing import Optional, Dict, Any, List
import sys

# Import the existing components
try:
    # Try relative imports first (when running as module)
    from .connection_manager import ConnectionManager
    from .migration_service import MigrationService
    from .postgresql_connection_manager import PostgreSQLConnectionManager
    from .postgresql_migration_service import PostgreSQLMigrationService
    from .config import Config
except ImportError:
    # Fall back to absolute imports (when running directly)
    from connection_manager import ConnectionManager
    from migration_service import MigrationService
    from postgresql_connection_manager import PostgreSQLConnectionManager
    from postgresql_migration_service import PostgreSQLMigrationService
    from config import Config

logger = logging.getLogger(__name__)

class CosmosDBMigrationGUI:
    """Modern Windows GUI for Cosmos DB MongoDB Migration Tool."""
    
    def __init__(self):
        """Initialize the GUI application."""
        self.root = tk.Tk()
        self.root.title("Azure Cosmos DB MongoDB Migration Tool")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 600)
        
        # Configure modern styling
        self.setup_styles()
        
        # Initialize application state
        self.config = None
        
        # MongoDB components
        self.connection_manager = None
        self.migration_service = None
        self.source_client = None
        self.destination_client = None
        
        # PostgreSQL components
        self.pg_connection_manager = None
        self.pg_migration_service = None
        self.pg_source_pool = None
        self.pg_dest_pool = None
        
        # GUI state
        self.connected = False
        self.pg_connected = False
        self.databases = []
        self.pg_databases = []
        self.migration_stats = {}
        self._migration_stopped = False
        
        # Threading for background operations
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        
        # Create the main interface
        self.create_widgets()
        self.setup_logging_handler()
        
        # Start the result processor
        self.process_results()
        
    def setup_styles(self):
        """Configure modern styling for the application."""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure colors and fonts
        style.configure('Title.TLabel', font=('Segoe UI', 16, 'bold'), foreground='#2E86AB')
        style.configure('Header.TLabel', font=('Segoe UI', 12, 'bold'), foreground='#1F4E79')
        style.configure('Status.TLabel', font=('Segoe UI', 9), foreground='#666666')
        style.configure('Success.TLabel', font=('Segoe UI', 9), foreground='#28A745')
        style.configure('Error.TLabel', font=('Segoe UI', 9), foreground='#DC3545')
        style.configure('Warning.TLabel', font=('Segoe UI', 9), foreground='#FFC107')
        
        # Button styles
        style.configure('Primary.TButton', font=('Segoe UI', 10))
        style.configure('Success.TButton', font=('Segoe UI', 10))
        style.configure('Danger.TButton', font=('Segoe UI', 10))
        
        # Configure root window
        self.root.configure(bg='#F8F9FA')
        
    def create_widgets(self):
        """Create and layout all GUI widgets."""
        # Main container with padding
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill='both', expand=True)
        
        # Title
        title_label = ttk.Label(main_frame, text="Azure Cosmos DB MongoDB Migration Tool", 
                               style='Title.TLabel')
        title_label.pack(pady=(0, 20))
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill='both', expand=True)
        
        # Create tabs
        self.create_connection_tab()
        self.create_migration_tab()
        self.create_postgresql_tab()
        self.create_logs_tab()
        
        # Status bar
        self.create_status_bar(main_frame)
        
    def create_connection_tab(self):
        """Create the connection configuration tab."""
        conn_frame = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(conn_frame, text="Connection")
        
        # Connection settings section
        conn_settings_frame = ttk.LabelFrame(conn_frame, text="Connection Settings", padding="15")
        conn_settings_frame.pack(fill='x', pady=(0, 20))
        
        # Source connection
        ttk.Label(conn_settings_frame, text="Source Connection String:", 
                 style='Header.TLabel').grid(row=0, column=0, sticky='w', pady=(0, 5))
        self.source_conn_var = tk.StringVar()
        source_entry = ttk.Entry(conn_settings_frame, textvariable=self.source_conn_var, 
                                width=80, show='*')
        source_entry.grid(row=1, column=0, columnspan=2, sticky='ew', pady=(0, 10))
        
        # Destination connection
        ttk.Label(conn_settings_frame, text="Destination Connection String:", 
                 style='Header.TLabel').grid(row=2, column=0, sticky='w', pady=(0, 5))
        self.dest_conn_var = tk.StringVar()
        dest_entry = ttk.Entry(conn_settings_frame, textvariable=self.dest_conn_var, 
                              width=80, show='*')
        dest_entry.grid(row=3, column=0, columnspan=2, sticky='ew', pady=(0, 15))
        
        # Connection buttons
        button_frame = ttk.Frame(conn_settings_frame)
        button_frame.grid(row=4, column=0, columnspan=2, sticky='w')
        
        self.connect_btn = ttk.Button(button_frame, text="Connect", 
                                     command=self.connect_to_databases, style='Primary.TButton')
        self.connect_btn.pack(side='left', padx=(0, 10))
        
        self.test_conn_btn = ttk.Button(button_frame, text="Test Connection", 
                                       command=self.test_connections)
        self.test_conn_btn.pack(side='left', padx=(0, 10))
        
        self.load_config_btn = ttk.Button(button_frame, text="Load Config", 
                                         command=self.load_config_file)
        self.load_config_btn.pack(side='left')
        
        # Configure grid weights
        conn_settings_frame.columnconfigure(0, weight=1)
        
        # Target database configuration
        target_config_frame = ttk.LabelFrame(conn_frame, text="Target Database Configuration", padding="15")
        target_config_frame.pack(fill='x', pady=(0, 20))
        
        # Database type selection
        ttk.Label(target_config_frame, text="Target Database Type:", 
                 style='Header.TLabel').grid(row=0, column=0, sticky='w', pady=(0, 10))
        
        self.target_type_var = tk.StringVar(value="vcore")
        type_frame = ttk.Frame(target_config_frame)
        type_frame.grid(row=1, column=0, sticky='w', pady=(0, 15))
        
        self.vcore_radio = ttk.Radiobutton(type_frame, text="Cosmos DB for MongoDB (vCore)", 
                                          variable=self.target_type_var, value="vcore",
                                          command=self.on_target_type_changed)
        self.vcore_radio.pack(side='left', padx=(0, 20))
        
        self.ru_radio = ttk.Radiobutton(type_frame, text="Cosmos DB for MongoDB (RU)", 
                                       variable=self.target_type_var, value="ru",
                                       command=self.on_target_type_changed)
        self.ru_radio.pack(side='left')
        
        # RU Configuration (initially hidden)
        self.ru_config_frame = ttk.LabelFrame(target_config_frame, text="RU Configuration", padding="10")
        self.ru_config_frame.grid(row=2, column=0, sticky='ew', pady=(10, 0))
        
        # Throughput mode
        ttk.Label(self.ru_config_frame, text="Throughput Mode:").grid(row=0, column=0, sticky='w')
        self.throughput_mode_var = tk.StringVar(value="manual")
        
        throughput_frame = ttk.Frame(self.ru_config_frame)
        throughput_frame.grid(row=0, column=1, sticky='w', padx=(10, 0))
        
        ttk.Radiobutton(throughput_frame, text="Manual", variable=self.throughput_mode_var, 
                       value="manual", command=self.on_throughput_mode_changed).pack(side='left', padx=(0, 15))
        ttk.Radiobutton(throughput_frame, text="Autoscale", variable=self.throughput_mode_var, 
                       value="autoscale", command=self.on_throughput_mode_changed).pack(side='left')
        
        # Manual throughput settings
        self.manual_frame = ttk.Frame(self.ru_config_frame)
        self.manual_frame.grid(row=1, column=0, columnspan=2, sticky='ew', pady=(10, 0))
        
        ttk.Label(self.manual_frame, text="Manual RU/s:").grid(row=0, column=0, sticky='w')
        self.manual_ru_var = tk.StringVar(value="400")
        manual_ru_entry = ttk.Entry(self.manual_frame, textvariable=self.manual_ru_var, width=10)
        manual_ru_entry.grid(row=0, column=1, padx=(10, 0), sticky='w')
        ttk.Label(self.manual_frame, text="(Min: 400, increments of 100)").grid(row=0, column=2, padx=(5, 0), sticky='w')
        
        # Autoscale throughput settings
        self.autoscale_frame = ttk.Frame(self.ru_config_frame)
        self.autoscale_frame.grid(row=2, column=0, columnspan=2, sticky='ew', pady=(10, 0))
        
        ttk.Label(self.autoscale_frame, text="Max Autoscale RU/s:").grid(row=0, column=0, sticky='w')
        self.autoscale_ru_var = tk.StringVar(value="4000")
        autoscale_ru_entry = ttk.Entry(self.autoscale_frame, textvariable=self.autoscale_ru_var, width=10)
        autoscale_ru_entry.grid(row=0, column=1, padx=(10, 0), sticky='w')
        ttk.Label(self.autoscale_frame, text="(Min: 1000, increments of 1000)").grid(row=0, column=2, padx=(5, 0), sticky='w')
        
        # Default partition key
        ttk.Label(self.ru_config_frame, text="Default Partition Key:").grid(row=3, column=0, sticky='w', pady=(10, 0))
        self.partition_key_var = tk.StringVar(value="_id")
        partition_key_entry = ttk.Entry(self.ru_config_frame, textvariable=self.partition_key_var, width=20)
        partition_key_entry.grid(row=3, column=1, padx=(10, 0), sticky='w', pady=(10, 0))
        
        # Configure grid weights for target config
        target_config_frame.columnconfigure(0, weight=1)
        self.ru_config_frame.columnconfigure(1, weight=1)
        
        # Initially hide RU config
        self.on_target_type_changed()
        self.on_throughput_mode_changed()
        
        # Connection status section
        status_frame = ttk.LabelFrame(conn_frame, text="Connection Status", padding="15")
        status_frame.pack(fill='x', pady=(0, 20))
        
        self.connection_status_var = tk.StringVar(value="Not connected")
        self.connection_status_label = ttk.Label(status_frame, textvariable=self.connection_status_var,
                                                style='Status.TLabel')
        self.connection_status_label.pack(anchor='w')
        
        # Database list section
        db_frame = ttk.LabelFrame(conn_frame, text="Available Databases", padding="15")
        db_frame.pack(fill='both', expand=True)
        
        # Database tree view
        columns = ('Database', 'Collections', 'Documents')
        self.db_tree = ttk.Treeview(db_frame, columns=columns, show='tree headings', height=8)
        
        self.db_tree.heading('#0', text='Name')
        self.db_tree.heading('Database', text='Database')
        self.db_tree.heading('Collections', text='Collections')
        self.db_tree.heading('Documents', text='Documents')
        
        self.db_tree.column('#0', width=200)
        self.db_tree.column('Database', width=150)
        self.db_tree.column('Collections', width=100)
        self.db_tree.column('Documents', width=120)
        
        # Scrollbar for tree view
        db_scrollbar = ttk.Scrollbar(db_frame, orient='vertical', command=self.db_tree.yview)
        self.db_tree.configure(yscrollcommand=db_scrollbar.set)
        
        self.db_tree.pack(side='left', fill='both', expand=True)
        db_scrollbar.pack(side='right', fill='y')
        
        # Refresh button
        ttk.Button(db_frame, text="Refresh Database List", 
                  command=self.refresh_database_list).pack(pady=(10, 0))
        
    def create_migration_tab(self):
        """Create the migration operations tab."""
        migration_frame = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(migration_frame, text="Migration")
        
        # Migration options section
        options_frame = ttk.LabelFrame(migration_frame, text="Migration Options", padding="15")
        options_frame.pack(fill='x', pady=(0, 20))
        
        # Migration type selection
        ttk.Label(options_frame, text="Migration Type:", style='Header.TLabel').grid(row=0, column=0, sticky='w')
        self.migration_type_var = tk.StringVar(value="database")
        
        type_frame = ttk.Frame(options_frame)
        type_frame.grid(row=1, column=0, sticky='w', pady=(5, 15))
        
        ttk.Radiobutton(type_frame, text="Entire Database", variable=self.migration_type_var, 
                       value="database", command=self.on_migration_type_changed).pack(side='left', padx=(0, 20))
        ttk.Radiobutton(type_frame, text="Specific Collections", variable=self.migration_type_var, 
                       value="collections", command=self.on_migration_type_changed).pack(side='left', padx=(0, 20))
        ttk.Radiobutton(type_frame, text="All Databases", variable=self.migration_type_var, 
                       value="all", command=self.on_migration_type_changed).pack(side='left')
        
        # Database/Collection selection
        selection_frame = ttk.Frame(options_frame)
        selection_frame.grid(row=2, column=0, sticky='ew', pady=(0, 15))
        
        ttk.Label(selection_frame, text="Select Database:").grid(row=0, column=0, sticky='w')
        self.selected_db_var = tk.StringVar()
        self.db_combo = ttk.Combobox(selection_frame, textvariable=self.selected_db_var, 
                                    state='readonly', width=30)
        self.db_combo.grid(row=0, column=1, padx=(10, 0), sticky='w')
        self.db_combo.bind('<<ComboboxSelected>>', self.on_database_selected)
        
        # Container/Collection selection with enhanced UI
        ttk.Label(selection_frame, text="Select Collections:", style='Header.TLabel').grid(row=1, column=0, sticky='nw', pady=(15, 5))
        
        # Collections frame with tree view for better display
        coll_frame = ttk.LabelFrame(selection_frame, text="Available Collections", padding="10")
        coll_frame.grid(row=2, column=0, columnspan=2, sticky='ew', pady=(5, 0))
        
        # Collections tree view with checkboxes simulation
        columns = ('Selected', 'Collection', 'Documents', 'Size')
        self.collections_tree = ttk.Treeview(coll_frame, columns=columns, show='tree headings', height=8)
        
        self.collections_tree.heading('#0', text='Select')
        self.collections_tree.heading('Selected', text='✓')
        self.collections_tree.heading('Collection', text='Collection Name')
        self.collections_tree.heading('Documents', text='Document Count')
        self.collections_tree.heading('Size', text='Size (MB)')
        
        self.collections_tree.column('#0', width=60)
        self.collections_tree.column('Selected', width=40)
        self.collections_tree.column('Collection', width=200)
        self.collections_tree.column('Documents', width=120)
        self.collections_tree.column('Size', width=100)
        
        # Scrollbar for collections tree
        coll_scrollbar = ttk.Scrollbar(coll_frame, orient='vertical', command=self.collections_tree.yview)
        self.collections_tree.configure(yscrollcommand=coll_scrollbar.set)
        
        self.collections_tree.pack(side='left', fill='both', expand=True)
        coll_scrollbar.pack(side='right', fill='y')
        
        # Bind click event for collection selection
        self.collections_tree.bind('<Button-1>', self.on_collection_click)
        self.collections_tree.bind('<space>', self.on_collection_space)
        
        # Collection selection controls
        coll_controls_frame = ttk.Frame(coll_frame)
        coll_controls_frame.pack(fill='x', pady=(10, 0))
        
        ttk.Button(coll_controls_frame, text="Select All", 
                  command=self.select_all_collections).pack(side='left', padx=(0, 10))
        ttk.Button(coll_controls_frame, text="Select None", 
                  command=self.select_no_collections).pack(side='left', padx=(0, 10))
        ttk.Button(coll_controls_frame, text="Refresh Collections", 
                  command=self.refresh_collections).pack(side='left')
        
        # Selected collections summary
        self.selected_summary_var = tk.StringVar(value="No collections selected")
        ttk.Label(coll_controls_frame, textvariable=self.selected_summary_var, 
                 style='Status.TLabel').pack(side='right')
        
        # Migration settings
        settings_frame = ttk.Frame(options_frame)
        settings_frame.grid(row=3, column=0, sticky='ew', pady=(15, 0))
        
        ttk.Label(settings_frame, text="Batch Size:").grid(row=0, column=0, sticky='w')
        self.batch_size_var = tk.StringVar(value="1000")
        ttk.Entry(settings_frame, textvariable=self.batch_size_var, width=10).grid(row=0, column=1, padx=(10, 20), sticky='w')
        
        ttk.Label(settings_frame, text="Parallel Collections:").grid(row=0, column=2, sticky='w')
        self.parallel_collections_var = tk.StringVar(value="1")
        ttk.Entry(settings_frame, textvariable=self.parallel_collections_var, width=10).grid(row=0, column=3, padx=(10, 0), sticky='w')
        
        # Configure grid weights
        options_frame.columnconfigure(0, weight=1)
        selection_frame.columnconfigure(1, weight=1)
        coll_frame.columnconfigure(0, weight=1)
        
        # Migration controls
        controls_frame = ttk.LabelFrame(migration_frame, text="Migration Controls", padding="15")
        controls_frame.pack(fill='x', pady=(0, 20))
        
        self.migrate_btn = ttk.Button(controls_frame, text="Start Migration", 
                                     command=self.start_migration, style='Success.TButton', 
                                     state='disabled')
        self.migrate_btn.pack(side='left', padx=(0, 10))
        
        self.stop_migration_btn = ttk.Button(controls_frame, text="Stop Migration", 
                                           command=self.stop_migration, style='Danger.TButton', 
                                           state='disabled')
        self.stop_migration_btn.pack(side='left', padx=(0, 10))
        
        self.pause_migration_btn = ttk.Button(controls_frame, text="Pause Migration", 
                                             command=self.pause_migration, style='Warning.TButton', 
                                             state='disabled')
        self.pause_migration_btn.pack(side='left')
        
        # Create a horizontal paned window for progress and logs
        paned_window = ttk.PanedWindow(migration_frame, orient='horizontal')
        paned_window.pack(fill='both', expand=True)
        
        # Migration progress (left panel)
        progress_frame = ttk.LabelFrame(paned_window, text="Migration Progress", padding="15")
        paned_window.add(progress_frame, weight=1)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, 
                                          maximum=100, length=300)
        self.progress_bar.pack(pady=(0, 10))
        
        self.progress_label_var = tk.StringVar(value="Ready to migrate")
        ttk.Label(progress_frame, textvariable=self.progress_label_var).pack()
        
        # Migration statistics
        stats_frame = ttk.Frame(progress_frame)
        stats_frame.pack(fill='x', pady=(20, 0))
        
        # Stats labels
        self.stats_vars = {
            'collections': tk.StringVar(value="Collections: 0 / 0"),
            'documents': tk.StringVar(value="Documents: 0 / 0"),
            'elapsed_time': tk.StringVar(value="Elapsed Time: 00:00:00"),
            'rate': tk.StringVar(value="Rate: 0 docs/sec")
        }
        
        for i, (key, var) in enumerate(self.stats_vars.items()):
            ttk.Label(stats_frame, textvariable=var, style='Status.TLabel').grid(row=i//2, column=i%2, 
                                                                                sticky='w', padx=(0, 20), pady=2)
        
        # Migration logs (right panel)
        logs_frame = ttk.LabelFrame(paned_window, text="Migration Logs", padding="15")
        paned_window.add(logs_frame, weight=1)
        
        # Log controls for migration tab
        log_controls_frame = ttk.Frame(logs_frame)
        log_controls_frame.pack(fill='x', pady=(0, 10))
        
        ttk.Button(log_controls_frame, text="Clear Migration Logs", 
                  command=self.clear_migration_logs).pack(side='left', padx=(0, 10))
        ttk.Button(log_controls_frame, text="Save Migration Logs", 
                  command=self.save_migration_logs).pack(side='left', padx=(0, 10))
        
        # Auto-scroll checkbox
        self.auto_scroll_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(log_controls_frame, text="Auto-scroll", 
                       variable=self.auto_scroll_var).pack(side='left', padx=(20, 0))
        
        # Migration log output
        self.migration_log_text = scrolledtext.ScrolledText(logs_frame, height=20, wrap='word')
        self.migration_log_text.pack(fill='both', expand=True)
        
        # Configure text tags for colored output
        self.migration_log_text.tag_configure("success", foreground="#28A745")
        self.migration_log_text.tag_configure("error", foreground="#DC3545")
        self.migration_log_text.tag_configure("warning", foreground="#FFC107")
        self.migration_log_text.tag_configure("info", foreground="#17A2B8")
        
        # Track selected collections
        self.selected_collections = set()
        self.collection_data = {}  # Store collection metadata
    
    def create_postgresql_tab(self):
        """Create the PostgreSQL migration tab."""
        pg_frame = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(pg_frame, text="PostgreSQL Migration")
        
        # Connection section
        conn_frame = ttk.LabelFrame(pg_frame, text="PostgreSQL Connections", padding="15")
        conn_frame.pack(fill='x', pady=(0, 20))
        
        # Source connection
        ttk.Label(conn_frame, text="Source PostgreSQL Connection:", 
                 style='Header.TLabel').grid(row=0, column=0, columnspan=4, sticky='w', pady=(0, 10))
        
        # Source connection fields
        ttk.Label(conn_frame, text="Server URL:").grid(row=1, column=0, sticky='w', padx=(0, 5))
        self.pg_source_server_var = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.pg_source_server_var, width=25).grid(row=1, column=1, sticky='ew', padx=(0, 10))
        
        ttk.Label(conn_frame, text="Database:").grid(row=1, column=2, sticky='w', padx=(0, 5))
        self.pg_source_db_var = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.pg_source_db_var, width=20).grid(row=1, column=3, sticky='ew')
        
        ttk.Label(conn_frame, text="Username:").grid(row=2, column=0, sticky='w', padx=(0, 5))
        self.pg_source_user_var = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.pg_source_user_var, width=25).grid(row=2, column=1, sticky='ew', padx=(0, 10))
        
        ttk.Label(conn_frame, text="Password:").grid(row=2, column=2, sticky='w', padx=(0, 5))
        self.pg_source_pass_var = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.pg_source_pass_var, width=20, show='*').grid(row=2, column=3, sticky='ew')
        
        # Destination connection
        ttk.Label(conn_frame, text="Destination PostgreSQL Connection:", 
                 style='Header.TLabel').grid(row=3, column=0, columnspan=4, sticky='w', pady=(20, 10))
        
        # Destination connection fields
        ttk.Label(conn_frame, text="Server URL:").grid(row=4, column=0, sticky='w', padx=(0, 5))
        self.pg_dest_server_var = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.pg_dest_server_var, width=25).grid(row=4, column=1, sticky='ew', padx=(0, 10))
        
        ttk.Label(conn_frame, text="Database:").grid(row=4, column=2, sticky='w', padx=(0, 5))
        self.pg_dest_db_var = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.pg_dest_db_var, width=20).grid(row=4, column=3, sticky='ew')
        
        ttk.Label(conn_frame, text="Username:").grid(row=5, column=0, sticky='w', padx=(0, 5))
        self.pg_dest_user_var = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.pg_dest_user_var, width=25).grid(row=5, column=1, sticky='ew', padx=(0, 10))
        
        ttk.Label(conn_frame, text="Password:").grid(row=5, column=2, sticky='w', padx=(0, 5))
        self.pg_dest_pass_var = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.pg_dest_pass_var, width=20, show='*').grid(row=5, column=3, sticky='ew')
        
        # Connection buttons
        btn_frame = ttk.Frame(conn_frame)
        btn_frame.grid(row=6, column=0, columnspan=4, pady=(20, 10))
        
        ttk.Button(btn_frame, text="Test Connections", 
                  command=self.test_postgresql_connections).pack(side='left', padx=(0, 10))
        ttk.Button(btn_frame, text="Connect", 
                  command=self.connect_to_postgresql).pack(side='left', padx=(0, 10))
        ttk.Button(btn_frame, text="Refresh Schema", 
                  command=self.refresh_postgresql_schemas).pack(side='left')
        
        # Configure grid weights
        conn_frame.columnconfigure(1, weight=1)
        conn_frame.columnconfigure(3, weight=1)
        
        # Schema selection section
        schema_frame = ttk.LabelFrame(pg_frame, text="Schema and Table Selection", padding="15")
        schema_frame.pack(fill='both', expand=True, pady=(0, 20))
        
        # Schema tree
        tree_frame = ttk.Frame(schema_frame)
        tree_frame.pack(fill='both', expand=True)
        
        self.pg_schema_tree = ttk.Treeview(tree_frame, columns=('Type', 'Count'), show='tree headings')
        self.pg_schema_tree.heading('#0', text='Schema/Table')
        self.pg_schema_tree.heading('Type', text='Type')
        self.pg_schema_tree.heading('Count', text='Records')
        
        # Tree scrollbars
        pg_v_scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.pg_schema_tree.yview)
        pg_h_scrollbar = ttk.Scrollbar(tree_frame, orient='horizontal', command=self.pg_schema_tree.xview)
        self.pg_schema_tree.configure(yscrollcommand=pg_v_scrollbar.set, xscrollcommand=pg_h_scrollbar.set)
        
        self.pg_schema_tree.pack(side='left', fill='both', expand=True)
        pg_v_scrollbar.pack(side='right', fill='y')
        pg_h_scrollbar.pack(side='bottom', fill='x')
        
        # Migration options
        pg_options_frame = ttk.LabelFrame(pg_frame, text="PostgreSQL Migration Options", padding="15")
        pg_options_frame.pack(fill='x', pady=(0, 20))
        
        # Migration type
        ttk.Label(pg_options_frame, text="Migration Type:", style='Header.TLabel').grid(row=0, column=0, sticky='w')
        self.pg_migration_type_var = tk.StringVar(value="schema_and_data")
        
        type_frame = ttk.Frame(pg_options_frame)
        type_frame.grid(row=1, column=0, sticky='w', pady=(5, 15))
        
        ttk.Radiobutton(type_frame, text="Schema Only", variable=self.pg_migration_type_var, 
                       value="schema_only").pack(side='left', padx=(0, 20))
        ttk.Radiobutton(type_frame, text="Data Only", variable=self.pg_migration_type_var, 
                       value="data_only").pack(side='left', padx=(0, 20))
        ttk.Radiobutton(type_frame, text="Schema + Data", variable=self.pg_migration_type_var, 
                       value="schema_and_data").pack(side='left')
        
        # Migration scope
        ttk.Label(pg_options_frame, text="Migration Scope:", style='Header.TLabel').grid(row=2, column=0, sticky='w', pady=(15, 5))
        self.pg_migration_scope_var = tk.StringVar(value="all_schemas")
        
        scope_frame = ttk.Frame(pg_options_frame)
        scope_frame.grid(row=3, column=0, sticky='w', pady=(5, 15))
        
        ttk.Radiobutton(scope_frame, text="All Schemas", variable=self.pg_migration_scope_var, 
                       value="all_schemas").pack(side='left', padx=(0, 20))
        ttk.Radiobutton(scope_frame, text="Selected Schema", variable=self.pg_migration_scope_var, 
                       value="selected_schema").pack(side='left', padx=(0, 20))
        ttk.Radiobutton(scope_frame, text="Selected Tables", variable=self.pg_migration_scope_var, 
                       value="selected_tables").pack(side='left')
        
        # Batch size
        ttk.Label(pg_options_frame, text="Batch Size:").grid(row=4, column=0, sticky='w', pady=(15, 5))
        self.pg_batch_size_var = tk.StringVar(value="1000")
        ttk.Entry(pg_options_frame, textvariable=self.pg_batch_size_var, width=10).grid(row=5, column=0, sticky='w')
        
        # Configure grid weights
        pg_options_frame.columnconfigure(0, weight=1)
        
        # Migration controls
        pg_controls_frame = ttk.LabelFrame(pg_frame, text="PostgreSQL Migration Controls", padding="15")
        pg_controls_frame.pack(fill='x', pady=(0, 20))
        
        self.pg_migrate_btn = ttk.Button(pg_controls_frame, text="Start PostgreSQL Migration", 
                                        command=self.start_postgresql_migration, style='Success.TButton', 
                                        state='disabled')
        self.pg_migrate_btn.pack(side='left', padx=(0, 10))
        
        self.pg_stop_migration_btn = ttk.Button(pg_controls_frame, text="Stop PostgreSQL Migration", 
                                               command=self.stop_postgresql_migration, style='Danger.TButton', 
                                               state='disabled')
        self.pg_stop_migration_btn.pack(side='left')
        
        # Migration progress
        pg_progress_frame = ttk.LabelFrame(pg_frame, text="PostgreSQL Migration Progress", padding="15")
        pg_progress_frame.pack(fill='x')
        
        self.pg_progress_var = tk.DoubleVar()
        self.pg_progress_bar = ttk.Progressbar(pg_progress_frame, variable=self.pg_progress_var, 
                                              maximum=100, length=400)
        self.pg_progress_bar.pack(pady=(0, 10))
        
        self.pg_progress_label_var = tk.StringVar(value="Ready to migrate PostgreSQL")
        ttk.Label(pg_progress_frame, textvariable=self.pg_progress_label_var).pack()
        
        # PostgreSQL Migration statistics
        pg_stats_frame = ttk.Frame(pg_progress_frame)
        pg_stats_frame.pack(fill='x', pady=(20, 0))
        
        # Stats labels
        self.pg_stats_vars = {
            'schemas': tk.StringVar(value="Schemas: 0 / 0"),
            'tables': tk.StringVar(value="Tables: 0 / 0"),
            'records': tk.StringVar(value="Records: 0 / 0"),
            'elapsed_time': tk.StringVar(value="Elapsed Time: 00:00:00"),
            'rate': tk.StringVar(value="Rate: 0 records/sec")
        }
        
        for i, (key, var) in enumerate(self.pg_stats_vars.items()):
            ttk.Label(pg_stats_frame, textvariable=var, style='Status.TLabel').grid(row=i//2, column=i%2, 
                                                                                   sticky='w', padx=(0, 30))
    
    def create_logs_tab(self):
        """Create the logs and output tab."""
        logs_frame = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(logs_frame, text="Logs")
        
        # Log controls
        controls_frame = ttk.Frame(logs_frame)
        controls_frame.pack(fill='x', pady=(0, 10))
        
        ttk.Button(controls_frame, text="Clear Logs", command=self.clear_logs).pack(side='left', padx=(0, 10))
        ttk.Button(controls_frame, text="Save Logs", command=self.save_logs).pack(side='left', padx=(0, 10))
        
        # Log level selection
        ttk.Label(controls_frame, text="Log Level:").pack(side='left', padx=(20, 5))
        self.log_level_var = tk.StringVar(value="INFO")
        log_level_combo = ttk.Combobox(controls_frame, textvariable=self.log_level_var, 
                                      values=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                                      state='readonly', width=10)
        log_level_combo.pack(side='left')
        log_level_combo.bind('<<ComboboxSelected>>', self.change_log_level)
        
        # Log output
        self.log_text = scrolledtext.ScrolledText(logs_frame, height=25, wrap='word')
        self.log_text.pack(fill='both', expand=True)
        
    def create_status_bar(self, parent):
        """Create the status bar at the bottom."""
        self.status_frame = ttk.Frame(parent)
        self.status_frame.pack(fill='x', pady=(10, 0))
        
        self.status_var = tk.StringVar(value="Ready")
        self.status_label = ttk.Label(self.status_frame, textvariable=self.status_var, 
                                     style='Status.TLabel')
        self.status_label.pack(side='left')
        
        # Connection indicator
        self.conn_indicator = tk.Label(self.status_frame, text="●", fg="red", font=('Arial', 12))
        self.conn_indicator.pack(side='right', padx=(0, 10))
        
        ttk.Label(self.status_frame, text="Connection:", style='Status.TLabel').pack(side='right')
        
    def setup_logging_handler(self):
        """Setup logging handler to display logs in the GUI."""
        class GUILogHandler(logging.Handler):
            def __init__(self, text_widget, queue_obj):
                super().__init__()
                self.text_widget = text_widget
                self.queue = queue_obj
                
            def emit(self, record):
                msg = self.format(record)
                self.queue.put(('log', msg))
        
        # Add GUI log handler
        gui_handler = GUILogHandler(self.log_text, self.result_queue)
        gui_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(gui_handler)
        
    def process_results(self):
        """Process results from background tasks."""
        try:
            while True:
                try:
                    result_type, data = self.result_queue.get_nowait()
                    
                    if result_type == 'log':
                        self.log_text.insert('end', data + '\n')
                        self.log_text.see('end')
                    elif result_type == 'connection_status':
                        self.update_connection_status(data)
                    elif result_type == 'database_list':
                        self.update_database_list(data)
                    elif result_type == 'migration_progress':
                        self.update_migration_progress(data)

                    elif result_type == 'error':
                        messagebox.showerror("Error", data)
                    elif result_type == 'info':
                        messagebox.showinfo("Information", data)
                    elif result_type == 'pg_test_success':
                        messagebox.showinfo("Connection Test", data)
                    elif result_type == 'pg_test_error':
                        messagebox.showerror("Connection Test Failed", data)
                    elif result_type == 'pg_connect_success':
                        messagebox.showinfo("Connection Success", data)
                        # Enable PostgreSQL controls
                        if hasattr(self, 'pg_migrate_btn'):
                            self.pg_migrate_btn.config(state='normal')
                        self.log_message("PostgreSQL connections ready for migration", "SUCCESS")
                    elif result_type == 'pg_connect_error':
                        messagebox.showerror("Connection Failed", data)
                        self.pg_connected = False
                    elif result_type == 'pg_connection_status':
                        self.update_pg_connection_status(data)
                    elif result_type == 'pg_schema_list':
                        self.update_pg_schema_list(data)
                    elif result_type == 'pg_migration_start':
                        self.start_pg_migration(data)
                    elif result_type == 'pg_current_operation':
                        self.update_pg_current_operation(data)
                    elif result_type == 'pg_schema_migrated' or result_type == 'pg_table_migrated':
                        self.complete_pg_migration(data)
                    elif result_type == 'pg_migration_complete':
                        self.finish_pg_migration(data)
                        
                except queue.Empty:
                    break
        except Exception as e:
            logger.error(f"Error processing results: {e}")
        finally:
            # Schedule next check
            self.root.after(100, self.process_results)
            
    def run_in_background(self, func, *args, **kwargs):
        """Run a function in a background thread."""
        def wrapper():
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                self.result_queue.put(('error', str(e)))
                logger.exception(f"Background task error: {e}")
                
        thread = threading.Thread(target=wrapper, daemon=True)
        thread.start()
        
    def load_config_file(self):
        """Load configuration from a file."""
        try:
            file_path = filedialog.askopenfilename(
                title="Select Configuration File",
                filetypes=[("Environment files", "*.env"), ("All files", "*.*")]
            )
            
            if file_path:
                # Load the config
                self.config = Config()
                # Temporarily set the path for loading
                old_env = os.environ.get('DOTENV_PATH')
                os.environ['DOTENV_PATH'] = file_path
                self.config.load_config()
                if old_env:
                    os.environ['DOTENV_PATH'] = old_env
                
                # Update GUI fields
                if self.config.source_connection_string:
                    self.source_conn_var.set(self.config.source_connection_string)
                if self.config.dest_connection_string:
                    self.dest_conn_var.set(self.config.dest_connection_string)
                    
                self.batch_size_var.set(str(self.config.batch_size))
                
                # Update target configuration
                self.target_type_var.set("vcore" if self.config.target_is_vcore else "ru")
                self.throughput_mode_var.set(self.config.ru_throughput_mode)
                self.manual_ru_var.set(str(self.config.ru_manual_throughput))
                self.autoscale_ru_var.set(str(self.config.ru_autoscale_max_throughput))
                self.partition_key_var.set(self.config.ru_default_partition_key)
                
                # Update UI visibility
                self.on_target_type_changed()
                self.on_throughput_mode_changed()
                
                messagebox.showinfo("Success", "Configuration loaded successfully!")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load configuration: {e}")
            
    def refresh_database_list(self):
        """Refresh the database list in the GUI."""
        if not self.connected:
            messagebox.showwarning("Warning", "Please connect to databases first.")
            return
            
        self.refresh_database_list_background()
        
    def refresh_database_list_background(self):
        """Refresh database list in background."""
        def refresh():
            try:
                if not self.migration_service:
                    return
                    
                databases = self.migration_service.list_databases()
                db_data = []
                
                for db_name in databases:
                    collections = self.migration_service.list_collections(db_name)
                    total_docs = 0
                    
                    coll_data = []
                    for coll_name in collections:
                        doc_count = self.migration_service.count_documents(db_name, coll_name)
                        total_docs += doc_count
                        coll_data.append({
                            'name': coll_name,
                            'documents': doc_count
                        })
                        
                    db_data.append({
                        'name': db_name,
                        'collections': coll_data,
                        'total_collections': len(collections),
                        'total_documents': total_docs
                    })
                    
                self.result_queue.put(('database_list', db_data))
                
            except Exception as e:
                self.result_queue.put(('error', f'Failed to refresh database list: {str(e)}'))
                
        self.run_in_background(refresh)
        
    def connect_to_databases(self):
        """Connect to MongoDB databases."""
        if self.connected:
            result = messagebox.askyesno("Reconnect", "Already connected to databases. Do you want to reconnect?")
            if not result:
                return
                
        if not self.source_conn_var.get() or not self.dest_conn_var.get():
            messagebox.showerror("Error", "Please provide both connection strings.")
            return
            
        self.connect_btn.configure(state='disabled', text="Connecting...")
        self.connection_status_var.set("Connecting...")
        
        def connect():
            try:
                # Initialize configuration
                if not self.config:
                    self.config = Config()
                    self.config.load_config()
                
                # Update config with GUI values
                self.config.source_connection_string = self.source_conn_var.get()
                self.config.dest_connection_string = self.dest_conn_var.get()
                self.config.batch_size = int(self.batch_size_var.get())
                
                # Update target configuration
                self.config.target_is_vcore = (self.target_type_var.get() == "vcore")
                self.config.ru_throughput_mode = self.throughput_mode_var.get()
                self.config.ru_manual_throughput = int(self.manual_ru_var.get())
                self.config.ru_autoscale_max_throughput = int(self.autoscale_ru_var.get())
                self.config.ru_default_partition_key = self.partition_key_var.get()
                
                # Initialize connection manager
                self.connection_manager = ConnectionManager(self.config)
                
                # Connect to source
                self.source_client = self.connection_manager.connect_to_source()
                if not self.source_client:
                    raise Exception("Failed to connect to source database")
                    
                # Connect to destination
                self.destination_client = self.connection_manager.connect_to_destination()
                if not self.destination_client:
                    raise Exception("Failed to connect to destination database")
                    
                # Initialize migration service
                self.migration_service = MigrationService(
                    self.source_client, 
                    self.destination_client, 
                    self.config,
                    connection_manager=self.connection_manager
                )
                
                self.connected = True
                
                # Determine target message
                target_msg = "Target: vCore" if self.config.target_is_vcore else f"Target: RU ({self.config.ru_throughput_mode})"
                
                self.result_queue.put(('connection_status', {
                    'connected': True,
                    'message': f'Successfully connected to both databases. {target_msg}'
                }))
                
                # Load database list
                self.refresh_database_list_background()
                
            except Exception as e:
                self.result_queue.put(('connection_status', {
                    'connected': False,
                    'message': f'Connection failed: {str(e)}'
                }))
                
        self.run_in_background(connect)

    def test_connections(self):
        """Test database connections without fully connecting."""
        if not self.source_conn_var.get() or not self.dest_conn_var.get():
            messagebox.showerror("Error", "Please provide both connection strings.")
            return
            
        self.test_conn_btn.configure(state='disabled', text="Testing...")
        
        def test():
            try:
                # Test connections without storing clients
                config = Config()
                config.source_connection_string = self.source_conn_var.get()
                config.dest_connection_string = self.dest_conn_var.get()
                
                connection_manager = ConnectionManager(config)
                
                # Test source
                source_client = connection_manager.connect_to_source()
                if source_client:
                    source_client.close()
                else:
                    raise Exception("Source connection failed")
                    
                # Test destination
                dest_client = connection_manager.connect_to_destination()
                if dest_client:
                    dest_client.close()
                else:
                    raise Exception("Destination connection failed")
                    
                self.result_queue.put(('info', 'Connection test successful'))
                
            except Exception as e:
                self.result_queue.put(('error', f'Connection test failed: {str(e)}'))
            finally:
                self.root.after(0, lambda: self.test_conn_btn.configure(state='normal', text="Test Connection"))
                
        self.run_in_background(test)
        
    def on_target_type_changed(self):
        """Handle target type selection change (vCore vs RU)."""
        target_type = self.target_type_var.get()
        
        if target_type == "vcore":
            # Hide RU configuration for vCore
            self.ru_config_frame.grid_remove()
        else:
            # Show RU configuration for RU-based Cosmos DB
            self.ru_config_frame.grid()
            
    def on_throughput_mode_changed(self):
        """Handle throughput mode selection change (manual vs autoscale)."""
        mode = self.throughput_mode_var.get()
        
        if mode == "manual":
            # Show manual throughput controls
            self.manual_frame.grid()
            self.autoscale_frame.grid_remove()
        else:
            # Show autoscale throughput controls
            self.manual_frame.grid_remove()
            self.autoscale_frame.grid()

    def update_connection_status(self, data):
        """Update connection status in the GUI."""
        if data['connected']:
            self.connection_status_var.set(data['message'])
            self.connection_status_label.configure(style='Success.TLabel')
            self.conn_indicator.configure(fg="green")
            
            # Enable migration controls
            self.migrate_btn.configure(state='normal')
            
        else:
            self.connection_status_var.set(data['message'])
            self.connection_status_label.configure(style='Error.TLabel')
            self.conn_indicator.configure(fg="red")
            
        self.connect_btn.configure(state='normal', text="Connect")
        self.status_var.set("Ready")

    def update_pg_connection_status(self, data):
        """Update PostgreSQL connection status in the GUI."""
        if data['connected']:
            self.pg_connection_status_var.set(data['message'])
            self.pg_connection_status_label.configure(style='Success.TLabel')
            
            # Enable PostgreSQL migration controls
            self.pg_migrate_schema_btn.configure(state='normal')
            self.pg_migrate_table_btn.configure(state='normal')
            self.pg_migrate_all_btn.configure(state='normal')
            
        else:
            self.pg_connection_status_var.set(data['message'])
            self.pg_connection_status_label.configure(style='Error.TLabel')
            
        self.pg_connect_btn.configure(state='normal', text="Connect to PostgreSQL")

    def update_pg_schema_list(self, schema_data):
        """Update the PostgreSQL schema list and tree view."""
        # Clear existing items
        for item in self.pg_schema_tree.get_children():
            self.pg_schema_tree.delete(item)
            
        # Update schema combobox
        schema_names = [schema['name'] for schema in schema_data]
        self.pg_schema_combo['values'] = schema_names
        
        # Populate tree view
        for schema in schema_data:
            schema_item = self.pg_schema_tree.insert('', 'end', text=schema['name'], 
                                             values=(schema['name'], '', schema['total_records']))
            
            for table in schema['tables']:
                self.pg_schema_tree.insert(schema_item, 'end', text=table['name'],
                                   values=('', table['name'], table.get('estimated_rows', 0)))
        
        # Enable refresh button
        self.pg_schema_combo.configure(state='readonly')

    def start_pg_migration(self, data):
        """Start PostgreSQL migration progress tracking."""
        # Update status and disable migration buttons during migration
        self.pg_migrate_schema_btn.configure(state='disabled')
        self.pg_migrate_table_btn.configure(state='disabled')
        self.pg_migrate_all_btn.configure(state='disabled')
        
        # Log the start of migration
        migration_info = f"Starting PostgreSQL migration: {data}"
        logger.info(migration_info)
        self.result_queue.put(('log', migration_info))

    def update_pg_current_operation(self, operation):
        """Update current PostgreSQL operation status."""
        logger.info(f"PostgreSQL operation: {operation}")
        self.result_queue.put(('log', f"PostgreSQL: {operation}"))

    def complete_pg_migration(self, data):
        """Handle completion of PostgreSQL schema or table migration."""
        migration_info = f"PostgreSQL migration completed: {data}"
        logger.info(migration_info)
        self.result_queue.put(('log', migration_info))

    def finish_pg_migration(self, data):
        """Finish PostgreSQL migration and re-enable controls."""
        # Re-enable migration buttons
        self.pg_migrate_schema_btn.configure(state='normal')
        self.pg_migrate_table_btn.configure(state='normal')
        self.pg_migrate_all_btn.configure(state='normal')
        
        # Log completion
        migration_info = f"PostgreSQL migration finished: {data}"
        logger.info(migration_info)
        self.result_queue.put(('log', migration_info))
        
        # Show completion message
        messagebox.showinfo("Migration Complete", f"PostgreSQL migration completed successfully!")

    def update_database_list(self, db_data):
        """Update the database list tree view."""
        # Clear existing items
        for item in self.db_tree.get_children():
            self.db_tree.delete(item)
            
        # Update comboboxes
        db_names = [db['name'] for db in db_data]
        self.db_combo['values'] = db_names
        
        # Populate tree view
        for db in db_data:
            db_item = self.db_tree.insert('', 'end', text=db['name'], 
                                         values=(db['name'], db['total_collections'], db['total_documents']))
            
            for coll in db['collections']:
                self.db_tree.insert(db_item, 'end', text=f"  {coll['name']}", 
                                   values=('', coll['name'], coll['documents']))
        
        # Store database data for later use
        self.databases = db_data

    def on_database_selected(self, event=None):
        """Handle database selection in the combobox."""
        selected_db = self.selected_db_var.get()
        if not selected_db:
            return
            
        # Find and expand the selected database in the tree
        for item in self.db_tree.get_children():
            if self.db_tree.item(item, 'text') == selected_db:
                self.db_tree.item(item, open=True)
                self.db_tree.selection_set(item)
                self.db_tree.focus(item)
                break
        
        # Refresh collections for the selected database
        self.refresh_collections()
        self.log_migration_message(f"Database selected: {selected_db}", "INFO")
        self.update_migration_controls()

    def start_migration(self):
        """Start MongoDB migration process."""
        if not self.connected:
            messagebox.showerror("Error", "Please connect to databases first.")
            return
            
        if not self.migration_service:
            messagebox.showerror("Error", "Migration service not initialized.")
            return
            
        # Get migration settings
        migration_type = self.migration_type_var.get()
        selected_db = self.selected_db_var.get()
        
        if migration_type == "database" and not selected_db:
            messagebox.showerror("Error", "Please select a database to migrate.")
            return
        elif migration_type == "collections" and (not selected_db or not self.selected_collections):
            messagebox.showerror("Error", "Please select a database and at least one collection to migrate.")
            return
            
        # Prepare migration confirmation message
        if migration_type == "all":
            message = "Are you sure you want to migrate ALL databases?\nThis operation may take a long time."
        elif migration_type == "database":
            message = f"Are you sure you want to migrate the entire database '{selected_db}'?"
        else:  # collections
            selected_count = len(self.selected_collections)
            selected_names = [self.collection_data[item]['name'] for item in self.selected_collections]
            message = (f"Are you sure you want to migrate {selected_count} collection(s) "
                      f"from database '{selected_db}'?\n\nCollections:\n" + 
                      "\n".join(f"• {name}" for name in selected_names[:10]))
            if len(selected_names) > 10:
                message += f"\n... and {len(selected_names) - 10} more"
            
        if not messagebox.askyesno("Confirm Migration", message):
            return
            
        def migration_task():
            from datetime import datetime
            migration_start_time = datetime.now()
            
            try:
                self.log_migration_message(f'🚀 Starting {migration_type} migration at {migration_start_time.strftime("%Y-%m-%d %H:%M:%S")}', "INFO")
                
                # Disable migration controls
                self.migrate_btn.configure(state='disabled')
                self.stop_migration_btn.configure(state='normal')
                self.pause_migration_btn.configure(state='normal')
                
                if migration_type == "all":
                    # Migrate all databases
                    databases = self.migration_service.list_databases()
                    total_dbs = len(databases)
                    
                    self.log_migration_message(f'📊 Found {total_dbs} databases to migrate', "INFO")
                    
                    for i, db_name in enumerate(databases):
                        if self._migration_stopped:
                            break
                            
                        db_start_time = datetime.now()
                        self.log_migration_message(f'📚 Processing database {i+1}/{total_dbs}: {db_name}', "INFO")
                        collections = self.migration_service.list_collections(db_name)
                        
                        for coll_name in collections:
                            if self._migration_stopped:
                                break
                                
                            # Migrate collection with timing
                            coll_start_time = datetime.now()
                            self.log_migration_message(f'📄 Migrating collection: {db_name}.{coll_name}', "INFO")
                            
                            result = self.migration_service.migrate_collection(db_name, coll_name)
                            coll_end_time = datetime.now()
                            
                            # Add timing information to result
                            result['collection'] = f"{db_name}.{coll_name}"
                            result['start_time'] = coll_start_time
                            result['end_time'] = coll_end_time
                            result['duration'] = (coll_end_time - coll_start_time).total_seconds()
                            
                            # Log detailed migration result
                            self.log_detailed_migration_result(result)
                            
                        db_end_time = datetime.now()
                        db_duration = self.format_migration_timing(db_start_time, db_end_time)
                        self.log_migration_message(f'✅ Completed database {db_name} in {db_duration}', "SUCCESS")
                            
                elif migration_type == "database":
                    # Migrate selected database
                    collections = self.migration_service.list_collections(selected_db)
                    total_collections = len(collections)
                    
                    self.log_migration_message(f'📊 Found {total_collections} collections in database: {selected_db}', "INFO")
                    
                    for i, coll_name in enumerate(collections):
                        if self._migration_stopped:
                            break
                            
                        coll_start_time = datetime.now()
                        self.log_migration_message(f'📄 Migrating collection {i+1}/{total_collections}: {selected_db}.{coll_name}', "INFO")
                        
                        result = self.migration_service.migrate_collection(selected_db, coll_name)
                        coll_end_time = datetime.now()
                        
                        # Add timing information to result
                        result['collection'] = f"{selected_db}.{coll_name}"
                        result['start_time'] = coll_start_time
                        result['end_time'] = coll_end_time
                        result['duration'] = (coll_end_time - coll_start_time).total_seconds()
                        
                        # Log detailed migration result
                        self.log_detailed_migration_result(result)
                        
                else:  # collections
                    # Migrate selected collections
                    selected_count = len(self.selected_collections)
                    self.log_migration_message(f'📊 Migrating {selected_count} selected collections from database: {selected_db}', "INFO")
                    
                    for i, item in enumerate(self.selected_collections):
                        if self._migration_stopped:
                            break
                            
                        coll_name = self.collection_data[item]['name']
                        coll_start_time = datetime.now()
                        self.log_migration_message(f'📄 Migrating collection {i+1}/{selected_count}: {selected_db}.{coll_name}', "INFO")
                        
                        result = self.migration_service.migrate_collection(selected_db, coll_name)
                        coll_end_time = datetime.now()
                        
                        # Add timing information to result
                        result['collection'] = f"{selected_db}.{coll_name}"
                        result['start_time'] = coll_start_time
                        result['end_time'] = coll_end_time
                        result['duration'] = (coll_end_time - coll_start_time).total_seconds()
                        
                        # Log detailed migration result
                        self.log_detailed_migration_result(result)
                
                migration_end_time = datetime.now()
                total_duration = self.format_migration_timing(migration_start_time, migration_end_time)
                
                if not self._migration_stopped:
                    self.log_migration_message(f'🎉 Migration completed successfully in {total_duration}!', "SUCCESS")
                    messagebox.showinfo("Success", f"Migration completed successfully!\nTotal time: {total_duration}")
                else:
                    self.log_migration_message(f'⏹️ Migration stopped by user after {total_duration}', "WARNING")
                    
            except Exception as e:
                migration_end_time = datetime.now()
                total_duration = self.format_migration_timing(migration_start_time, migration_end_time)
                error_msg = f'❌ Migration failed after {total_duration}: {str(e)}'
                self.log_migration_message(error_msg, "ERROR")
                messagebox.showerror("Migration Error", f"Migration failed: {str(e)}")
                logger.exception("Migration error")
            finally:
                # Re-enable controls
                self.migrate_btn.configure(state='normal')
                self.stop_migration_btn.configure(state='disabled')
                self.pause_migration_btn.configure(state='disabled')
                self._migration_stopped = False
                
        # Initialize migration state
        self._migration_stopped = False
        
        # Start migration in background
        threading.Thread(target=migration_task, daemon=True).start()
    
    def log_detailed_migration_result(self, result):
        """Log detailed migration result with statistics."""
        try:
            total = result.get('total_documents', 0)
            migrated = result.get('migrated_documents', 0)
            failed = result.get('failed_documents', 0)
            inserted = result.get('inserted_documents', 0)
            upserted = result.get('upserted_documents', 0)
            modified = result.get('modified_documents', 0)
            success = result.get('success', False)
            collection_name = result.get('collection', 'Unknown')
            duration = result.get('duration', 0)
            
            # Create detailed log message
            status = "✅ SUCCESS" if success else "❌ FAILED"
            
            log_message = (
                f"Migration Document Result - {collection_name} (Duration: {duration:.2f}s):\n"
                f"  Status: {status}\n"
                f"  Total Documents: {total:,}\n"
                f"  Migrated Documents: {migrated:,}\n"
                f"  Failed Documents: {failed:,}\n"
                f"  Inserted Documents: {inserted:,}\n"
                f"  Upserted Documents: {upserted:,}\n"
                f"  Modified Documents: {modified:,}"
            )
            
            if total > 0:
                success_rate = ((migrated / total) * 100)
                log_message += f"\n  Success Rate: {success_rate:.1f}%"
                
                if duration > 0:
                    rate = migrated / duration
                    log_message += f"\n  Migration Rate: {rate:.1f} docs/sec"
            
            # Log the detailed message
            level = "SUCCESS" if success else "ERROR"
            self.log_migration_message(log_message, level)
            
            # Update progress statistics
            self.update_migration_statistics(result)
            
        except Exception as e:
            self.log_migration_message(f"Error logging migration result: {str(e)}", "ERROR")
    
    def update_migration_statistics(self, result):
        """Update migration progress statistics."""
        try:
            # Update progress bar and labels based on result
            if hasattr(self, 'progress_var') and hasattr(self, 'progress_label_var'):
                total = result.get('total_documents', 0)
                migrated = result.get('migrated_documents', 0)
                
                if total > 0:
                    progress_percentage = (migrated / total) * 100
                    self.progress_var.set(progress_percentage)
                    self.progress_label_var.set(f"Processing: {migrated:,} of {total:,} documents")
            
        except Exception as e:
            self.log_migration_message(f"Error updating statistics: {str(e)}", "ERROR")

    def stop_migration(self):
        """Stop the ongoing migration."""
        if hasattr(self, '_migration_stopped'):
            self._migration_stopped = True
            self.log_migration_message('Stopping migration...', "WARNING")
            self.stop_migration_btn.configure(state='disabled')

    def pause_migration(self):
        """Pause the ongoing migration (placeholder for future implementation)."""
        self.log_migration_message('Migration pause requested (not yet implemented)', "WARNING")

    def format_migration_timing(self, start_time, end_time):
        """Format migration timing into a readable string."""
        try:
            duration = end_time - start_time
            total_seconds = int(duration.total_seconds())
            
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            
            if hours > 0:
                return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            else:
                return f"{minutes:02d}:{seconds:02d}"
                
        except Exception as e:
            logger.error(f"Error formatting migration timing: {e}")
            return "00:00"

    # Logs Methods
    def clear_logs(self):
        """Clear the log text area."""
        self.log_text.delete(1.0, tk.END)

    def save_logs(self):
        """Save logs to a file."""
        try:
            from tkinter import filedialog
            filename = filedialog.asksaveasfilename(
                defaultextension=".log",
                filetypes=[("Log files", "*.log"), ("Text files", "*.txt"), ("All files", "*.*")]
            )
            if filename:
                with open(filename, 'w') as f:
                    f.write(self.log_text.get(1.0, tk.END))
                self.log_message(f"Logs saved to {filename}", "SUCCESS")
        except Exception as e:
            self.log_message(f"Error saving logs: {str(e)}", "ERROR")

    def change_log_level(self, event=None):
        """Change the log level."""
        level = self.log_level_var.get()
        self.log_message(f"Log level changed to {level}", "INFO")

    # PostgreSQL Methods
    def test_postgresql_connections(self):
        """Test PostgreSQL connections with the provided credentials."""
        try:
            # Get connection parameters for source
            source_server = self.pg_source_server_var.get().strip()
            source_db = self.pg_source_db_var.get().strip()
            source_user = self.pg_source_user_var.get().strip()
            source_pass = self.pg_source_pass_var.get().strip()
            
            # Get connection parameters for destination
            dest_server = self.pg_dest_server_var.get().strip()
            dest_db = self.pg_dest_db_var.get().strip()
            dest_user = self.pg_dest_user_var.get().strip()
            dest_pass = self.pg_dest_pass_var.get().strip()
            
            # Validate source connection fields
            source_errors = self.validate_postgresql_connection_fields(
                source_server, source_db, source_user, source_pass
            )
            
            # Validate destination connection fields
            dest_errors = self.validate_postgresql_connection_fields(
                dest_server, dest_db, dest_user, dest_pass
            )
            
            if source_errors or dest_errors:
                error_msg = "Connection validation failed:\n"
                if source_errors:
                    error_msg += "Source errors:\n" + "\n".join(f"  - {error}" for error in source_errors) + "\n"
                if dest_errors:
                    error_msg += "Destination errors:\n" + "\n".join(f"  - {error}" for error in dest_errors)
                messagebox.showerror("Validation Error", error_msg)
                return
            
            # Build connection strings
            try:
                source_conn_str = self.build_postgresql_connection_string(
                    source_server, source_db, source_user, source_pass
                )
                dest_conn_str = self.build_postgresql_connection_string(
                    dest_server, dest_db, dest_user, dest_pass
                )
            except ValueError as e:
                messagebox.showerror("Connection Error", str(e))
                return
            
            # Test connections in background thread
            def test_connections():
                try:
                    if not self.pg_connection_manager:
                        try:
                            from .config import Config
                        except ImportError:
                            from config import Config
                        self.config = Config()
                        self.pg_connection_manager = PostgreSQLConnectionManager(self.config)
                    
                    # Test source connection
                    self.log_message("Testing source PostgreSQL connection...", "INFO")
                    source_pool = self.pg_connection_manager.create_connection_pool(source_conn_str)
                    
                    if source_pool:
                        # Test a simple query
                        with source_pool.get_connection() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute("SELECT version();")
                                version = cursor.fetchone()[0]
                        source_pool.close_all()
                        self.log_message(f"Source connection successful: {version[:50]}...", "SUCCESS")
                    else:
                        raise Exception("Failed to create source connection pool")
                    
                    # Test destination connection
                    self.log_message("Testing destination PostgreSQL connection...", "INFO")
                    dest_pool = self.pg_connection_manager.create_connection_pool(dest_conn_str)
                    
                    if dest_pool:
                        # Test a simple query
                        with dest_pool.get_connection() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute("SELECT version();")
                                version = cursor.fetchone()[0]
                        dest_pool.close_all()
                        self.log_message(f"Destination connection successful: {version[:50]}...", "SUCCESS")
                    else:
                        raise Exception("Failed to create destination connection pool")
                    
                    # Success
                    self.result_queue.put(("pg_test_success", "Both PostgreSQL connections tested successfully!"))
                    
                except Exception as e:
                    self.log_message(f"Connection test failed: {str(e)}", "ERROR")
                    self.result_queue.put(("pg_test_error", str(e)))
            
            # Start test in background
            threading.Thread(target=test_connections, daemon=True).start()
            self.log_message("Starting PostgreSQL connection tests...", "INFO")
            
        except Exception as e:
            self.log_message(f"Error during connection test: {str(e)}", "ERROR")
            messagebox.showerror("Test Error", f"Error during connection test: {str(e)}")

    def connect_to_postgresql(self):
        """Connect to PostgreSQL databases."""
        try:
            # Get connection parameters for source
            source_server = self.pg_source_server_var.get().strip()
            source_db = self.pg_source_db_var.get().strip()
            source_user = self.pg_source_user_var.get().strip()
            source_pass = self.pg_source_pass_var.get().strip()
            
            # Get connection parameters for destination
            dest_server = self.pg_dest_server_var.get().strip()
            dest_db = self.pg_dest_db_var.get().strip()
            dest_user = self.pg_dest_user_var.get().strip()
            dest_pass = self.pg_dest_pass_var.get().strip()
            
            # Validate connection fields
            source_errors = self.validate_postgresql_connection_fields(
                source_server, source_db, source_user, source_pass
            )
            dest_errors = self.validate_postgresql_connection_fields(
                dest_server, dest_db, dest_user, dest_pass
            )
            
            if source_errors or dest_errors:
                error_msg = "Connection validation failed:\n"
                if source_errors:
                    error_msg += "Source errors:\n" + "\n".join(f"  - {error}" for error in source_errors) + "\n"
                if dest_errors:
                    error_msg += "Destination errors:\n" + "\n".join(f"  - {error}" for error in dest_errors)
                messagebox.showerror("Validation Error", error_msg)
                return
            
            # Build connection strings
            try:
                source_conn_str = self.build_postgresql_connection_string(
                    source_server, source_db, source_user, source_pass
                )
                dest_conn_str = self.build_postgresql_connection_string(
                    dest_server, dest_db, dest_user, dest_pass
                )
            except ValueError as e:
                messagebox.showerror("Connection Error", str(e))
                return
            
            # Connect in background thread
            def connect():
                try:
                    if not self.pg_connection_manager:
                        try:
                            from .config import Config
                        except ImportError:
                            from config import Config
                        self.config = Config()
                        self.pg_connection_manager = PostgreSQLConnectionManager(self.config)
                    
                    if not self.pg_migration_service:
                        self.pg_migration_service = PostgreSQLMigrationService(self.pg_connection_manager)
                    
                    # Create connection pools
                    self.log_message("Connecting to source PostgreSQL...", "INFO")
                    self.pg_source_pool = self.pg_connection_manager.create_connection_pool(source_conn_str)
                    
                    if not self.pg_source_pool:
                        raise Exception("Failed to connect to source PostgreSQL")
                    
                    self.log_message("Connecting to destination PostgreSQL...", "INFO")
                    self.pg_dest_pool = self.pg_connection_manager.create_connection_pool(dest_conn_str)
                    
                    if not self.pg_dest_pool:
                        raise Exception("Failed to connect to destination PostgreSQL")
                    
                    self.pg_connected = True
                    self.result_queue.put(("pg_connect_success", "PostgreSQL connections established successfully!"))
                    
                except Exception as e:
                    self.log_message(f"PostgreSQL connection failed: {str(e)}", "ERROR")
                    self.result_queue.put(("pg_connect_error", str(e)))
            
            # Start connection in background
            threading.Thread(target=connect, daemon=True).start()
            self.log_message("Connecting to PostgreSQL databases...", "INFO")
            
        except Exception as e:
            self.log_message(f"Error during PostgreSQL connection: {str(e)}", "ERROR")
            messagebox.showerror("Connection Error", f"Error during PostgreSQL connection: {str(e)}")

    def refresh_postgresql_schemas(self):
        """Refresh PostgreSQL schema list."""
        if not self.pg_connected or not self.pg_source_pool:
            messagebox.showwarning("Not Connected", "Please connect to PostgreSQL databases first.")
            return
        
        def fetch_schemas():
            try:
                self.log_message("Fetching PostgreSQL schemas...", "INFO")
                schemas = {}
                
                with self.pg_source_pool.get_connection() as conn:
                    with conn.cursor() as cursor:
                        # Get all user schemas and their tables
                        cursor.execute("""
                            SELECT schemaname, tablename, 
                                   COALESCE(n_tup_ins - n_tup_del, 0) as row_count
                            FROM pg_tables pt
                            LEFT JOIN pg_stat_user_tables pst ON pt.tablename = pst.relname 
                                AND pt.schemaname = pst.schemaname
                            WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                            ORDER BY schemaname, tablename;
                        """)
                        
                        results = cursor.fetchall()
                        
                        for schema_name, table_name, row_count in results:
                            if schema_name not in schemas:
                                schemas[schema_name] = []
                            schemas[schema_name].append({
                                'name': table_name,
                                'type': 'table',
                                'count': row_count if row_count is not None else 0
                            })
                
                self.result_queue.put(("pg_schema_list", schemas))
                self.log_message(f"Found {len(schemas)} schemas", "SUCCESS")
                
            except Exception as e:
                self.log_message(f"Error fetching PostgreSQL schemas: {str(e)}", "ERROR")
                self.result_queue.put(("error", f"Failed to fetch schemas: {str(e)}"))
        
        # Start fetch in background
        threading.Thread(target=fetch_schemas, daemon=True).start()

    def start_postgresql_migration(self):
        """Start PostgreSQL migration."""
        pass

    def stop_postgresql_migration(self):
        """Stop PostgreSQL migration."""
        pass

    def handle_postgresql_results(self):
        """Handle PostgreSQL result queue messages."""
        pass

    def update_postgresql_schema_tree(self, schemas):
        """Update PostgreSQL schema tree with schemas and tables."""
        pass

    def log_migration_message(self, message, level="INFO"):
        """Log a message to the migration log panel with timestamp and color coding."""
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            # Add emoji based on level
            level_emojis = {
                "INFO": "ℹ️",
                "SUCCESS": "✅", 
                "WARNING": "⚠️",
                "ERROR": "❌",
                "DEBUG": "🔍"
           
            }
            
            emoji = level_emojis.get(level, "📝")
            formatted_message = f"[{timestamp}] {emoji} {message}\n"
            
            # Determine text tag for coloring
            tag = level.lower()
            
            # Insert into migration log
            if hasattr(self, 'migration_log_text'):
                self.migration_log_text.insert(tk.END, formatted_message, tag)
                
                # Auto-scroll if enabled
                if hasattr(self, 'auto_scroll_var') and self.auto_scroll_var.get():
                    self.migration_log_text.see(tk.END)
            
            # Also log to console/file
            getattr(logger, level.lower(), logger.info)(message)
            
        except Exception as e:
            logger.error(f"Error logging migration message: {e}")

    def clear_migration_logs(self):
        """Clear the migration log panel."""
        try:
            if hasattr(self, 'migration_log_text'):
                self.migration_log_text.delete(1.0, tk.END)
                self.log_migration_message("Migration logs cleared", "INFO")
        except Exception as e:
            logger.error(f"Error clearing migration logs: {e}")

    def save_migration_logs(self):
        """Save migration logs to a file."""
        try:
            if not hasattr(self, 'migration_log_text'):
                return
                
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = filedialog.asksaveasfilename(
                defaultextension=".log",
                filetypes=[("Log files", "*.log"), ("Text files", "*.txt"), ("All files", "*.*")],
                initialvalue=f"migration_log_{timestamp}.log"
            )
            
            if filename:
                content = self.migration_log_text.get(1.0, tk.END)
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content)
                self.log_migration_message(f"Migration logs saved to: {filename}", "SUCCESS")
                
        except Exception as e:
            self.log_migration_message(f"Error saving migration logs: {str(e)}", "ERROR")

    def on_migration_type_changed(self):
        """Handle migration type selection change."""
        try:
            migration_type = self.migration_type_var.get()
            
            # Enable/disable collection selection based on migration type
            if migration_type == "collections":
                # Enable collection selection
                self.refresh_collections()
                self.log_migration_message("Collection selection enabled", "INFO")
            else:
                # Clear collection selection
                self.select_no_collections()
                if migration_type == "all":
                    self.log_migration_message("All databases migration selected", "INFO")
                elif migration_type == "database":
                    self.log_migration_message("Single database migration selected", "INFO")
                    
            self.update_migration_controls()
            
        except Exception as e:
            self.log_migration_message(f"Error handling migration type change: {str(e)}", "ERROR")

    def update_migration_controls(self):
        """Update the state of migration controls based on current selections."""
        try:
            if not self.connected:
                self.migrate_btn.configure(state='disabled')
                return

            migration_type = self.migration_type_var.get()
            selected_db = self.selected_db_var.get()
            
            can_migrate = False
            
            if migration_type == "all":
                # Can migrate if connected
                can_migrate = True
            elif migration_type == "database":
                # Can migrate if database is selected
                can_migrate = bool(selected_db)
            elif migration_type == "collections":
                # Can migrate if database and collections are selected
                can_migrate = bool(selected_db and self.selected_collections)
            
            self.migrate_btn.configure(state='normal' if can_migrate else 'disabled')
            
        except Exception as e:
            self.log_migration_message(f"Error updating migration controls: {str(e)}", "ERROR")

    def on_collection_click(self, event):
        """Handle collection tree click to toggle selection."""
        try:
            item = self.collections_tree.identify('item', event.x, event.y)
            if item:
                self.toggle_collection_selection(item)
        except Exception as e:
            self.log_migration_message(f"Error handling collection click: {str(e)}", "ERROR")

    def on_collection_space(self, event):
        """Handle spacebar press to toggle collection selection."""
        try:
            selection = self.collections_tree.selection()
            if selection:
                for item in selection:
                    self.toggle_collection_selection(item)
        except Exception as e:
            self.log_migration_message(f"Error handling collection space: {str(e)}", "ERROR")

    def toggle_collection_selection(self, item):
        """Toggle the selection state of a collection."""
        try:
            if item in self.selected_collections:
                # Deselect
                self.selected_collections.remove(item)
                self.collections_tree.set(item, 'Selected', '☐')
            else:
                # Select
                self.selected_collections.add(item)
                self.collections_tree.set(item, 'Selected', '☑')
            
            self.update_selected_summary()
        except Exception as e:
            self.log_migration_message(f"Error toggling collection selection: {str(e)}", "ERROR")

    def select_all_collections(self):
        """Select all collections in the current database."""
        try:
            for item in self.collections_tree.get_children():
                if item not in self.selected_collections:
                    self.selected_collections.add(item)
                    self.collections_tree.set(item, 'Selected', '☑')
            
            self.update_selected_summary()
            self.log_migration_message("Selected all collections", "INFO")
        except Exception as e:
            self.log_migration_message(f"Error selecting all collections: {str(e)}", "ERROR")

    def select_no_collections(self):
        """Deselect all collections."""
        try:
            for item in list(self.selected_collections):
                self.selected_collections.remove(item)
                self.collections_tree.set(item, 'Selected', '☐')
            
            self.update_selected_summary()
            self.log_migration_message("Deselected all collections", "INFO")
        except Exception as e:
            self.log_migration_message(f"Error deselecting collections: {str(e)}", "ERROR")

    def refresh_collections(self):
        """Refresh the collections list for the selected database."""
        try:
            selected_db = self.selected_db_var.get()
            if not selected_db or not self.migration_service:
                # Clear existing collections
                for item in self.collections_tree.get_children():
                    self.collections_tree.delete(item)
                self.selected_collections.clear()
                self.collection_data.clear()
                self.update_selected_summary()
                return

            # Clear existing collections
            for item in self.collections_tree.get_children():
                self.collections_tree.delete(item)
            self.selected_collections.clear()
            self.collection_data.clear()

            # Get collections from the database
            collections = self.migration_service.list_collections(selected_db)
            
            for coll_name in collections:
                try:
                    # Get document count and size estimate
                    doc_count = self.migration_service.get_collection_count(selected_db, coll_name)
                    size_mb = "Calculating..."
                    
                    # Insert collection into tree
                    item_id = self.collections_tree.insert('', 'end', values=('☐', coll_name, f"{doc_count:,}", size_mb))
                    
                    # Store collection metadata
                    self.collection_data[item_id] = {
                        'name': coll_name,
                        'doc_count': doc_count,
                        'size_mb': 0,
                        'database': selected_db
                    }
                    
                except Exception as e:
                    # Insert with error info
                    item_id = self.collections_tree.insert('', 'end', values=('☐', coll_name, 'Error', 'Error'))
                    self.collection_data[item_id] = {
                        'name': coll_name,
                        'doc_count': 0,
                        'size_mb': 0,
                        'database': selected_db
                    }
                    logger.warning(f"Error getting collection info for {coll_name}: {e}")

            self.update_selected_summary()
            self.log_migration_message(f"Refreshed collections for database: {selected_db}", "INFO")
            
        except Exception as e:
            self.log_migration_message(f"Error refreshing collections: {str(e)}", "ERROR")

    def update_selected_summary(self):
        """Update the selected collections summary label."""
        try:
            selected_count = len(self.selected_collections)
            if selected_count == 0:
                summary = "No collections selected"
            elif selected_count == 1:
                summary = "1 collection selected"
            else:
                summary = f"{selected_count} collections selected"
            
            self.selected_summary_var.set(summary)
        except Exception as e:
            self.selected_summary_var.set("Error updating summary")

    def run(self):
        """Start the GUI application main loop."""
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            print("Application interrupted by user")
        except Exception as e:
            logger.error(f"GUI application error: {e}")
            raise

def main():
    """Main entry point for the GUI application."""
    try:
        # Create and run the application
        app = CosmosDBMigrationGUI()
        app.run()
    except Exception as e:
        print(f"Error starting GUI application: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

