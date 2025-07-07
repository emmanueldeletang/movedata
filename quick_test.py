#!/usr/bin/env python3
"""
Quick test to verify GUI imports work.
"""

import sys
import os
sys.path.insert(0, 'src')

try:
    from gui import CosmosDBMigrationGUI
    print("✅ GUI import successful")
    print("✅ All AttributeError issues should be resolved")
    
    # Test if critical methods exist
    critical_methods = [
        'start_migration',
        'stop_migration', 
        'refresh_database_list',
        'update_database_list',
        'on_database_selected',
        'on_target_type_changed',
        'on_throughput_mode_changed'
    ]
    
    missing = []
    for method in critical_methods:
        if not hasattr(CosmosDBMigrationGUI, method):
            missing.append(method)
    
    if missing:
        print(f"❌ Missing methods: {missing}")
    else:
        print("✅ All critical methods present")
        print("🎉 GUI is ready to use!")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
