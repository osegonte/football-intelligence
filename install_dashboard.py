#!/usr/bin/env python3
"""
This script installs the enhanced dashboard files to your football-intelligence project.
"""
import os
import sys
import shutil
from pathlib import Path

def print_colored(text, color="green"):
    """Print colored text to the console"""
    colors = {
        "red": "\033[91m",
        "green": "\033[92m",
        "yellow": "\033[93m",
        "blue": "\033[94m",
        "purple": "\033[95m",
        "end": "\033[0m"
    }
    print(f"{colors.get(color, colors['green'])}{text}{colors['end']}")

def create_backup(file_path):
    """Create a backup of the specified file"""
    if os.path.exists(file_path):
        backup_path = f"{file_path}.bak"
        shutil.copy2(file_path, backup_path)
        return True
    return False

def install_dashboard_files():
    """Install the enhanced dashboard files"""
    # Get project root directory (parent of this script's directory)
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    project_root = script_dir.parent
    
    # Create directories if they don't exist
    dashboard_dir = project_root / "dashboard"
    if not dashboard_dir.exists():
        dashboard_dir.mkdir(parents=True)
        print_colored(f"Created dashboard directory: {dashboard_dir}")
    
    # Install enhanced app.py
    source_file = script_dir / "football-dashboard.py"
    target_file = dashboard_dir / "enhanced_app.py"
    
    if not source_file.exists():
        print_colored(f"Source file not found: {source_file}", "red")
        return False
    
    # Create backup of existing file if it exists
    if target_file.exists():
        create_backup(target_file)
        print_colored(f"Created backup: {target_file}.bak", "yellow")
    
    # Copy the file
    shutil.copy2(source_file, target_file)
    print_colored(f"Installed enhanced dashboard: {target_file}")
    
    # Install visualizations.py
    source_file = script_dir / "dashboard-visualizations.py"
    target_file = dashboard_dir / "visualizations.py"
    
    if not source_file.exists():
        print_colored(f"Source file not found: {source_file}", "red")
    else:
        # Create backup of existing file if it exists
        if target_file.exists():
            create_backup(target_file)
            print_colored(f"Created backup: {target_file}.bak", "yellow")
        
        # Copy the file
        shutil.copy2(source_file, target_file)
        print_colored(f"Installed visualizations: {target_file}")
    
    # Install run_dashboard.py script
    source_file = script_dir / "run-dashboard.py"
    target_file = project_root / "run_dashboard.py"
    
    if not source_file.exists():
        print_colored(f"Source file not found: {source_file}", "red")
    else:
        # Create backup of existing file if it exists
        if target_file.exists():
            create_backup(target_file)
            print_colored(f"Created backup: {target_file}.bak", "yellow")
        
        # Copy the file
        shutil.copy2(source_file, target_file)
        print_colored(f"Installed run_dashboard.py: {target_file}")
        
        # Make it executable
        os.chmod(target_file, 0o755)
    
    return True

def main():
    print_colored("⚽ Football Intelligence Enhanced Dashboard Installer", "blue")
    print_colored("======================================================")
    
    # Confirm installation
    print_colored("\nThis will install the enhanced dashboard files to your project.", "yellow")
    print_colored("Existing files will be backed up with a .bak extension.", "yellow")
    print_colored("Do you want to continue? (y/n)", "yellow")
    
    choice = input("> ").strip().lower()
    if choice != "y":
        print_colored("Installation cancelled.", "red")
        return
    
    # Install files
    if install_dashboard_files():
        print_colored("\n✅ Installation complete!", "green")
        print_colored("\nYou can now run the enhanced dashboard with:", "blue")
        print_colored("  python run_dashboard.py", "purple")
    else:
        print_colored("\n❌ Installation failed!", "red")

if __name__ == "__main__":
    main()