#!/usr/bin/env python3
"""
Test script for fbref_db_updater.py
Runs through different command options and checks if they're working properly.
"""
import subprocess
import os
import sys
import time
import argparse

def run_command(cmd, description):
    """Run a command and print the result"""
    print(f"\n{'='*80}")
    print(f"TESTING: {description}")
    print(f"COMMAND: {' '.join(cmd)}")
    print(f"{'='*80}")
    
    try:
        # Run the command and capture output
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        # Print stdout
        if result.stdout:
            print("\nOutput:")
            print(result.stdout[:1000])  # Limit output to first 1000 chars
            if len(result.stdout) > 1000:
                print("... (output truncated)")
        
        print(f"\n✅ SUCCESS: {description}")
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"\n❌ ERROR: Command failed with exit code {e.returncode}")
        if e.stdout:
            print("\nOutput:")
            print(e.stdout[:500])
        if e.stderr:
            print("\nError:")
            print(e.stderr[:500])
        return False

def main():
    parser = argparse.ArgumentParser(description="Test the fbref_db_updater.py script")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    parser.add_argument("--stats", action="store_true", help="Only test database stats")
    parser.add_argument("--team", action="store_true", help="Only test single team update")
    parser.add_argument("--fixture", action="store_true", help="Only test fixture update")
    parser.add_argument("--league", action="store_true", help="Only test league update")
    parser.add_argument("--upcoming", action="store_true", help="Only test upcoming fixtures update")
    
    args = parser.parse_args()
    
    # If no specific test is selected, show help
    if not any([args.all, args.stats, args.team, args.fixture, args.league, args.upcoming]):
        parser.print_help()
        return 1
    
    # Path to the script being tested
    script_path = "fbref_db_updater.py"
    
    # Ensure the script exists
    if not os.path.exists(script_path):
        print(f"❌ ERROR: Script not found: {script_path}")
        return 1
    
    successes = 0
    failures = 0
    
    # Test database stats
    if args.all or args.stats:
        cmd = [sys.executable, script_path, "--stats"]
        if run_command(cmd, "Database statistics"):
            successes += 1
        else:
            failures += 1
        time.sleep(1)  # Add small delay between tests
    
    # Test single team update
    if args.all or args.team:
        cmd = [sys.executable, script_path, "--team", "Arsenal", "--matches", "3"]
        if run_command(cmd, "Single team update (Arsenal)"):
            successes += 1
        else:
            failures += 1
        time.sleep(2)  # Delay to avoid rate limiting
    
    # Test fixture update
    if args.all or args.fixture:
        cmd = [sys.executable, script_path, "--fixture", "Arsenal", "Tottenham"]
        if run_command(cmd, "Fixture update (Arsenal vs Tottenham)"):
            successes += 1
        else:
            failures += 1
        time.sleep(2)  # Delay to avoid rate limiting
    
    # Test league update (with small limit)
    if args.all or args.league:
        cmd = [sys.executable, script_path, "--league", "Premier League", "--limit", "2"]
        if run_command(cmd, "League update (Premier League, 2 teams)"):
            successes += 1
        else:
            failures += 1
        time.sleep(2)  # Delay to avoid rate limiting
    
    # Test upcoming fixtures update
    if args.all or args.upcoming:
        cmd = [sys.executable, script_path, "--upcoming-fixtures", "--days", "7"]
        if run_command(cmd, "Upcoming fixtures update (7 days)"):
            successes += 1
        else:
            failures += 1
    
    # Print summary
    print("\n" + "="*80)
    print(f"TEST SUMMARY: {successes} successes, {failures} failures")
    print("="*80)
    
    return 0 if failures == 0 else 1

if __name__ == "__main__":
    sys.exit(main())