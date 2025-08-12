#!/usr/bin/env python3
"""
Quick test script for the Penn State LionPath Course Scraper
"""

import subprocess
import sys
import time
import json
from pathlib import Path

def test_scraper():
    """Test the scraper with a small dataset"""
    print("🧪 Testing Penn State Course Scraper...")
    print("=" * 50)
    
    # Test command with limited subjects
    cmd = [
        sys.executable, "scraper.py",
        "--output", "test_output.jsonl",
        "--max-subjects", "3",
        "--max-workers", "2",
        "--max-detail-workers", "5",
        "--rate-limit", "5",
        "--debug"
    ]
    
    print(f"Running: {' '.join(cmd)}")
    print("-" * 50)
    
    start_time = time.time()
    
    try:
        # Run the scraper
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n📊 Test Results:")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"✅ Exit code: {result.returncode}")
        
        if result.returncode == 0:
            print("🎉 Scraper executed successfully!")
            
            # Check output file
            output_file = Path("test_output.jsonl")
            if output_file.exists():
                with open(output_file, 'r') as f:
                    lines = f.readlines()
                
                print(f"📁 Output file: {len(lines)} courses found")
                
                if lines:
                    # Parse first course as example
                    try:
                        first_course = json.loads(lines[0])
                        print(f"📚 Example course: {first_course.get('course_code', 'Unknown')}")
                        print(f"🏫 Campus: {first_course.get('campus', 'Unknown')}")
                        print(f"👥 Capacity: {first_course.get('class_capacity', 'Unknown')}")
                    except json.JSONDecodeError:
                        print("⚠️  Could not parse first course")
                
                # Cleanup
                output_file.unlink()
                print("🧹 Cleaned up test output file")
            else:
                print("❌ No output file generated")
        else:
            print("❌ Scraper failed!")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
    
    except subprocess.TimeoutExpired:
        print("⏰ Test timed out after 5 minutes")
    except Exception as e:
        print(f"💥 Test failed with error: {e}")
    
    print("\n" + "=" * 50)
    print("Test completed!")

if __name__ == "__main__":
    test_scraper()
