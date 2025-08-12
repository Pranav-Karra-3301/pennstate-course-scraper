#!/usr/bin/env python3
"""
Test script for the optimized Penn State LionPath Course Scraper
"""

import subprocess
import sys
import time
import json
from pathlib import Path

def test_optimized_scraper():
    """Test the optimized scraper with a small dataset"""
    print("🧪 Testing Optimized Penn State Course Scraper...")
    print("=" * 60)
    
    # Test command with limited subjects
    cmd = [
        sys.executable, "scraper_optimized.py",
        "--output", "test_optimized_output.jsonl",
        "--max-subjects", "3",
        "--max-workers", "2",
        "--max-detail-workers", "5",
        "--rate-limit", "5",
        "--debug"
    ]
    
    print(f"Running: {' '.join(cmd)}")
    print("-" * 60)
    
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
            print("🎉 Optimized scraper executed successfully!")
            
            # Check output file
            output_file = Path("test_optimized_output.jsonl")
            if output_file.exists():
                with open(output_file, 'r') as f:
                    lines = f.readlines()
                
                print(f"📁 Output file: {len(lines)} course records found")
                
                if lines:
                    # Parse first course as example
                    try:
                        first_record = json.loads(lines[0])
                        course_info = first_record.get('course', {})
                        sections_info = first_record.get('sections', [])
                        stats_info = first_record.get('stats', {})
                        
                        print(f"📚 Example course: {course_info.get('course_code', 'Unknown')}")
                        print(f"📖 Title: {course_info.get('course_title', 'Unknown')}")
                        print(f"📝 Sections: {len(sections_info)}")
                        print(f"👥 Total capacity: {stats_info.get('total_capacity', 0)}")
                        print(f"🏫 Campuses: {', '.join(stats_info.get('campuses', []))}")
                        
                        # Show section details
                        if sections_info:
                            section = sections_info[0]
                            print(f"📍 First section: {section.get('section', 'N/A')} - {section.get('instructor', 'TBA')}")
                            print(f"⏰ Time: {section.get('days', 'N/A')} {section.get('times', 'N/A')}")
                            print(f"📍 Location: {section.get('campus', 'N/A')} - {section.get('room', 'N/A')}")
                        
                        # Validate data structure
                        required_course_fields = ['course_code', 'course_title', 'subject', 'catalog_number']
                        required_section_fields = ['section', 'class_number', 'campus']
                        
                        missing_course_fields = [f for f in required_course_fields if not course_info.get(f)]
                        missing_section_fields = [f for f in required_section_fields if not sections_info[0].get(f)] if sections_info else required_section_fields
                        
                        if not missing_course_fields and not missing_section_fields:
                            print("✅ Data structure validation passed")
                        else:
                            print(f"⚠️ Missing course fields: {missing_course_fields}")
                            print(f"⚠️ Missing section fields: {missing_section_fields}")
                        
                    except json.JSONDecodeError as e:
                        print(f"❌ Could not parse first record: {e}")
                    except Exception as e:
                        print(f"❌ Error analyzing record: {e}")
                
                # Test a few more records
                if len(lines) > 1:
                    print(f"\n🔍 Testing additional records...")
                    valid_records = 0
                    
                    for i, line in enumerate(lines[:5]):  # Test first 5 records
                        try:
                            record = json.loads(line.strip())
                            if 'course' in record and 'sections' in record and 'stats' in record:
                                valid_records += 1
                            else:
                                print(f"⚠️ Record {i+1}: Missing required top-level fields")
                        except json.JSONDecodeError:
                            print(f"❌ Record {i+1}: Invalid JSON")
                    
                    print(f"✅ Valid records: {valid_records}/{min(len(lines), 5)}")
                
                # Cleanup
                output_file.unlink()
                print("🧹 Cleaned up test output file")
            else:
                print("❌ No output file generated")
        else:
            print("❌ Optimized scraper failed!")
            print("\nSTDOUT:")
            print(result.stdout)
            print("\nSTDERR:")
            print(result.stderr)
    
    except subprocess.TimeoutExpired:
        print("⏰ Test timed out after 5 minutes")
    except Exception as e:
        print(f"💥 Test failed with error: {e}")
    
    print("\n" + "=" * 60)
    print("Test completed!")

def test_data_structure():
    """Test the data structure classes"""
    print("\n🏗️ Testing data structures...")
    
    try:
        from scraper_optimized import CourseInfo, SectionInfo, OptimizedCourseData
        
        # Test CourseInfo
        course = CourseInfo(
            course_code="CMPSC 121",
            course_title="Programming Techniques",
            subject="CMPSC",
            catalog_number="121",
            units="4.0",
            course_description="Test description"
        )
        
        # Test SectionInfo
        section1 = SectionInfo(
            section="001",
            class_number="12345",
            instructor="Test Instructor",
            class_capacity=150,
            enrollment_total=140,
            available_seats=10,
            campus="UP"
        )
        
        section2 = SectionInfo(
            section="002", 
            class_number="12346",
            instructor="Another Instructor",
            class_capacity=120,
            enrollment_total=115,
            available_seats=5,
            campus="UP"
        )
        
        # Test OptimizedCourseData
        course_data = OptimizedCourseData(
            course_info=course,
            sections=[section1, section2]
        )
        
        # Test aggregation methods
        total_capacity = course_data.get_total_capacity()
        total_enrollment = course_data.get_total_enrollment()
        available_seats = course_data.get_available_seats()
        section_count = course_data.get_section_count()
        campuses = course_data.get_campuses()
        
        print(f"✅ Data structure test passed:")
        print(f"   Course: {course.course_code}")
        print(f"   Sections: {section_count}")
        print(f"   Total capacity: {total_capacity}")
        print(f"   Total enrollment: {total_enrollment}")
        print(f"   Available seats: {available_seats}")
        print(f"   Campuses: {list(campuses)}")
        
    except Exception as e:
        print(f"❌ Data structure test failed: {e}")

if __name__ == "__main__":
    test_data_structure()
    test_optimized_scraper()
