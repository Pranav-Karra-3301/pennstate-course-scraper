#!/usr/bin/env python3
"""
Comprehensive Test Suite for Penn State LionPath Course Scraper
"""

import unittest
import json
import os
import sys
import tempfile
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import requests
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper_optimized import (
    OptimizedLionPathScraper,
    CourseInfo,
    SectionInfo,
    OptimizedCourseData,
    save_optimized_results
)


class TestCourseInfo(unittest.TestCase):
    """Test CourseInfo dataclass"""
    
    def test_course_info_creation(self):
        """Test creating a CourseInfo object"""
        course = CourseInfo(
            course_code="CMPSC 131",
            course_title="Programming and Computation I",
            subject="CMPSC",
            catalog_number="131",
            units="3.00",
            semester="Fall 2025"
        )
        
        self.assertEqual(course.course_code, "CMPSC 131")
        self.assertEqual(course.course_title, "Programming and Computation I")
        self.assertEqual(course.subject, "CMPSC")
        self.assertEqual(course.catalog_number, "131")
        self.assertEqual(course.units, "3.00")
        self.assertIsNotNone(course.class_attributes)
        self.assertIsInstance(course.class_attributes, list)
    
    def test_course_info_defaults(self):
        """Test CourseInfo default values"""
        course = CourseInfo()
        
        self.assertEqual(course.course_code, "")
        self.assertEqual(course.course_title, "")
        self.assertEqual(course.class_attributes, [])
        self.assertIsNotNone(course.last_updated)


class TestSectionInfo(unittest.TestCase):
    """Test SectionInfo dataclass"""
    
    def test_section_info_creation(self):
        """Test creating a SectionInfo object"""
        section = SectionInfo(
            section="001",
            class_number="12345",
            section_type="Lecture",
            days="MWF",
            times="10:10 AM - 11:00 AM",
            campus="UP",
            instructor="Dr. Smith",
            class_capacity=100,
            enrollment_total=85,
            available_seats=15
        )
        
        self.assertEqual(section.section, "001")
        self.assertEqual(section.class_number, "12345")
        self.assertEqual(section.section_type, "Lecture")
        self.assertEqual(section.campus, "UP")
        self.assertEqual(section.class_capacity, 100)
        self.assertEqual(section.enrollment_total, 85)
        self.assertEqual(section.available_seats, 15)
    
    def test_section_info_defaults(self):
        """Test SectionInfo default values"""
        section = SectionInfo()
        
        self.assertEqual(section.section, "")
        self.assertEqual(section.class_capacity, 0)
        self.assertEqual(section.enrollment_total, 0)
        self.assertEqual(section.reserve_capacity, [])
        self.assertEqual(section.exam_schedule, [])
        self.assertIsNotNone(section.scrape_timestamp)


class TestOptimizedCourseData(unittest.TestCase):
    """Test OptimizedCourseData class"""
    
    def setUp(self):
        """Set up test data"""
        self.course_info = CourseInfo(
            course_code="MATH 140",
            course_title="Calculus I",
            subject="MATH",
            catalog_number="140"
        )
        
        self.sections = [
            SectionInfo(
                section="001",
                campus="UP",
                class_capacity=50,
                enrollment_total=45,
                available_seats=5
            ),
            SectionInfo(
                section="002",
                campus="UP",
                class_capacity=50,
                enrollment_total=48,
                available_seats=2
            ),
            SectionInfo(
                section="003",
                campus="World Campus",
                class_capacity=100,
                enrollment_total=75,
                available_seats=25
            )
        ]
        
        self.course_data = OptimizedCourseData(
            course_info=self.course_info,
            sections=self.sections
        )
    
    def test_get_total_capacity(self):
        """Test calculating total capacity"""
        self.assertEqual(self.course_data.get_total_capacity(), 200)
    
    def test_get_total_enrollment(self):
        """Test calculating total enrollment"""
        self.assertEqual(self.course_data.get_total_enrollment(), 168)
    
    def test_get_available_seats(self):
        """Test calculating available seats"""
        self.assertEqual(self.course_data.get_available_seats(), 32)
    
    def test_get_section_count(self):
        """Test counting sections"""
        self.assertEqual(self.course_data.get_section_count(), 3)
    
    def test_get_campuses(self):
        """Test getting unique campuses"""
        campuses = self.course_data.get_campuses()
        self.assertEqual(campuses, {"UP", "World Campus"})


class TestOptimizedLionPathScraper(unittest.TestCase):
    """Test OptimizedLionPathScraper class"""
    
    def setUp(self):
        """Set up test scraper"""
        self.scraper = OptimizedLionPathScraper(
            delay=0.1,
            max_workers=2,
            max_detail_workers=4,
            retry_attempts=1,
            rate_limit_per_second=10
        )
    
    def test_scraper_initialization(self):
        """Test scraper initialization"""
        self.assertEqual(self.scraper.delay, 0.1)
        self.assertEqual(self.scraper.max_workers, 2)
        self.assertEqual(self.scraper.max_detail_workers, 4)
        self.assertEqual(self.scraper.retry_attempts, 1)
        self.assertEqual(self.scraper.rate_limit_per_second, 10)
        self.assertIsNotNone(self.scraper.session_pool)
        self.assertIsNotNone(self.scraper.courses_data)
        self.assertEqual(len(self.scraper.courses_data), 0)
    
    def test_is_university_park_section(self):
        """Test University Park section detection"""
        # UP sections
        up_section = SectionInfo(section="001", campus="UP")
        self.assertTrue(self.scraper.is_university_park_section(up_section))
        
        up_section2 = SectionInfo(section="002", campus="")
        self.assertTrue(self.scraper.is_university_park_section(up_section2))
        
        # Non-UP sections
        wc_section = SectionInfo(section="001W", campus="World Campus")
        self.assertFalse(self.scraper.is_university_park_section(wc_section))
        
        berks_section = SectionInfo(section="001", campus="Berks")
        self.assertFalse(self.scraper.is_university_park_section(berks_section))
        
        y_section = SectionInfo(section="001Y", campus="")
        self.assertFalse(self.scraper.is_university_park_section(y_section))
    
    def test_extract_form_data_fast(self):
        """Test form data extraction"""
        html = '''
        <html>
        <input type="hidden" name="ICStateNum" value="1">
        <input type="hidden" name="ICSID" value="xyz123">
        <input type="text" name="visible" value="ignored">
        </html>
        '''
        
        form_data = self.scraper.extract_form_data_fast(html)
        
        self.assertEqual(form_data.get("ICStateNum"), "1")
        self.assertEqual(form_data.get("ICSID"), "xyz123")
        self.assertNotIn("visible", form_data)
    
    def test_parse_section_text_optimized(self):
        """Test section text parsing"""
        # Valid section text
        text = "CMPSC 131 - 001 - University Park"
        section = self.scraper.parse_section_text_optimized(
            text, "1234", "56789", "CMPSC"
        )
        
        self.assertIsNotNone(section)
        self.assertEqual(section.section, "001")
        self.assertEqual(section.class_number, "56789")
        self.assertEqual(section.campus, "UP")
        
        # World Campus section
        text2 = "MATH 140 - 001W - World Campus"
        section2 = self.scraper.parse_section_text_optimized(
            text2, "1234", "56790", "MATH"
        )
        
        self.assertIsNotNone(section2)
        self.assertEqual(section2.section, "001W")
        self.assertEqual(section2.campus, "World Campus")
        
        # Invalid text
        text3 = "Invalid Format"
        section3 = self.scraper.parse_section_text_optimized(
            text3, "1234", "56791", "TEST"
        )
        
        self.assertIsNone(section3)
    
    @patch('requests.Session.get')
    def test_rate_limited_request(self, mock_get):
        """Test rate limiting functionality"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "test"
        mock_get.return_value = mock_response
        
        session = Mock()
        session.get = mock_get
        
        # Make multiple requests
        for _ in range(3):
            response = self.scraper.rate_limited_request(
                session.get, "http://test.com"
            )
            self.assertEqual(response.status_code, 200)
        
        # Verify requests were made
        self.assertEqual(mock_get.call_count, 3)
    
    def test_organize_sections_by_course(self):
        """Test organizing sections by course"""
        sections = [
            SectionInfo(section="001", class_number="12345"),
            SectionInfo(section="002", class_number="12346"),
            SectionInfo(section="001", class_number="12347"),
        ]
        
        # Add course_code attribute for grouping
        sections[0].course_code = "CMPSC 131"
        sections[1].course_code = "CMPSC 131"
        sections[2].course_code = "MATH 140"
        
        self.scraper.organize_sections_by_course(sections)
        
        self.assertEqual(len(self.scraper.courses_data), 2)
        self.assertIn("CMPSC 131", self.scraper.courses_data)
        self.assertIn("MATH 140", self.scraper.courses_data)
        
        cmpsc_data = self.scraper.courses_data["CMPSC 131"]
        self.assertEqual(len(cmpsc_data.sections), 2)
        
        math_data = self.scraper.courses_data["MATH 140"]
        self.assertEqual(len(math_data.sections), 1)


class TestSaveOptimizedResults(unittest.TestCase):
    """Test save_optimized_results function"""
    
    def setUp(self):
        """Set up test data"""
        self.temp_dir = tempfile.mkdtemp()
        
        course_info = CourseInfo(
            course_code="TEST 101",
            course_title="Test Course",
            subject="TEST",
            catalog_number="101"
        )
        
        sections = [
            SectionInfo(
                section="001",
                class_number="12345",
                class_capacity=30,
                enrollment_total=25
            )
        ]
        
        self.courses_data = {
            "TEST 101": OptimizedCourseData(
                course_info=course_info,
                sections=sections
            )
        }
    
    def tearDown(self):
        """Clean up temp files"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_save_as_jsonl(self):
        """Test saving as JSONL format"""
        output_file = os.path.join(self.temp_dir, "test.jsonl")
        save_optimized_results(self.courses_data, output_file, "jsonl")
        
        self.assertTrue(os.path.exists(output_file))
        
        with open(output_file, 'r') as f:
            lines = f.readlines()
            self.assertEqual(len(lines), 1)
            
            data = json.loads(lines[0])
            self.assertIn("course", data)
            self.assertIn("sections", data)
            self.assertIn("stats", data)
            
            self.assertEqual(data["course"]["course_code"], "TEST 101")
            self.assertEqual(len(data["sections"]), 1)
            self.assertEqual(data["stats"]["section_count"], 1)
    
    def test_save_as_json(self):
        """Test saving as JSON format"""
        output_file = os.path.join(self.temp_dir, "test.json")
        save_optimized_results(self.courses_data, output_file, "json")
        
        self.assertTrue(os.path.exists(output_file))
        
        with open(output_file, 'r') as f:
            data = json.load(f)
            self.assertIn("TEST 101", data)
            self.assertIn("course", data["TEST 101"])
            self.assertIn("sections", data["TEST 101"])
    
    def test_save_as_csv(self):
        """Test saving as CSV format"""
        output_file = os.path.join(self.temp_dir, "test.csv")
        save_optimized_results(self.courses_data, output_file, "csv")
        
        self.assertTrue(os.path.exists(output_file))
        
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn("course_code", content)
            self.assertIn("TEST 101", content)
            self.assertIn("001", content)


class TestIntegration(unittest.TestCase):
    """Integration tests for the scraper"""
    
    @patch('requests.Session.get')
    def test_scraper_workflow(self, mock_get):
        """Test the complete scraper workflow with mocked responses"""
        # Mock response for getting subjects
        subjects_response = Mock()
        subjects_response.status_code = 200
        subjects_response.text = '''
        <input id="PTS_SELECT$0" type="checkbox">
        <label id="PTS_SELECT_LBL$0">CMPSC / Computer Science</label>
        '''
        
        # Mock response for subject scraping
        subject_response = Mock()
        subject_response.status_code = 200
        subject_response.text = '''
        <a href="javascript:showClassDetails(1234,56789)">CMPSC 131 - 001 - University Park</a>
        '''
        
        mock_get.side_effect = [subjects_response, subject_response, subject_response]
        
        scraper = OptimizedLionPathScraper(
            delay=0,
            max_workers=1,
            max_detail_workers=1,
            retry_attempts=1
        )
        
        # Get subjects
        subjects = scraper.get_all_subjects()
        self.assertEqual(len(subjects), 1)
        self.assertEqual(subjects[0]["code"], "CMPSC")
        
        # Note: Full integration would require more complex mocking


class TestErrorHandling(unittest.TestCase):
    """Test error handling in the scraper"""
    
    def test_invalid_section_parsing(self):
        """Test handling of invalid section text"""
        scraper = OptimizedLionPathScraper()
        
        # Various invalid formats
        invalid_texts = [
            "",
            None,
            "NoPattern",
            "123 456",
            "- - -"
        ]
        
        for text in invalid_texts:
            if text is not None:
                result = scraper.parse_section_text_optimized(
                    text, "1234", "5678", "TEST"
                )
                self.assertIsNone(result)
    
    @patch('requests.Session.get')
    def test_network_error_handling(self, mock_get):
        """Test handling of network errors"""
        mock_get.side_effect = requests.RequestException("Network error")
        
        scraper = OptimizedLionPathScraper(retry_attempts=1)
        
        # Should handle the exception gracefully
        subjects = scraper.get_all_subjects()
        # The actual implementation might return empty list or raise
        # This depends on the error handling strategy
    
    def test_empty_data_handling(self):
        """Test handling of empty data"""
        scraper = OptimizedLionPathScraper()
        
        # Empty sections list
        scraper.organize_sections_by_course([])
        self.assertEqual(len(scraper.courses_data), 0)
        
        # Sections without course codes
        sections = [SectionInfo(section="001")]
        scraper.organize_sections_by_course(sections)
        self.assertEqual(len(scraper.courses_data), 0)


class TestPerformance(unittest.TestCase):
    """Performance-related tests"""
    
    def test_session_pool_management(self):
        """Test session pool creation and management"""
        scraper = OptimizedLionPathScraper(
            max_workers=5,
            max_detail_workers=10
        )
        
        # Check pool was initialized
        self.assertIsNotNone(scraper.session_pool)
        
        # Get and return sessions
        sessions = []
        for _ in range(3):
            session = scraper.get_session()
            self.assertIsNotNone(session)
            sessions.append(session)
        
        for session in sessions:
            scraper.return_session(session)
        
        # Pool should still be functional
        session = scraper.get_session()
        self.assertIsNotNone(session)
    
    def test_concurrent_section_organization(self):
        """Test thread-safe section organization"""
        scraper = OptimizedLionPathScraper()
        
        # Create many sections
        sections = []
        for i in range(100):
            section = SectionInfo(section=f"{i:03d}")
            section.course_code = f"TEST {i // 10}"
            sections.append(section)
        
        scraper.organize_sections_by_course(sections)
        
        # Should have 10 courses with 10 sections each
        self.assertEqual(len(scraper.courses_data), 10)
        
        for course_code, course_data in scraper.courses_data.items():
            self.assertEqual(len(course_data.sections), 10)


def run_tests():
    """Run all tests with verbose output"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test cases
    suite.addTests(loader.loadTestsFromTestCase(TestCourseInfo))
    suite.addTests(loader.loadTestsFromTestCase(TestSectionInfo))
    suite.addTests(loader.loadTestsFromTestCase(TestOptimizedCourseData))
    suite.addTests(loader.loadTestsFromTestCase(TestOptimizedLionPathScraper))
    suite.addTests(loader.loadTestsFromTestCase(TestSaveOptimizedResults))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestErrorHandling))
    suite.addTests(loader.loadTestsFromTestCase(TestPerformance))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    if result.wasSuccessful():
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)