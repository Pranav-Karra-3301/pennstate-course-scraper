#!/usr/bin/env python3
"""
Penn State LionPath Course Scraper - Optimized Data Structure
Organizes data to minimize redundancy by separating course info from section info
"""

import requests
import json
import re
import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Set, Any, Tuple
import logging
from pathlib import Path
import argparse
from urllib.parse import urljoin, urlparse, parse_qs, unquote
import sys
from datetime import datetime
from bs4 import BeautifulSoup
import concurrent.futures
from threading import Lock, BoundedSemaphore
import random
from collections import defaultdict
from queue import Queue, Empty

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('psu_scraper_optimized.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class CourseInfo:
    """Course-level information that stays constant across sections"""
    course_code: str = ""
    course_title: str = ""
    subject: str = ""
    catalog_number: str = ""
    
    # Academic details
    units: str = ""
    career: str = ""
    grading: str = ""
    component: str = ""
    
    # Course content
    course_description: str = ""
    enrollment_requirements: str = ""
    enforced_concurrent: str = ""
    class_attributes: List[str] = None
    academic_organization: str = ""
    
    # Course-level requirements and notes
    course_notes: str = ""
    textbook_info: str = ""
    
    # Metadata
    semester: str = ""
    last_updated: str = ""
    
    def __post_init__(self):
        if self.class_attributes is None:
            self.class_attributes = []

@dataclass
class SectionInfo:
    """Section-specific information that varies per section"""
    section: str = ""
    class_number: str = ""
    section_type: str = ""  # Lecture, Lab, Recitation, etc.
    
    # Scheduling
    days: str = ""
    times: str = ""
    start_time: str = ""
    end_time: str = ""
    start_date: str = ""
    end_date: str = ""
    meeting_dates: str = ""
    
    # Location
    campus: str = ""
    location: str = ""
    building: str = ""
    room: str = ""
    instruction_mode: str = ""
    
    # Instructor
    instructor: str = ""
    instructor_email: str = ""
    
    # Enrollment - General Capacity
    class_capacity: int = 0
    enrollment_total: int = 0
    available_seats: int = 0
    waitlist_capacity: int = 0
    waitlist_total: int = 0
    status: str = ""
    
    # Reserve Capacity details
    reserve_capacity: List[Dict[str, Any]] = None
    
    # Section-specific details
    add_consent: str = ""
    drop_consent: str = ""
    class_notes: str = ""
    
    # Exam schedule
    exam_schedule: List[Dict[str, str]] = None
    
    # Metadata
    course_url: str = ""
    detail_url: str = ""
    scrape_timestamp: str = ""
    
    def __post_init__(self):
        if not self.scrape_timestamp:
            self.scrape_timestamp = datetime.now().isoformat()
        if self.reserve_capacity is None:
            self.reserve_capacity = []
        if self.exam_schedule is None:
            self.exam_schedule = []

@dataclass
class OptimizedCourseData:
    """Optimized course data structure separating course from section info"""
    course_info: CourseInfo
    sections: List[SectionInfo]
    
    def get_total_capacity(self) -> int:
        """Get total capacity across all sections"""
        return sum(section.class_capacity for section in self.sections)
    
    def get_total_enrollment(self) -> int:
        """Get total enrollment across all sections"""
        return sum(section.enrollment_total for section in self.sections)
    
    def get_available_seats(self) -> int:
        """Get total available seats across all sections"""
        return sum(section.available_seats for section in self.sections)
    
    def get_section_count(self) -> int:
        """Get number of sections"""
        return len(self.sections)
    
    def get_campuses(self) -> Set[str]:
        """Get unique campuses where course is offered"""
        return set(section.campus for section in self.sections if section.campus)

class OptimizedLionPathScraper:
    """Optimized scraper with improved data structure"""
    
    def __init__(self, 
                 delay: float = 0.2, 
                 max_workers: int = 16, 
                 max_detail_workers: int = 50,
                 retry_attempts: int = 2,
                 rate_limit_per_second: int = 20):
        
        self.delay = delay
        self.max_workers = max_workers
        self.max_detail_workers = max_detail_workers
        self.retry_attempts = retry_attempts
        self.rate_limit_per_second = rate_limit_per_second
        
        # Rate limiting
        self.rate_limiter = BoundedSemaphore(rate_limit_per_second)
        self.last_request_times = []
        self.request_lock = Lock()
        
        # Data storage - organized by course code
        self.courses_data = {}  # Dict[str, OptimizedCourseData]
        self.data_lock = Lock()
        
        # Session pool
        self.session_pool = Queue()
        self.init_session_pool(max_workers + max_detail_workers)
        
        # URLs
        self.base_url = "https://public.lionpath.psu.edu"
        self.search_url = "https://public.lionpath.psu.edu/psc/CSPRD/EMPLOYEE/SA/c/PE_SR175_PUBLIC.PE_SR175_CLS_SRCH.GBL"
        
        # Statistics
        self.stats = {
            'total_subjects': 0,
            'processed_subjects': 0,
            'unique_courses': 0,
            'total_sections': 0,
            'detailed_sections': 0,
            'failed_subjects': [],
            'failed_details': 0,
            'start_time': None,
            'end_time': None
        }
    
    def init_session_pool(self, pool_size: int):
        """Initialize a pool of session objects for reuse"""
        for _ in range(pool_size):
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
            })
            self.session_pool.put(session)
    
    def get_session(self) -> requests.Session:
        """Get a session from the pool"""
        try:
            return self.session_pool.get_nowait()
        except Empty:
            session = requests.Session()
            session.headers.update({
                'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Connection': 'keep-alive',
            })
            return session
    
    def return_session(self, session: requests.Session):
        """Return a session to the pool"""
        try:
            self.session_pool.put_nowait(session)
        except:
            pass
    
    def rate_limited_request(self, method, *args, **kwargs):
        """Make a rate-limited request"""
        with self.request_lock:
            now = time.time()
            self.last_request_times = [t for t in self.last_request_times if now - t < 1.0]
            
            if len(self.last_request_times) >= self.rate_limit_per_second:
                sleep_time = 1.0 - (now - self.last_request_times[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            self.last_request_times.append(now)
        
        return method(*args, **kwargs)
    
    def scrape_all_courses(self, campus_filter: str = "UP", max_subjects: int = None) -> Dict[str, OptimizedCourseData]:
        """Main scraping method with optimized data structure"""
        self.stats['start_time'] = datetime.now()
        logger.info("🚀 Starting Optimized LionPath scraping...")
        
        try:
            # Get all subjects
            logger.info("📚 Getting all subject codes...")
            subjects = self.get_all_subjects()
            self.stats['total_subjects'] = len(subjects)
            logger.info(f"Found {len(subjects)} subjects")
            
            if max_subjects:
                subjects = subjects[:max_subjects]
                logger.info(f"Limited to first {max_subjects} subjects for testing")
            
            # Scrape subjects in parallel
            logger.info("🏃‍♂️ Starting parallel subject scraping...")
            raw_sections = self.scrape_subjects_parallel(subjects, campus_filter)
            
            # Organize data by course
            logger.info("📊 Organizing sections by course...")
            self.organize_sections_by_course(raw_sections)
            
            # Extract detailed information for each unique course
            logger.info(f"🔍 Extracting course details for {len(self.courses_data)} unique courses...")
            self.extract_course_details_parallel()
            
            # Update statistics
            self.stats['unique_courses'] = len(self.courses_data)
            self.stats['total_sections'] = sum(len(course_data.sections) for course_data in self.courses_data.values())
            self.stats['detailed_sections'] = sum(
                len([s for s in course_data.sections if s.class_capacity > 0])
                for course_data in self.courses_data.values()
            )
            self.stats['end_time'] = datetime.now()
            
            self.log_final_stats()
            return self.courses_data
            
        except Exception as e:
            logger.error(f"💥 Scraping failed: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return {}
    
    def scrape_subjects_parallel(self, subjects: List[Dict], campus_filter: str) -> List[SectionInfo]:
        """Scrape all subjects in parallel, returning raw section data"""
        all_sections = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_subject = {
                executor.submit(self.scrape_subject_optimized, subject): subject 
                for subject in subjects
            }
            
            for future in concurrent.futures.as_completed(future_to_subject):
                subject = future_to_subject[future]
                try:
                    sections = future.result()
                    if sections:
                        # Filter for campus if requested
                        if campus_filter.upper() == "UP":
                            up_sections = [s for s in sections if self.is_university_park_section(s)]
                            if up_sections:
                                all_sections.extend(up_sections)
                                logger.info(f"✅ {subject.get('code', 'unknown')}: {len(up_sections)} UP sections")
                        else:
                            all_sections.extend(sections)
                            logger.info(f"✅ {subject.get('code', 'unknown')}: {len(sections)} sections")
                    
                    self.stats['processed_subjects'] += 1
                    
                except Exception as e:
                    self.stats['failed_subjects'].append(subject.get('code', 'unknown'))
                    logger.error(f"❌ {subject.get('code', 'unknown')} failed: {e}")
        
        logger.info(f"📊 Subject scraping complete: {len(all_sections)} total sections found")
        return all_sections
    
    def organize_sections_by_course(self, sections: List[SectionInfo]):
        """Organize sections by course code, creating course-level data"""
        course_sections = defaultdict(list)
        
        # Group sections by course code
        for section in sections:
            course_code = getattr(section, 'course_code', None)
            if not course_code:
                # Skip sections without course code
                logger.debug(f"Skipping section without course code: {section.class_number}")
                continue
            
            # Remove the temporary course_code attribute before storing
            if hasattr(section, 'course_code'):
                delattr(section, 'course_code')
            
            course_sections[course_code].append(section)
        
        # Create optimized course data structures
        for course_code, course_sections_list in course_sections.items():
            if not course_sections_list:
                continue
            
            # Create course info from course code
            course_info = CourseInfo(
                course_code=course_code,
                semester="Fall 2025",
                last_updated=datetime.now().isoformat()
            )
            
            # Extract subject and catalog number from course code
            course_match = re.match(r'^([A-Z]+-?[A-Z]+)\s+(\d+[A-Z]*)', course_code)
            if course_match:
                course_info.subject = course_match.group(1)
                course_info.catalog_number = course_match.group(2)
            
            # Create optimized course data
            self.courses_data[course_code] = OptimizedCourseData(
                course_info=course_info,
                sections=course_sections_list
            )
        
        logger.info(f"📊 Organized {len(sections)} sections into {len(self.courses_data)} unique courses")
    
    def extract_course_details_parallel(self):
        """Extract detailed course information for each unique course"""
        # We'll enhance this to get course-level details and section-level details separately
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_detail_workers) as executor:
            future_to_course = {
                executor.submit(self.enhance_course_data, course_code, course_data): course_code
                for course_code, course_data in self.courses_data.items()
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_course):
                course_code = future_to_course[future]
                try:
                    enhanced_course_data = future.result()
                    if enhanced_course_data:
                        self.courses_data[course_code] = enhanced_course_data
                    
                    completed += 1
                    if completed % 50 == 0:
                        logger.info(f"🔍 Course enhancement progress: {completed}/{len(self.courses_data)}")
                        
                except Exception as e:
                    self.stats['failed_details'] += 1
                    logger.debug(f"Failed to enhance {course_code}: {e}")
        
        logger.info(f"✅ Course enhancement complete: {len(self.courses_data)} courses processed")
    
    def enhance_course_data(self, course_code: str, course_data: OptimizedCourseData) -> OptimizedCourseData:
        """Enhance course data with detailed information"""
        session = self.get_session()
        try:
            # Get detailed course information (course-level details)
            enhanced_course_info = self.get_course_level_details(session, course_data.course_info, course_data.sections[0])
            
            # Get enhanced section details
            enhanced_sections = []
            for section in course_data.sections:
                enhanced_section = self.get_section_details(session, section)
                enhanced_sections.append(enhanced_section)
            
            return OptimizedCourseData(
                course_info=enhanced_course_info,
                sections=enhanced_sections
            )
            
        finally:
            self.return_session(session)
    
    def get_course_level_details(self, session: requests.Session, course_info: CourseInfo, sample_section: SectionInfo) -> CourseInfo:
        """Get course-level details that are consistent across sections"""
        try:
            # Use the sample section to get course details
            if "showClassDetails" in sample_section.course_url:
                match = re.search(r'showClassDetails\((\d+),(\d+)\)', sample_section.course_url)
                if match:
                    strm, class_nbr = match.groups()
                    
                    detail_url = f"{self.base_url}/psc/CSPRD/EMPLOYEE/SA/c/SA_LEARNER_SERVICES.SSR_SSENRL_DETAIL.GBL"
                    params = {
                        'Page': 'SSR_SSENRL_DETAIL',
                        'Action': 'A',
                        'STRM': strm,
                        'CLASS_NBR': class_nbr,
                        'ACAD_CAREER': 'UGRD',
                    }
                    
                    response = self.rate_limited_request(
                        session.get,
                        detail_url,
                        params=params,
                        timeout=8
                    )
                    
                    if response.status_code == 200:
                        enhanced_info = self.parse_course_level_info(response.text, course_info)
                        return enhanced_info
            
            return course_info
            
        except Exception as e:
            logger.debug(f"Failed to get course-level details: {e}")
            return course_info
    
    def get_section_details(self, session: requests.Session, section: SectionInfo) -> SectionInfo:
        """Get section-specific details by making request for detailed info"""
        try:
            # Extract strm and class_nbr from course_url
            if "showClassDetails" in section.course_url:
                match = re.search(r'showClassDetails\((\d+),(\d+)\)', section.course_url)
                if match:
                    strm, class_nbr = match.groups()
                    
                    # Make request for detailed section info
                    form_data = self.extract_form_data_fast(session.cookies.get('PS_TOKEN', ''))
                    form_data.update({
                        'ICAction': f'DERIVED_CLSRCH_SSR_CLASSNAME_LONG${class_nbr}',
                        'CLASS_SRCH_WRK2_STRM$273$': strm,
                        'SSR_CLS_DTL_WRK_CLASS_NBR': class_nbr
                    })
                    
                    response = self.rate_limited_request(
                        session.post,
                        self.base_url,
                        data=form_data,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        return self.parse_detailed_section_info(response.text, section)
            
            return section
            
        except Exception as e:
            logger.debug(f"Failed to get section details for {section.class_number}: {e}")
            return section
    
    def parse_detailed_section_info(self, html: str, base_section: SectionInfo) -> SectionInfo:
        """Parse detailed section information from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text()
            
            # Copy base section info
            section = SectionInfo(**asdict(base_section))
            
            # Extract status (Open/Closed/Waitlist)
            status_match = re.search(r'Status[:\s]*([^\n]+)', text, re.IGNORECASE)
            if status_match:
                section.status = status_match.group(1).strip()
            
            # Extract section type (Lecture/Lab/Recitation)
            type_patterns = [
                r'(?:Class Type|Component)[:\s]*([^\n]+)',
                r'Section Type[:\s]*([^\n]+)',
            ]
            for pattern in type_patterns:
                type_match = re.search(pattern, text, re.IGNORECASE)
                if type_match:
                    section.section_type = type_match.group(1).strip()
                    break
            
            # Extract meeting days and times
            days_match = re.search(r'Days?[:\s]*([^\n]+)', text, re.IGNORECASE)
            if days_match:
                days_text = days_match.group(1).strip()
                # Clean up days
                days_text = re.sub(r'[^\w\s]', '', days_text)
                section.days = days_text
            
            # Extract times with various patterns
            time_patterns = [
                r'Times?[:\s]*(\d{1,2}:\d{2}\s*[AP]M)\s*[-to]+\s*(\d{1,2}:\d{2}\s*[AP]M)',
                r'(\d{1,2}:\d{2}[AP]M)\s*-\s*(\d{1,2}:\d{2}[AP]M)',
            ]
            
            for pattern in time_patterns:
                times_match = re.search(pattern, text, re.IGNORECASE)
                if times_match:
                    section.start_time = times_match.group(1).strip()
                    section.end_time = times_match.group(2).strip()
                    section.times = f"{section.start_time} to {section.end_time}"
                    break
            
            # Extract meeting dates
            dates_patterns = [
                r'(?:Meeting Dates?|Dates?)[:\s]*(\d{1,2}/\d{1,2}/\d{4})\s*[-to]+\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'Start Date[:\s]*(\d{1,2}/\d{1,2}/\d{4}).*?End Date[:\s]*(\d{1,2}/\d{1,2}/\d{4})',
            ]
            
            for pattern in dates_patterns:
                dates_match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                if dates_match:
                    section.start_date = dates_match.group(1)
                    section.end_date = dates_match.group(2)
                    section.meeting_dates = f"{section.start_date} - {section.end_date}"
                    break
            
            # Extract instruction mode
            mode_patterns = [
                r'Instruction Mode[:\s]*([^\n]+)',
                r'Delivery Mode[:\s]*([^\n]+)',
            ]
            
            for pattern in mode_patterns:
                mode_match = re.search(pattern, text, re.IGNORECASE)
                if mode_match:
                    section.instruction_mode = mode_match.group(1).strip()
                    break
            
            # Extract location information
            location_match = re.search(r'(?:Location|Campus)[:\s]*([^\n]+)', text, re.IGNORECASE)
            if location_match:
                section.location = location_match.group(1).strip()
            
            # Extract building and room
            building_match = re.search(r'Building[:\s]*([^\n]+)', text, re.IGNORECASE)
            if building_match:
                section.building = building_match.group(1).strip()
            
            room_match = re.search(r'Room[:\s]*([^\n]+)', text, re.IGNORECASE)
            if room_match:
                section.room = room_match.group(1).strip()
            
            # Extract instructor information
            instructor_patterns = [
                r'Instructor[:\s]*([^\n]+)',
                r'Professor[:\s]*([^\n]+)',
                r'Taught by[:\s]*([^\n]+)',
            ]
            
            for pattern in instructor_patterns:
                instructor_match = re.search(pattern, text, re.IGNORECASE)
                if instructor_match:
                    section.instructor = instructor_match.group(1).strip()
                    # Clean up instructor name
                    section.instructor = re.sub(r'\s+', ' ', section.instructor)
                    break
            
            # Extract enrollment information
            capacity_match = re.search(r'Class Capacity[:\s]*(\d+)', text, re.IGNORECASE)
            if capacity_match:
                section.class_capacity = int(capacity_match.group(1))
            
            enrolled_patterns = [
                r'(?:Enrollment Total|Total Enrolled|Enrolled)[:\s]*(\d+)',
                r'Current Enrollment[:\s]*(\d+)',
            ]
            
            for pattern in enrolled_patterns:
                enrolled_match = re.search(pattern, text, re.IGNORECASE)
                if enrolled_match:
                    section.enrollment_total = int(enrolled_match.group(1))
                    break
            
            available_match = re.search(r'(?:Available Seats?|Seats Available)[:\s]*(\d+)', text, re.IGNORECASE)
            if available_match:
                section.available_seats = int(available_match.group(1))
            elif section.class_capacity > 0 and section.enrollment_total >= 0:
                # Calculate if not provided
                section.available_seats = max(0, section.class_capacity - section.enrollment_total)
            
            # Extract waitlist information
            waitlist_cap_match = re.search(r'Wait ?List Capacity[:\s]*(\d+)', text, re.IGNORECASE)
            if waitlist_cap_match:
                section.waitlist_capacity = int(waitlist_cap_match.group(1))
            
            waitlist_total_match = re.search(r'Wait ?List Total[:\s]*(\d+)', text, re.IGNORECASE)
            if waitlist_total_match:
                section.waitlist_total = int(waitlist_total_match.group(1))
            
            # Extract class notes
            notes_patterns = [
                r'Class Notes?[:\s]*([^\n]+(?:\n(?![A-Z][a-z]+:)[^\n]+)*)',
                r'Section Notes?[:\s]*([^\n]+)',
            ]
            
            for pattern in notes_patterns:
                notes_match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if notes_match:
                    notes_text = notes_match.group(1).strip()
                    if "No Class Notes" not in notes_text and notes_text:
                        section.class_notes = re.sub(r'\s+', ' ', notes_text)
                        break
            
            # Extract consent requirements
            add_consent_match = re.search(r'Add Consent[:\s]*([^\n]+)', text, re.IGNORECASE)
            if add_consent_match:
                section.add_consent = add_consent_match.group(1).strip()
            
            drop_consent_match = re.search(r'Drop Consent[:\s]*([^\n]+)', text, re.IGNORECASE)
            if drop_consent_match:
                section.drop_consent = drop_consent_match.group(1).strip()
            
            # Update timestamp
            section.scrape_timestamp = datetime.now().isoformat()
            
            return section
            
        except Exception as e:
            logger.debug(f"Error parsing detailed section info: {e}")
            return base_section
    
    def parse_course_level_info(self, html: str, base_course_info: CourseInfo) -> CourseInfo:
        """Parse comprehensive course-level information from detailed page"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text()
            
            # Create enhanced course info
            enhanced_info = CourseInfo(**asdict(base_course_info))
            
            # Extract course title more comprehensively
            title_patterns = [
                r'([A-Z]+-?[A-Z]+\s+\d+[A-Z]*)\s+(.+?)(?:\n|Status|Units|$)',
                r'Course:\s*([A-Z]+-?[A-Z]+\s+\d+[A-Z]*)\s+(.+?)(?:\n|$)',
                r'([A-Z]+-?[A-Z]+\s+\d+[A-Z]*)\s*-\s*(.+?)(?:\n|$)',
            ]
            
            for pattern in title_patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    enhanced_info.course_code = match.group(1).strip()
                    title = match.group(2).strip()
                    # Clean up title
                    title = re.sub(r'\s+', ' ', title)
                    if len(title) > 3 and not title.startswith('Section'):
                        enhanced_info.course_title = title
                        break
            
            # Extract units (3.00)
            units_match = re.search(r'Units?[:\s]*(\d+\.?\d*)', text, re.IGNORECASE)
            if units_match:
                enhanced_info.units = units_match.group(1)
            
            # Extract career (Undergraduate/Graduate)
            career_match = re.search(r'Career[:\s]*([^\n]+)', text, re.IGNORECASE)
            if career_match:
                enhanced_info.career = career_match.group(1).strip()
            
            # Extract grading
            grading_match = re.search(r'Grading[:\s]*([^\n]+)', text, re.IGNORECASE)
            if grading_match:
                enhanced_info.grading = grading_match.group(1).strip()
            
            # Extract component (Lecture/Lab/etc)
            component_match = re.search(r'Component[:\s]*([^\n]+)', text, re.IGNORECASE)
            if component_match:
                enhanced_info.component = component_match.group(1).strip()
            
            # Extract course description - improved pattern
            desc_patterns = [
                r'Course Description[:\s]*\n([^]+?)(?:\n\n|\nEnrollment|\nClass Notes|$)',
                r'Description[:\s]*\n([^]+?)(?:\n\n|\nEnrollment|\nClass Notes|$)',
                r'(?:Course Description|Description)[:\s]*([^\n]+(?:\n(?![A-Z][a-z]+:)[^\n]+)*?)(?:\n\n|$)',
            ]
            
            for pattern in desc_patterns:
                desc_match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE | re.DOTALL)
                if desc_match:
                    desc_text = desc_match.group(1).strip()
                    # Clean up the description
                    desc_text = re.sub(r'\s+', ' ', desc_text)
                    desc_text = re.sub(r'\n+', ' ', desc_text)
                    if len(desc_text) > 50:  # Only use if it's a substantial description
                        enhanced_info.course_description = desc_text
                        break
            
            # Extract enrollment requirements - improved
            req_patterns = [
                r'Enrollment Requirements?[:\s]*([^\n]+(?:\n(?![A-Z][a-z]+:)[^\n]+)*)',
                r'Prerequisites?[:\s]*([^\n]+(?:\n(?![A-Z][a-z]+:)[^\n]+)*)',
                r'Enforced Prerequisites?[:\s]*([^\n]+)',
            ]
            
            for pattern in req_patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    req_text = match.group(1).strip()
                    req_text = re.sub(r'\s+', ' ', req_text)
                    if 'Prerequisites' in pattern:
                        if not enhanced_info.enrollment_requirements:
                            enhanced_info.enrollment_requirements = req_text
                    else:
                        enhanced_info.enrollment_requirements = req_text
            
            # Extract enforced concurrent
            concurrent_match = re.search(r'Enforced Concurrent[:\s]*([^\n]+)', text, re.IGNORECASE)
            if concurrent_match:
                enhanced_info.enforced_concurrent = concurrent_match.group(1).strip()
            
            # Extract class attributes - handle multiple
            attr_patterns = [
                r'Class Attributes?[:\s]*([^\n]+(?:\n(?![A-Z][a-z]+:)[^\n]+)*)',
                r'Attributes?[:\s]*([^\n]+)',
            ]
            
            for pattern in attr_patterns:
                attr_match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if attr_match:
                    attrs_text = attr_match.group(1).strip()
                    if "No Class Attributes" not in attrs_text and attrs_text:
                        # Split by common delimiters
                        attrs = re.split(r'[,;]|\n', attrs_text)
                        enhanced_info.class_attributes = [a.strip() for a in attrs if a.strip()]
                        break
            
            # Extract academic organization
            org_match = re.search(r'Academic Organization[:\s]*([^\n]+)', text, re.IGNORECASE)
            if org_match:
                enhanced_info.academic_organization = org_match.group(1).strip()
            
            # Extract course notes
            notes_patterns = [
                r'Course Notes?[:\s]*([^\n]+(?:\n(?![A-Z][a-z]+:)[^\n]+)*)',
                r'Notes?[:\s]*([^\n]+)',
            ]
            
            for pattern in notes_patterns:
                notes_match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if notes_match:
                    notes_text = notes_match.group(1).strip()
                    if "No Course Notes" not in notes_text and notes_text:
                        enhanced_info.course_notes = re.sub(r'\s+', ' ', notes_text)
                        break
            
            # Extract textbook info
            textbook_match = re.search(r'(?:Text Books?|Textbooks?)[:\s]*([^\n]+)', text, re.IGNORECASE)
            if textbook_match:
                enhanced_info.textbook_info = textbook_match.group(1).strip()
            
            enhanced_info.last_updated = datetime.now().isoformat()
            
            return enhanced_info
            
        except Exception as e:
            logger.debug(f"Error parsing course-level info: {e}")
            return base_course_info
    
    # ... (include other helper methods from the previous scraper)
    # I'll include the key methods here but truncate for brevity
    
    def scrape_subject_optimized(self, subject: Dict) -> List[SectionInfo]:
        """Scrape subject returning SectionInfo objects"""
        session = self.get_session()
        try:
            return self._scrape_subject_internal_optimized(session, subject)
        finally:
            self.return_session(session)
    
    def _scrape_subject_internal_optimized(self, session: requests.Session, subject: Dict) -> List[SectionInfo]:
        """Internal optimized subject scraping"""
        subject_code = subject.get('code', 'unknown')
        
        try:
            response = self.rate_limited_request(
                session.get, 
                self.search_url, 
                params={'Page': 'PE_SR175_CLS_SRCH', 'Action': 'U'},
                timeout=10
            )
            response.raise_for_status()
            
            form_data = self.extract_form_data_fast(response.text)
            
            checkbox_id = subject.get('checkbox_id', '')
            if checkbox_id:
                form_data[checkbox_id] = 'Y'
                form_data['ICAction'] = checkbox_id
                
                response = self.rate_limited_request(
                    session.post, 
                    self.search_url, 
                    data=form_data,
                    timeout=10
                )
                response.raise_for_status()
                
                sections = self.parse_sections_optimized(response.text, subject_code)
                return sections
            
            return []
            
        except Exception as e:
            logger.debug(f"Error scraping subject {subject_code}: {e}")
            raise
    
    def parse_sections_optimized(self, html: str, subject_code: str) -> List[SectionInfo]:
        """Parse sections from HTML, returning SectionInfo objects"""
        sections = []
        
        # Fast regex-based parsing for showClassDetails links
        pattern = r'javascript:showClassDetails\((\d+),(\d+)\)[^>]*>([^<]+)<'
        matches = re.findall(pattern, html)
        
        for strm, class_nbr, text in matches:
            try:
                section = self.parse_section_text_optimized(text, strm, class_nbr, subject_code)
                if section:
                    sections.append(section)
            except Exception as e:
                logger.debug(f"Error parsing section text '{text}': {e}")
        
        return sections
    
    def parse_section_text_optimized(self, text: str, strm: str, class_nbr: str, subject_code: str) -> Optional[SectionInfo]:
        """Parse section information from text"""
        try:
            text = text.strip()
            
            # Parse course code
            course_patterns = [
                r'^([A-Z]+-?[A-Z]+)\s+(\d+[A-Z]*)',
                r'^([A-Z]{2,})\s+(\d+[A-Z]*)',
            ]
            
            subject = ""
            catalog_number = ""
            course_code = ""
            
            for pattern in course_patterns:
                match = re.match(pattern, text)
                if match:
                    subject = match.group(1)
                    catalog_number = match.group(2)
                    course_code = f"{subject} {catalog_number}"
                    break
            
            if not course_code:
                return None
            
            # Extract section and campus
            section = ""
            campus = ""
            
            if ' - ' in text:
                parts = text.split(' - ')
                if len(parts) > 1:
                    section_campus = parts[1]
                    
                    # Extract section
                    section_match = re.search(r'(\d{3}[A-Z]*|[A-Z]\d{2}|\d{2,3})', section_campus)
                    if section_match:
                        section = section_match.group(1)
                    
                    # Extract campus
                    campus_names = [
                        'World Campus', 'Berks', 'Abington', 'Altoona', 'Brandywine',
                        'Dubois', 'Erie', 'Fayette', 'Greater Allegheny', 'Harrisburg',
                        'Hazleton', 'Lehigh Valley', 'Mont Alto', 'New Kensington',
                        'Schuylkill', 'Shenango', 'Wilkes-Barre', 'York', 'UP'
                    ]
                    
                    for campus_name in campus_names:
                        if campus_name in section_campus:
                            campus = campus_name
                            break
                    
                    if not campus and not any(c in section_campus.lower() for c in ['world', 'berks', 'y']):
                        campus = 'UP'
            
            # Create section info with course code reference
            section_info = SectionInfo(
                section=section,
                class_number=class_nbr,
                campus=campus,
                course_url=f"showClassDetails({strm},{class_nbr})"
            )
            
            # Store course code for grouping
            section_info.course_code = course_code  # Add this as a temporary attribute
            
            return section_info
            
        except Exception as e:
            logger.debug(f"Error in optimized parsing: {e}")
            return None
    
    def is_university_park_section(self, section: SectionInfo) -> bool:
        """Determine if a section is at University Park"""
        campus = section.campus.upper()
        section_num = section.section.upper()
        
        # Non-UP indicators
        non_up_indicators = [
            'WORLD CAMPUS', 'BERKS', 'ABINGTON', 'ALTOONA', 'BRANDYWINE',
            'DUBOIS', 'ERIE', 'FAYETTE', 'GREATER ALLEGHENY', 'HARRISBURG',
            'HAZLETON', 'LEHIGH VALLEY', 'MONT ALTO', 'NEW KENSINGTON',
            'SCHUYLKILL', 'SHENANGO', 'WILKES-BARRE', 'YORK'
        ]
        
        if any(indicator in campus for indicator in non_up_indicators):
            return False
        
        if section_num.endswith('Y') or section_num.endswith('W'):
            return False
        
        return True
    
    # Include other helper methods...
    def extract_form_data_fast(self, html: str) -> Dict[str, str]:
        """Fast form data extraction using regex"""
        form_data = {}
        hidden_pattern = r'<input[^>]*type=["\']hidden["\'][^>]*name=["\']([^"\']+)["\'][^>]*value=["\']([^"\']*)["\'][^>]*>'
        matches = re.findall(hidden_pattern, html, re.IGNORECASE)
        
        for name, value in matches:
            form_data[name] = value
        
        return form_data
    
    def get_all_subjects(self) -> List[Dict]:
        """Get all subjects quickly"""
        session = self.get_session()
        try:
            response = self.rate_limited_request(
                session.get,
                self.search_url,
                params={'Page': 'PE_SR175_CLS_SRCH', 'Action': 'U'},
                timeout=10
            )
            response.raise_for_status()
            
            subjects = []
            
            # Try multiple patterns to find subject checkboxes
            patterns = [
                # Original pattern
                r'<input[^>]*id="PTS_SELECT\$(\d+)"[^>]*>.*?<label[^>]*id="PTS_SELECT_LBL\$\1"[^>]*>([^<]+)</label>',
                # Alternative pattern for different HTML structure
                r'<input[^>]*name="PTS_SELECT\$(\d+)"[^>]*>.*?<label[^>]*for="PTS_SELECT\$\1"[^>]*>([^<]+)</label>',
                # More flexible pattern
                r'<input[^>]*id="PTS_SELECT\$(\d+)"[^>]*>.*?<label[^>]*>([^<]*)</label>'
            ]
            
            html_text = response.text
            logger.debug(f"HTML length: {len(html_text)}")
            
            for pattern in patterns:
                matches = re.findall(pattern, html_text, re.DOTALL | re.IGNORECASE)
                logger.debug(f"Pattern '{pattern[:50]}...' found {len(matches)} matches")
                
                if matches:
                    for checkbox_num, label_text in matches:
                        label_text = label_text.strip()
                        if '/' in label_text and len(label_text) > 5:  # Basic validation
                            parts = label_text.split('/', 1)
                            code = parts[0].strip()
                            name = parts[1].strip() if len(parts) > 1 else code
                            
                            # Validate code format (should be letters)
                            if re.match(r'^[A-Z]+-?[A-Z]*$', code):
                                subjects.append({
                                    'code': code,
                                    'name': name,
                                    'checkbox_id': f'PTS_SELECT${checkbox_num}',
                                    'full_text': label_text
                                })
                    
                    if subjects:  # If we found subjects, stop trying other patterns
                        break
            
            # If no subjects found, try a simpler approach
            if not subjects:
                logger.debug("No subjects found with regex patterns, trying BeautifulSoup...")
                soup = BeautifulSoup(html_text, 'html.parser')
                
                # Find all checkboxes with PTS_SELECT in ID
                checkboxes = soup.find_all('input', {'id': re.compile(r'PTS_SELECT\$\d+')})
                logger.debug(f"Found {len(checkboxes)} checkboxes with BeautifulSoup")
                
                for checkbox in checkboxes:
                    checkbox_id = checkbox.get('id', '')
                    checkbox_num = checkbox_id.split('$')[-1] if '$' in checkbox_id else ''
                    
                    # Find corresponding label
                    label_id = f'PTS_SELECT_LBL${checkbox_num}'
                    label = soup.find('label', {'id': label_id})
                    
                    if label:
                        label_text = label.get_text(strip=True)
                        if '/' in label_text:
                            parts = label_text.split('/', 1)
                            code = parts[0].strip()
                            name = parts[1].strip() if len(parts) > 1 else code
                            
                            if re.match(r'^[A-Z]+-?[A-Z]*$', code):
                                subjects.append({
                                    'code': code,
                                    'name': name,
                                    'checkbox_id': checkbox_id,
                                    'full_text': label_text
                                })
            
            subjects.sort(key=lambda x: x['code'])
            logger.debug(f"Final subjects count: {len(subjects)}")
            if subjects:
                logger.debug(f"Sample subjects: {[s['code'] for s in subjects[:5]]}")
            
            return subjects
            
        finally:
            self.return_session(session)
    
    def extract_field_value(self, soup: BeautifulSoup, field_names: List[str]) -> str:
        """Extract a field value by looking for labels"""
        for field_name in field_names:
            label_patterns = [
                soup.find('span', string=re.compile(field_name, re.IGNORECASE)),
                soup.find('td', string=re.compile(field_name, re.IGNORECASE)),
                soup.find('label', string=re.compile(field_name, re.IGNORECASE)),
            ]
            
            for label in label_patterns:
                if label:
                    next_elem = label.find_next_sibling()
                    if next_elem:
                        value = next_elem.get_text(strip=True)
                        if value and value != field_name:
                            return value
                    
                    parent = label.parent
                    if parent:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            value = next_elem.get_text(strip=True)
                            if value and value != field_name:
                                return value
        
        return ""
    
    def extract_course_description(self, soup: BeautifulSoup) -> str:
        """Extract course description"""
        try:
            desc_patterns = [
                ('div', {'class': re.compile('description', re.IGNORECASE)}),
                ('span', {'class': 'PSLONGEDITBOX'}),
                ('td', {'class': re.compile('description', re.IGNORECASE)}),
            ]
            
            for tag, attrs in desc_patterns:
                desc_elem = soup.find(tag, attrs)
                if desc_elem:
                    desc_text = desc_elem.get_text(strip=True)
                    if len(desc_text) > 50:
                        return desc_text
            
            desc_labels = soup.find_all(string=re.compile(r'Description', re.IGNORECASE))
            for label in desc_labels:
                if label.parent:
                    next_elem = label.parent.find_next()
                    if next_elem:
                        desc_text = next_elem.get_text(strip=True)
                        if len(desc_text) > 50:
                            return desc_text
            
            return ""
            
        except Exception as e:
            logger.debug(f"Error extracting course description: {e}")
            return ""
    
    def extract_class_attributes(self, soup: BeautifulSoup) -> List[str]:
        """Extract class attributes"""
        try:
            attributes = []
            attr_text = soup.get_text()
            
            attr_patterns = [
                r'General Education[:\s]*([^\n]+)',
                r'Attributes[:\s]*([^\n]+)',
                r'GenEd[:\s]*([^\n]+)',
            ]
            
            for pattern in attr_patterns:
                matches = re.findall(pattern, attr_text, re.IGNORECASE)
                for match in matches:
                    if match.strip():
                        attributes.append(match.strip())
            
            return list(set(attributes))
            
        except Exception as e:
            logger.debug(f"Error extracting class attributes: {e}")
            return []
    
    def log_final_stats(self):
        """Log comprehensive final statistics"""
        duration = self.stats['end_time'] - self.stats['start_time']
        
        logger.info("📊 OPTIMIZED SCRAPING STATISTICS")
        logger.info("=" * 60)
        logger.info(f"⏱️  Total time: {duration}")
        logger.info(f"📚 Subjects processed: {self.stats['processed_subjects']}/{self.stats['total_subjects']}")
        logger.info(f"🎓 Unique courses: {self.stats['unique_courses']}")
        logger.info(f"📖 Total sections: {self.stats['total_sections']}")
        logger.info(f"🔍 Detailed sections: {self.stats['detailed_sections']}")
        logger.info(f"❌ Failed subjects: {len(self.stats['failed_subjects'])}")
        logger.info(f"❌ Failed details: {self.stats['failed_details']}")
        
        if self.stats['total_sections'] > 0:
            sections_per_second = self.stats['total_sections'] / duration.total_seconds()
            logger.info(f"⚡ Rate: {sections_per_second:.2f} sections/second")
        
        # Data efficiency stats
        if self.stats['unique_courses'] > 0 and self.stats['total_sections'] > 0:
            avg_sections_per_course = self.stats['total_sections'] / self.stats['unique_courses']
            logger.info(f"📊 Avg sections per course: {avg_sections_per_course:.2f}")
            
            # Estimate data savings
            traditional_size = self.stats['total_sections']  # Each section would be a full record
            optimized_size = self.stats['unique_courses'] + self.stats['total_sections']  # Course records + section records
            savings_pct = ((traditional_size - optimized_size) / traditional_size) * 100 if traditional_size > 0 else 0
            logger.info(f"💾 Estimated data savings: {savings_pct:.1f}%")

def save_optimized_results(courses_data: Dict[str, OptimizedCourseData], output_file: str, format_type: str = 'jsonl'):
    """Save optimized results in various formats"""
    logger.info(f"💾 Saving {len(courses_data)} courses to {output_file}...")
    
    if format_type.lower() == 'jsonl':
        with open(output_file, 'w', encoding='utf-8') as f:
            for course_code, course_data in courses_data.items():
                # Create optimized record
                record = {
                    'course': asdict(course_data.course_info),
                    'sections': [asdict(section) for section in course_data.sections],
                    'stats': {
                        'total_capacity': course_data.get_total_capacity(),
                        'total_enrollment': course_data.get_total_enrollment(),
                        'available_seats': course_data.get_available_seats(),
                        'section_count': course_data.get_section_count(),
                        'campuses': list(course_data.get_campuses())
                    }
                }
                json.dump(record, f, ensure_ascii=False, separators=(',', ':'))
                f.write('\n')
    
    elif format_type.lower() == 'json':
        data = {}
        for course_code, course_data in courses_data.items():
            data[course_code] = {
                'course': asdict(course_data.course_info),
                'sections': [asdict(section) for section in course_data.sections],
                'stats': {
                    'total_capacity': course_data.get_total_capacity(),
                    'total_enrollment': course_data.get_total_enrollment(),
                    'available_seats': course_data.get_available_seats(),
                    'section_count': course_data.get_section_count(),
                    'campuses': list(course_data.get_campuses())
                }
            }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    elif format_type.lower() == 'csv':
        # Flatten to CSV format (one row per section, with course info repeated)
        import csv
        flattened_data = []
        
        for course_code, course_data in courses_data.items():
            course_dict = asdict(course_data.course_info)
            
            for section in course_data.sections:
                section_dict = asdict(section)
                # Combine course and section data
                combined = {**course_dict, **section_dict}
                # Clean up class_attributes for CSV
                combined['class_attributes'] = '; '.join(combined.get('class_attributes', []))
                flattened_data.append(combined)
        
        if flattened_data:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=flattened_data[0].keys())
                writer.writeheader()
                writer.writerows(flattened_data)

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Penn State LionPath Course Scraper - Optimized Data Structure')
    parser.add_argument('--output', '-o', default='psu_courses_optimized.jsonl', help='Output file')
    parser.add_argument('--format', choices=['jsonl', 'json', 'csv'], default='jsonl', help='Output format')
    parser.add_argument('--campus', '-c', default='UP', help='Campus filter (UP for University Park, ALL for all)')
    parser.add_argument('--delay', type=float, default=0.2, help='Delay between requests')
    parser.add_argument('--max-workers', type=int, default=16, help='Max concurrent workers for subjects')
    parser.add_argument('--max-detail-workers', type=int, default=50, help='Max concurrent workers for course details')
    parser.add_argument('--rate-limit', type=int, default=20, help='Requests per second limit')
    parser.add_argument('--max-subjects', type=int, help='Limit number of subjects (for testing)')
    parser.add_argument('--retry-attempts', type=int, default=2, help='Number of retry attempts')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("🎓 Penn State LionPath Course Scraper - Optimized Data Structure")
    logger.info(f"📁 Output file: {args.output}")
    logger.info(f"📊 Output format: {args.format}")
    logger.info(f"🏫 Campus filter: {args.campus}")
    
    scraper = OptimizedLionPathScraper(
        delay=args.delay,
        max_workers=args.max_workers,
        max_detail_workers=args.max_detail_workers,
        retry_attempts=args.retry_attempts,
        rate_limit_per_second=args.rate_limit
    )
    
    try:
        # Run the scraper
        courses_data = scraper.scrape_all_courses(
            campus_filter=args.campus,
            max_subjects=args.max_subjects
        )
        
        # Save results
        save_optimized_results(courses_data, args.output, args.format)
        
        logger.info(f"✅ Optimized scraping completed successfully!")
        logger.info(f"💾 Results saved to: {args.output}")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Scraping interrupted by user")
    except Exception as e:
        logger.error(f"💥 Error during scraping: {e}")
        import traceback
        logger.debug(traceback.format_exc())

if __name__ == "__main__":
    main()
