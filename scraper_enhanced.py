#!/usr/bin/env python3
"""
Enhanced Penn State LionPath Course Scraper
Captures ALL available information including descriptions, enrollment details, etc.
"""

import asyncio
import aiohttp
import requests
import logging
import json
import csv
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from bs4 import BeautifulSoup
import pandas as pd
from queue import Queue
import concurrent.futures
from threading import Lock
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('psu_scraper_enhanced.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('scraper_enhanced')

@dataclass
class CourseInfo:
    """Enhanced course-level information - same across all sections"""
    course_code: str = ""
    course_title: str = ""
    subject: str = ""
    catalog_number: str = ""
    units: str = ""
    
    # Academic details
    career: str = ""  # Undergraduate/Graduate
    grading: str = ""  # e.g., "Undergraduate Standard Grades"
    component: str = ""  # Lecture, Lab, etc.
    
    # Description and requirements
    course_description: str = ""
    enrollment_requirements: str = ""
    enforced_concurrent: str = ""
    class_attributes: List[str] = field(default_factory=list)
    
    # Additional info
    academic_organization: str = ""
    course_notes: str = ""
    textbook_info: str = ""
    
    # Metadata
    semester: str = "Fall 2025"
    last_updated: str = ""
    
    def __post_init__(self):
        if not self.last_updated:
            self.last_updated = datetime.now().isoformat()

@dataclass
class SectionInfo:
    """Enhanced section-specific information"""
    section: str = ""
    class_number: str = ""
    section_type: str = ""  # Lecture, Lab, Recitation, etc.
    status: str = ""  # Open, Closed, Waitlist
    
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
    instruction_mode: str = ""  # In Person, Web, Video - Receiving, etc.
    
    # Instructor
    instructor: str = ""
    instructor_email: str = ""
    
    # Enrollment
    class_capacity: int = 0
    enrollment_total: int = 0
    available_seats: int = 0
    waitlist_capacity: int = 0
    waitlist_total: int = 0
    
    # Reserve Capacity details
    reserve_capacity: List[Dict[str, Any]] = field(default_factory=list)
    
    # Section-specific details
    add_consent: str = ""
    drop_consent: str = ""
    class_notes: str = ""
    
    # Exam schedule
    exam_schedule: List[Dict[str, str]] = field(default_factory=list)
    
    # Metadata
    course_url: str = ""
    detail_url: str = ""
    scrape_timestamp: str = ""
    
    # Reference to course (will be added dynamically)
    course_code: str = ""
    
    def __post_init__(self):
        if not self.scrape_timestamp:
            self.scrape_timestamp = datetime.now().isoformat()

@dataclass
class EnhancedCourseData:
    """Enhanced course data structure with complete information"""
    course_info: CourseInfo
    sections: List[SectionInfo]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'course': asdict(self.course_info),
            'sections': [asdict(s) for s in self.sections],
            'stats': {
                'section_count': len(self.sections),
                'total_capacity': sum(s.class_capacity for s in self.sections),
                'total_enrollment': sum(s.enrollment_total for s in self.sections),
                'available_seats': sum(s.available_seats for s in self.sections),
                'campuses': list(set(s.campus for s in self.sections if s.campus))
            }
        }

class EnhancedLionPathScraper:
    """Enhanced scraper that captures ALL available course information"""
    
    def __init__(self, delay: float = 0.5, max_workers: int = 10, 
                 max_detail_workers: int = 20, retry_attempts: int = 3,
                 rate_limit_per_second: float = 15):
        self.base_url = "https://public.lionpath.psu.edu/psp/CSPRD/EMPLOYEE/HRMS/c/COMMUNITY_ACCESS.CLASS_SEARCH.GBL"
        self.delay = delay
        self.max_workers = max_workers
        self.max_detail_workers = max_detail_workers
        self.retry_attempts = retry_attempts
        self.rate_limit_per_second = rate_limit_per_second
        self.rate_limiter = threading.Semaphore(int(rate_limit_per_second))
        self.rate_limit_lock = Lock()
        self.last_request_time = 0
        
        # Session pool for connection reuse
        self.session_pool = Queue()
        for _ in range(max_workers + max_detail_workers):
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            self.session_pool.put(session)
        
        # Storage
        self.courses_data = {}
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_subjects': 0,
            'processed_subjects': 0,
            'failed_subjects': [],
            'unique_courses': 0,
            'total_sections': 0,
            'detailed_sections': 0
        }
    
    def get_session(self) -> requests.Session:
        """Get a session from the pool"""
        return self.session_pool.get()
    
    def return_session(self, session: requests.Session):
        """Return a session to the pool"""
        self.session_pool.put(session)
    
    def rate_limited_request(self, method, *args, **kwargs):
        """Make a rate-limited request"""
        with self.rate_limit_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            min_interval = 1.0 / self.rate_limit_per_second
            
            if time_since_last < min_interval:
                time.sleep(min_interval - time_since_last)
            
            self.last_request_time = time.time()
        
        return method(*args, **kwargs)
    
    def get_all_subjects(self) -> List[Dict]:
        """Get all available subject codes"""
        session = self.get_session()
        try:
            response = self.rate_limited_request(
                session.get, 
                self.base_url,
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get subjects: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            subjects = []
            
            # Find all subject checkboxes
            subject_inputs = soup.find_all('input', {'id': re.compile(r'PTS_SELECT\$\d+')})
            
            for input_elem in subject_inputs:
                # Get corresponding label
                label_id = input_elem.get('id', '').replace('PTS_SELECT', 'PTS_SELECT_LBL')
                label = soup.find('label', {'id': label_id})
                
                if label:
                    text = label.get_text(strip=True)
                    # Parse subject code and name
                    match = re.match(r'([A-Z]+-?[A-Z]*)\s*/\s*(.+)', text)
                    if match:
                        subjects.append({
                            'code': match.group(1),
                            'name': match.group(2),
                            'checkbox_id': input_elem.get('id')
                        })
            
            logger.info(f"Found {len(subjects)} subjects")
            return subjects
            
        except Exception as e:
            logger.error(f"Error getting subjects: {e}")
            return []
        finally:
            self.return_session(session)
    
    def scrape_subject(self, subject: Dict, campus_filter: str = "UP") -> List[SectionInfo]:
        """Scrape all sections for a subject"""
        session = self.get_session()
        try:
            sections = []
            
            # Make request to get subject courses
            form_data = self.build_subject_form_data(subject)
            
            response = self.rate_limited_request(
                session.post,
                self.base_url,
                data=form_data,
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get {subject['code']}: {response.status_code}")
                return sections
            
            # Parse sections from response
            sections = self.parse_subject_sections(response.text, subject['code'])
            
            # Filter for campus if needed
            if campus_filter == "UP":
                sections = [s for s in sections if self.is_university_park_section(s)]
            
            return sections
            
        except Exception as e:
            logger.error(f"Error scraping {subject['code']}: {e}")
            return []
        finally:
            self.return_session(session)
    
    def parse_subject_sections(self, html: str, subject_code: str) -> List[SectionInfo]:
        """Parse sections from subject page HTML"""
        sections = []
        
        # Find all class detail links
        pattern = r'javascript:showClassDetails\((\d+),(\d+)\)[^>]*>([^<]+)<'
        matches = re.findall(pattern, html)
        
        for strm, class_nbr, text in matches:
            section = self.parse_section_text(text, strm, class_nbr, subject_code)
            if section:
                sections.append(section)
        
        return sections
    
    def parse_section_text(self, text: str, strm: str, class_nbr: str, subject_code: str) -> Optional[SectionInfo]:
        """Parse basic section information from text"""
        try:
            text = text.strip()
            
            # Parse course code and section
            # Format: "A-I 285 - 555V - Hazleton Campus"
            parts = text.split(' - ')
            
            if len(parts) < 2:
                return None
            
            # Extract course code
            course_match = re.match(r'^([A-Z]+-?[A-Z]*)\s+(\d+[A-Z]*)', parts[0])
            if not course_match:
                return None
            
            course_code = parts[0].strip()
            section_num = parts[1].strip() if len(parts) > 1 else ""
            campus = parts[2].strip() if len(parts) > 2 else "UP"
            
            # Determine campus from text
            if "World Campus" in text:
                campus = "World Campus"
            elif any(c in text for c in ['Berks', 'Abington', 'Altoona', 'Hazleton', 'Erie']):
                for c in ['Berks', 'Abington', 'Altoona', 'Hazleton', 'Erie', 'Harrisburg']:
                    if c in text:
                        campus = c
                        break
            elif section_num and not any(c in text.lower() for c in ['world', 'campus']):
                # If no campus mentioned and not World Campus, assume UP
                campus = "UP" if not campus else campus
            
            section_info = SectionInfo(
                section=section_num,
                class_number=class_nbr,
                campus=campus,
                course_code=course_code,
                detail_url=f"showClassDetails({strm},{class_nbr})"
            )
            
            return section_info
            
        except Exception as e:
            logger.debug(f"Error parsing section text: {e}")
            return None
    
    def get_section_details(self, session: requests.Session, section: SectionInfo) -> SectionInfo:
        """Get detailed information for a section"""
        try:
            # Extract strm and class_nbr from detail_url
            match = re.search(r'showClassDetails\((\d+),(\d+)\)', section.detail_url)
            if not match:
                return section
            
            strm, class_nbr = match.groups()
            
            # Build request to get detailed information
            form_data = {
                'ICSID': self.get_icsid(session),
                'ICStateNum': '1',
                'ICAction': f'DERIVED_CLSRCH_SSR_CLASSNAME_LONG${class_nbr}',
                'DERIVED_SSTSNAV_SSTS_MAIN_GOTO$7$': '9999',
                'CLASS_SRCH_WRK2_STRM$273$': strm,
                'SSR_CLS_DTL_WRK_CLASS_NBR': class_nbr
            }
            
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
            logger.debug(f"Error getting section details for {section.class_number}: {e}")
            return section
    
    def parse_detailed_section_info(self, html: str, base_section: SectionInfo) -> SectionInfo:
        """Parse detailed section information from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text()
            
            # Create enhanced section with all existing info
            section = SectionInfo(**asdict(base_section))
            
            # Extract status (Open/Closed/Waitlist)
            status_match = re.search(r'Status[:\s]*([^\n]+)', text, re.IGNORECASE)
            if status_match:
                section.status = status_match.group(1).strip()
            
            # Extract class type (Lecture/Lab/etc)
            type_match = re.search(r'Class Type[:\s]*([^\n]+)', text, re.IGNORECASE)
            if type_match:
                section.section_type = type_match.group(1).strip()
            
            # Extract meeting information
            # Days and Times
            days_match = re.search(r'Days[:\s]*([^\n]+)', text, re.IGNORECASE)
            if days_match:
                section.days = days_match.group(1).strip()
            
            times_match = re.search(r'Times?[:\s]*(\d{1,2}:\d{2}[AP]M)\s*-\s*(\d{1,2}:\d{2}[AP]M)', text, re.IGNORECASE)
            if times_match:
                section.start_time = times_match.group(1)
                section.end_time = times_match.group(2)
                section.times = f"{section.start_time} to {section.end_time}"
            
            # Meeting dates
            dates_match = re.search(r'(?:Meeting Dates|Dates)[:\s]*(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})', text, re.IGNORECASE)
            if dates_match:
                section.start_date = dates_match.group(1)
                section.end_date = dates_match.group(2)
                section.meeting_dates = f"{section.start_date} - {section.end_date}"
            
            # Instruction mode
            mode_match = re.search(r'Instruction Mode[:\s]*([^\n]+)', text, re.IGNORECASE)
            if mode_match:
                section.instruction_mode = mode_match.group(1).strip()
            
            # Location details
            location_match = re.search(r'Location[:\s]*([^\n]+)', text, re.IGNORECASE)
            if location_match:
                section.location = location_match.group(1).strip()
            
            room_match = re.search(r'Room[:\s]*([^\n]+)', text, re.IGNORECASE)
            if room_match:
                section.room = room_match.group(1).strip()
            
            building_match = re.search(r'Building[:\s]*([^\n]+)', text, re.IGNORECASE)
            if building_match:
                section.building = building_match.group(1).strip()
            
            # Instructor
            instructor_match = re.search(r'Instructor[:\s]*([^\n]+)', text, re.IGNORECASE)
            if instructor_match:
                section.instructor = instructor_match.group(1).strip()
            
            # Enrollment information
            capacity_match = re.search(r'Class Capacity[:\s]*(\d+)', text, re.IGNORECASE)
            if capacity_match:
                section.class_capacity = int(capacity_match.group(1))
            
            enrolled_match = re.search(r'(?:Enrollment Total|Total Enrolled)[:\s]*(\d+)', text, re.IGNORECASE)
            if enrolled_match:
                section.enrollment_total = int(enrolled_match.group(1))
            
            available_match = re.search(r'Available Seats[:\s]*(\d+)', text, re.IGNORECASE)
            if available_match:
                section.available_seats = int(available_match.group(1))
            
            waitlist_cap_match = re.search(r'Wait List Capacity[:\s]*(\d+)', text, re.IGNORECASE)
            if waitlist_cap_match:
                section.waitlist_capacity = int(waitlist_cap_match.group(1))
            
            waitlist_total_match = re.search(r'Wait List Total[:\s]*(\d+)', text, re.IGNORECASE)
            if waitlist_total_match:
                section.waitlist_total = int(waitlist_total_match.group(1))
            
            # Class notes
            notes_match = re.search(r'Class Notes[:\s]*([^\n]+)', text, re.IGNORECASE)
            if notes_match and notes_match.group(1).strip() != "No Class Notes":
                section.class_notes = notes_match.group(1).strip()
            
            # Consent requirements
            add_consent_match = re.search(r'Add Consent[:\s]*([^\n]+)', text, re.IGNORECASE)
            if add_consent_match:
                section.add_consent = add_consent_match.group(1).strip()
            
            drop_consent_match = re.search(r'Drop Consent[:\s]*([^\n]+)', text, re.IGNORECASE)
            if drop_consent_match:
                section.drop_consent = drop_consent_match.group(1).strip()
            
            return section
            
        except Exception as e:
            logger.debug(f"Error parsing detailed section info: {e}")
            return base_section
    
    def get_course_details(self, session: requests.Session, course_code: str, 
                          sample_section: SectionInfo) -> CourseInfo:
        """Get detailed course-level information"""
        try:
            # Use the first section to get course details
            match = re.search(r'showClassDetails\((\d+),(\d+)\)', sample_section.detail_url)
            if not match:
                return CourseInfo(course_code=course_code)
            
            strm, class_nbr = match.groups()
            
            # Make request for course details
            form_data = {
                'ICSID': self.get_icsid(session),
                'ICStateNum': '1',
                'ICAction': f'DERIVED_CLSRCH_SSR_CLASSNAME_LONG${class_nbr}',
                'CLASS_SRCH_WRK2_STRM$273$': strm,
                'SSR_CLS_DTL_WRK_CLASS_NBR': class_nbr
            }
            
            response = self.rate_limited_request(
                session.post,
                self.base_url,
                data=form_data,
                timeout=30
            )
            
            if response.status_code == 200:
                return self.parse_course_details(response.text, course_code)
            
            return CourseInfo(course_code=course_code)
            
        except Exception as e:
            logger.debug(f"Error getting course details for {course_code}: {e}")
            return CourseInfo(course_code=course_code)
    
    def parse_course_details(self, html: str, course_code: str) -> CourseInfo:
        """Parse course-level details from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text()
            
            # Parse course code components
            match = re.match(r'^([A-Z]+-?[A-Z]*)\s+(\d+[A-Z]*)', course_code)
            subject = match.group(1) if match else ""
            catalog_number = match.group(2) if match else ""
            
            course_info = CourseInfo(
                course_code=course_code,
                subject=subject,
                catalog_number=catalog_number
            )
            
            # Extract course title
            title_match = re.search(r'(?:' + re.escape(course_code) + r')\s*-?\s*([^\n]+?)(?:\n|Status|$)', text)
            if title_match:
                course_info.course_title = title_match.group(1).strip()
            
            # Extract units
            units_match = re.search(r'Units[:\s]*(\d+\.?\d*)', text, re.IGNORECASE)
            if units_match:
                course_info.units = units_match.group(1)
            
            # Extract career (Undergraduate/Graduate)
            career_match = re.search(r'Career[:\s]*([^\n]+)', text, re.IGNORECASE)
            if career_match:
                course_info.career = career_match.group(1).strip()
            
            # Extract grading
            grading_match = re.search(r'Grading[:\s]*([^\n]+)', text, re.IGNORECASE)
            if grading_match:
                course_info.grading = grading_match.group(1).strip()
            
            # Extract component (Lecture/Lab/etc)
            component_match = re.search(r'Component[:\s]*([^\n]+)', text, re.IGNORECASE)
            if component_match:
                course_info.component = component_match.group(1).strip()
            
            # Extract course description (long text)
            desc_patterns = [
                r'Course Description[:\s]*\n([^]+?)(?:\n\n|Enrollment|Class Notes|$)',
                r'Description[:\s]*\n([^]+?)(?:\n\n|Enrollment|Class Notes|$)',
                r'(?:Course Description|Description)[:\s]*([^\n]+(?:\n[^\n]+)*?)(?:\n\n|Enrollment|$)'
            ]
            
            for pattern in desc_patterns:
                desc_match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if desc_match:
                    desc_text = desc_match.group(1).strip()
                    # Clean up the description
                    desc_text = re.sub(r'\s+', ' ', desc_text)
                    if len(desc_text) > 50:  # Only use if it's a substantial description
                        course_info.course_description = desc_text
                        break
            
            # Extract enrollment requirements
            req_match = re.search(r'Enrollment Requirements[:\s]*([^\n]+(?:\n[^\n]+)*?)(?:\n\n|Class|$)', text, re.IGNORECASE)
            if req_match:
                course_info.enrollment_requirements = req_match.group(1).strip()
            
            # Extract enforced concurrent
            concurrent_match = re.search(r'Enforced Concurrent[:\s]*([^\n]+)', text, re.IGNORECASE)
            if concurrent_match:
                course_info.enforced_concurrent = concurrent_match.group(1).strip()
            
            # Extract class attributes
            attr_match = re.search(r'Class Attributes[:\s]*([^\n]+)', text, re.IGNORECASE)
            if attr_match and "No Class Attributes" not in attr_match.group(1):
                # Split multiple attributes if comma-separated
                attrs = attr_match.group(1).strip()
                course_info.class_attributes = [a.strip() for a in attrs.split(',')]
            
            # Extract academic organization
            org_match = re.search(r'Academic Organization[:\s]*([^\n]+)', text, re.IGNORECASE)
            if org_match:
                course_info.academic_organization = org_match.group(1).strip()
            
            # Extract course notes
            notes_match = re.search(r'Course Notes[:\s]*([^\n]+)', text, re.IGNORECASE)
            if notes_match and "No Course Notes" not in notes_match.group(1):
                course_info.course_notes = notes_match.group(1).strip()
            
            return course_info
            
        except Exception as e:
            logger.debug(f"Error parsing course details: {e}")
            return CourseInfo(course_code=course_code)
    
    def get_icsid(self, session: requests.Session) -> str:
        """Get ICSID from current session"""
        # This would need to be extracted from the session's last response
        # For now, return a placeholder
        return "xyz123"
    
    def build_subject_form_data(self, subject: Dict) -> Dict:
        """Build form data for subject request"""
        return {
            'ICAction': 'CLASS_SRCH_WRK2_SSR_PB_CLASS_SRCH',
            'CLASS_SRCH_WRK2_STRM$273$': '2025',  # Fall 2025
            'SSR_CLSRCH_WRK_SUBJECT$0': subject['code'],
            'SSR_CLSRCH_WRK_SSR_OPEN_ONLY$chk': 'N',
            'SSR_CLSRCH_WRK_CAMPUS$0': 'UP',
            subject['checkbox_id']: 'Y'
        }
    
    def is_university_park_section(self, section: SectionInfo) -> bool:
        """Check if section is at University Park"""
        campus = section.campus.upper()
        section_num = section.section.upper()
        
        # Non-UP indicators
        if "WORLD CAMPUS" in campus:
            return False
        if any(c in campus for c in ['BERKS', 'ABINGTON', 'ALTOONA', 'HAZLETON']):
            return False
        
        # Section number indicators
        if section_num.endswith('W'):  # World Campus
            return False
        if section_num.endswith('Y'):  # York
            return False
        
        # If campus is explicitly UP or empty (default), it's UP
        return campus in ['UP', 'UNIVERSITY PARK', '']
    
    def scrape_all_courses(self, campus_filter: str = "UP", max_subjects: int = None) -> Dict[str, EnhancedCourseData]:
        """Main method to scrape all courses with complete information"""
        self.stats['start_time'] = datetime.now()
        logger.info("🚀 Starting Enhanced LionPath scraping...")
        
        try:
            # Get all subjects
            logger.info("📚 Getting all subject codes...")
            subjects = self.get_all_subjects()
            self.stats['total_subjects'] = len(subjects)
            
            if max_subjects:
                subjects = subjects[:max_subjects]
                logger.info(f"Limited to {max_subjects} subjects for testing")
            
            # Scrape subjects in parallel
            logger.info("🏃 Starting parallel subject scraping...")
            all_sections = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_subject = {
                    executor.submit(self.scrape_subject, subject, campus_filter): subject 
                    for subject in subjects
                }
                
                for future in concurrent.futures.as_completed(future_to_subject):
                    subject = future_to_subject[future]
                    try:
                        sections = future.result()
                        if sections:
                            all_sections.extend(sections)
                            logger.info(f"✅ {subject['code']}: {len(sections)} sections")
                        self.stats['processed_subjects'] += 1
                    except Exception as e:
                        self.stats['failed_subjects'].append(subject['code'])
                        logger.error(f"❌ {subject['code']} failed: {e}")
            
            # Organize by course
            logger.info("📊 Organizing sections by course...")
            courses_map = {}
            for section in all_sections:
                course_code = section.course_code
                if course_code not in courses_map:
                    courses_map[course_code] = []
                courses_map[course_code].append(section)
            
            logger.info(f"Found {len(courses_map)} unique courses")
            
            # Get detailed information for each course and section
            logger.info("🔍 Extracting detailed information...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_detail_workers) as executor:
                # Submit course detail jobs
                course_futures = {}
                for course_code, sections in courses_map.items():
                    if sections:
                        session = self.get_session()
                        future = executor.submit(
                            self.get_course_details, 
                            session, 
                            course_code, 
                            sections[0]
                        )
                        course_futures[future] = (course_code, sections, session)
                
                # Process results
                for future in concurrent.futures.as_completed(course_futures):
                    course_code, sections, session = course_futures[future]
                    try:
                        course_info = future.result()
                        
                        # Get detailed section information
                        detailed_sections = []
                        for section in sections:
                            detailed_section = self.get_section_details(session, section)
                            detailed_sections.append(detailed_section)
                        
                        # Create enhanced course data
                        self.courses_data[course_code] = EnhancedCourseData(
                            course_info=course_info,
                            sections=detailed_sections
                        )
                        
                        logger.info(f"✅ Detailed info for {course_code}: {len(detailed_sections)} sections")
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to get details for {course_code}: {e}")
                        # Still save basic info
                        self.courses_data[course_code] = EnhancedCourseData(
                            course_info=CourseInfo(course_code=course_code),
                            sections=sections
                        )
                    finally:
                        self.return_session(session)
            
            # Update statistics
            self.stats['unique_courses'] = len(self.courses_data)
            self.stats['total_sections'] = sum(
                len(course.sections) for course in self.courses_data.values()
            )
            self.stats['end_time'] = datetime.now()
            
            # Log summary
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            logger.info(f"""
            ============================================
            📊 SCRAPING COMPLETE
            ============================================
            ⏱️  Duration: {duration:.2f} seconds
            📚 Subjects: {self.stats['processed_subjects']}/{self.stats['total_subjects']}
            🎓 Unique courses: {self.stats['unique_courses']}
            📝 Total sections: {self.stats['total_sections']}
            ❌ Failed subjects: {len(self.stats['failed_subjects'])}
            ============================================
            """)
            
            return self.courses_data
            
        except Exception as e:
            logger.error(f"💥 Scraping failed: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return self.courses_data

def save_enhanced_results(courses_data: Dict[str, EnhancedCourseData], 
                         output_file: str, format: str = "jsonl"):
    """Save enhanced results to file"""
    logger.info(f"💾 Saving {len(courses_data)} courses to {output_file}...")
    
    if format == "jsonl":
        with open(output_file, 'w') as f:
            for course_code, course_data in courses_data.items():
                json_line = json.dumps(course_data.to_dict(), default=str)
                f.write(json_line + '\n')
    
    elif format == "json":
        output = {}
        for course_code, course_data in courses_data.items():
            output[course_code] = course_data.to_dict()
        
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2, default=str)
    
    elif format == "csv":
        # Flatten for CSV
        rows = []
        for course_code, course_data in courses_data.items():
            course_dict = asdict(course_data.course_info)
            for section in course_data.sections:
                row = {**course_dict, **asdict(section)}
                rows.append(row)
        
        if rows:
            df = pd.DataFrame(rows)
            df.to_csv(output_file, index=False)
    
    logger.info(f"✅ Saved {len(courses_data)} courses")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Penn State LionPath Course Scraper')
    parser.add_argument('--output', default='psu_courses_enhanced.jsonl', 
                       help='Output file path')
    parser.add_argument('--format', choices=['jsonl', 'json', 'csv'], 
                       default='jsonl', help='Output format')
    parser.add_argument('--campus', default='UP', 
                       help='Campus filter (UP for University Park, ALL for all campuses)')
    parser.add_argument('--max-subjects', type=int, 
                       help='Maximum number of subjects to scrape (for testing)')
    parser.add_argument('--max-workers', type=int, default=10,
                       help='Maximum parallel workers for subjects')
    parser.add_argument('--max-detail-workers', type=int, default=20,
                       help='Maximum parallel workers for details')
    parser.add_argument('--rate-limit', type=float, default=15,
                       help='Requests per second rate limit')
    parser.add_argument('--delay', type=float, default=0.5,
                       help='Delay between requests in seconds')
    parser.add_argument('--retry-attempts', type=int, default=3,
                       help='Number of retry attempts for failed requests')
    
    args = parser.parse_args()
    
    # Create scraper
    scraper = EnhancedLionPathScraper(
        delay=args.delay,
        max_workers=args.max_workers,
        max_detail_workers=args.max_detail_workers,
        retry_attempts=args.retry_attempts,
        rate_limit_per_second=args.rate_limit
    )
    
    # Scrape courses
    campus_filter = None if args.campus == 'ALL' else args.campus
    courses_data = scraper.scrape_all_courses(
        campus_filter=campus_filter,
        max_subjects=args.max_subjects
    )
    
    # Save results
    if courses_data:
        output_file = args.output
        if not output_file.endswith(f'.{args.format}'):
            output_file = f"{output_file.rsplit('.', 1)[0]}.{args.format}"
        
        save_enhanced_results(courses_data, output_file, args.format)
        logger.info(f"✅ Results saved to {output_file}")
    else:
        logger.warning("⚠️ No data to save")
    
    return 0

if __name__ == "__main__":
    exit(main())