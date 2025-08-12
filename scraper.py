#!/usr/bin/env python3
"""
Penn State LionPath Course Scraper - Enhanced with Full Course Details
Optimized for speed with aggressive parallelization and detailed data extraction
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
import asyncio
import aiohttp
from functools import partial
import multiprocessing
from queue import Queue, Empty
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('psu_scraper_enhanced.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class CourseDetails:
    """Comprehensive course data structure with all available details"""
    # Basic course info
    course_code: str = ""
    course_title: str = ""
    subject: str = ""
    catalog_number: str = ""
    section: str = ""
    
    # Class details
    class_number: str = ""
    career: str = ""
    units: str = ""
    grading: str = ""
    instruction_mode: str = ""
    component: str = ""
    
    # Scheduling
    semester: str = ""
    session: str = ""
    meeting_dates: str = ""
    days: str = ""
    times: str = ""
    start_time: str = ""
    end_time: str = ""
    start_date: str = ""
    end_date: str = ""
    
    # Location
    campus: str = ""
    location: str = ""
    building: str = ""
    room: str = ""
    
    # Instructor info
    instructor: str = ""
    instructor_email: str = ""
    
    # Enrollment data (from detailed view)
    class_capacity: int = 0
    enrollment_total: int = 0
    available_seats: int = 0
    waitlist_capacity: int = 0
    waitlist_total: int = 0
    
    # Status and requirements
    status: str = ""
    enrollment_requirements: str = ""
    add_consent: str = ""
    drop_consent: str = ""
    
    # Course descriptions and details
    course_description: str = ""
    class_notes: str = ""
    class_attributes: List[str] = None
    textbook_info: str = ""
    
    # Additional detailed fields
    academic_organization: str = ""
    course_components: str = ""
    enrollment_info: str = ""
    
    # Metadata
    course_url: str = ""
    detail_url: str = ""
    last_updated: str = ""
    scrape_timestamp: str = ""
    
    def __post_init__(self):
        if self.class_attributes is None:
            self.class_attributes = []
        if not self.scrape_timestamp:
            self.scrape_timestamp = datetime.now().isoformat()

class HighPerformanceLionPathScraper:
    """Ultra-fast scraper with full course details extraction"""
    
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
        
        # Data storage
        self.courses_data = []
        self.processed_courses = set()
        self.data_lock = Lock()
        
        # Session pool for better performance
        self.session_pool = Queue()
        self.init_session_pool(max_workers + max_detail_workers)
        
        # URLs and patterns
        self.base_url = "https://public.lionpath.psu.edu"
        self.search_url = "https://public.lionpath.psu.edu/psc/CSPRD/EMPLOYEE/SA/c/PE_SR175_PUBLIC.PE_SR175_CLS_SRCH.GBL"
        self.detail_url_template = "https://public.lionpath.psu.edu/psc/CSPRD/EMPLOYEE/SA/c/SA_LEARNER_SERVICES.SSR_SSENRL_DETAIL.GBL"
        
        # Statistics
        self.stats = {
            'total_subjects': 0,
            'processed_subjects': 0,
            'total_courses': 0,
            'detailed_courses': 0,
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
            # Create new session if pool is empty
            session = requests.Session()
            session.headers.update({
                'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
            })
            return session
    
    def return_session(self, session: requests.Session):
        """Return a session to the pool"""
        try:
            self.session_pool.put_nowait(session)
        except:
            pass  # If queue is full, just discard
    
    def rate_limited_request(self, method, *args, **kwargs):
        """Make a rate-limited request"""
        with self.request_lock:
            now = time.time()
            # Remove old timestamps
            self.last_request_times = [t for t in self.last_request_times if now - t < 1.0]
            
            # Wait if we're at the rate limit
            if len(self.last_request_times) >= self.rate_limit_per_second:
                sleep_time = 1.0 - (now - self.last_request_times[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            self.last_request_times.append(now)
        
        return method(*args, **kwargs)
    
    def scrape_all_courses(self, campus_filter: str = "UP", max_subjects: int = None) -> List[CourseDetails]:
        """Main scraping method with aggressive optimization"""
        self.stats['start_time'] = datetime.now()
        logger.info("🚀 Starting High-Performance LionPath scraping with full course details...")
        
        try:
            # Step 1: Get all subjects rapidly
            logger.info("📚 Getting all subject codes...")
            subjects = self.get_all_subjects()
            self.stats['total_subjects'] = len(subjects)
            logger.info(f"Found {len(subjects)} subjects")
            
            if max_subjects:
                subjects = subjects[:max_subjects]
                logger.info(f"Limited to first {max_subjects} subjects for testing")
            
            # Step 2: Rapid parallel subject scraping
            logger.info("🏃‍♂️ Starting rapid subject scraping...")
            all_courses = self.scrape_subjects_parallel(subjects, campus_filter)
            
            # Step 3: Massive parallel detail extraction
            logger.info(f"🔍 Starting detailed course info extraction for {len(all_courses)} courses...")
            detailed_courses = self.extract_course_details_parallel(all_courses)
            
            # Update statistics
            self.stats['total_courses'] = len(detailed_courses)
            self.stats['detailed_courses'] = len([c for c in detailed_courses if c.course_description])
            self.stats['end_time'] = datetime.now()
            
            self.log_final_stats()
            return detailed_courses
            
        except Exception as e:
            logger.error(f"💥 Scraping failed: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []
    
    def scrape_subjects_parallel(self, subjects: List[Dict], campus_filter: str) -> List[CourseDetails]:
        """Scrape all subjects in parallel with batching"""
        all_courses = []
        
        # Process in optimized batches
        batch_size = min(self.max_workers, 20)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all subject scraping tasks
            future_to_subject = {
                executor.submit(self.scrape_subject_optimized, subject): subject 
                for subject in subjects
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_subject):
                subject = future_to_subject[future]
                try:
                    courses = future.result()
                    if courses:
                        # Filter for campus if requested
                        if campus_filter.upper() == "UP":
                            up_courses = [c for c in courses if self.is_university_park(c)]
                            if up_courses:
                                all_courses.extend(up_courses)
                                logger.info(f"✅ {subject.get('code', 'unknown')}: {len(up_courses)} UP courses")
                        else:
                            all_courses.extend(courses)
                            logger.info(f"✅ {subject.get('code', 'unknown')}: {len(courses)} courses")
                    
                    self.stats['processed_subjects'] += 1
                    
                except Exception as e:
                    self.stats['failed_subjects'].append(subject.get('code', 'unknown'))
                    logger.error(f"❌ {subject.get('code', 'unknown')} failed: {e}")
        
        logger.info(f"📊 Subject scraping complete: {len(all_courses)} total courses found")
        return all_courses
    
    def is_university_park(self, course: CourseDetails) -> bool:
        """Determine if a course is at University Park"""
        campus = course.campus.upper()
        section = course.section.upper()
        
        # University Park indicators
        up_indicators = ['UP', 'UNIVERSITY PARK', '']
        
        # Non-UP indicators
        non_up_indicators = [
            'WORLD CAMPUS', 'BERKS', 'ABINGTON', 'ALTOONA', 'BRANDYWINE',
            'DUBOIS', 'ERIE', 'FAYETTE', 'GREATER ALLEGHENY', 'HARRISBURG',
            'HAZLETON', 'LEHIGH VALLEY', 'MONT ALTO', 'NEW KENSINGTON',
            'SCHUYLKILL', 'SHENANGO', 'WILKES-BARRE', 'YORK'
        ]
        
        # Check campus field
        if any(indicator in campus for indicator in non_up_indicators):
            return False
        
        if any(indicator in campus for indicator in up_indicators):
            return True
        
        # Check section suffixes (Y usually means online/World Campus)
        if section.endswith('Y') or section.endswith('W'):
            return False
        
        # Default to University Park if unclear
        return True
    
    def extract_course_details_parallel(self, courses: List[CourseDetails]) -> List[CourseDetails]:
        """Extract detailed course information in massive parallel fashion"""
        
        # Use a very high number of workers for detail extraction since it's mostly I/O
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_detail_workers) as executor:
            # Submit all detail extraction tasks
            future_to_course = {
                executor.submit(self.get_course_details, course): course 
                for course in courses
            }
            
            detailed_courses = []
            completed = 0
            
            # Process results with progress logging
            for future in concurrent.futures.as_completed(future_to_course):
                course = future_to_course[future]
                try:
                    detailed_course = future.result()
                    detailed_courses.append(detailed_course)
                    completed += 1
                    
                    if completed % 100 == 0:
                        logger.info(f"🔍 Detailed extraction progress: {completed}/{len(courses)}")
                        
                except Exception as e:
                    self.stats['failed_details'] += 1
                    logger.debug(f"Failed to get details for {course.course_code}: {e}")
                    # Keep the original course even if detail extraction fails
                    detailed_courses.append(course)
        
        logger.info(f"✅ Detail extraction complete: {len(detailed_courses)} courses processed")
        return detailed_courses
    
    def scrape_subject_optimized(self, subject: Dict) -> List[CourseDetails]:
        """Optimized subject scraping with minimal overhead"""
        session = self.get_session()
        try:
            return self._scrape_subject_internal(session, subject)
        finally:
            self.return_session(session)
    
    def _scrape_subject_internal(self, session: requests.Session, subject: Dict) -> List[CourseDetails]:
        """Internal subject scraping method"""
        subject_code = subject.get('code', 'unknown')
        
        try:
            # Load search page quickly
            response = self.rate_limited_request(
                session.get, 
                self.search_url, 
                params={'Page': 'PE_SR175_CLS_SRCH', 'Action': 'U'},
                timeout=10
            )
            response.raise_for_status()
            
            # Quick form data extraction
            form_data = self.extract_form_data_fast(response.text)
            
            # Select subject and submit
            checkbox_id = subject.get('checkbox_id', '')
            if checkbox_id:
                form_data[checkbox_id] = 'Y'
                form_data['ICAction'] = checkbox_id
                
                # Submit form
                response = self.rate_limited_request(
                    session.post, 
                    self.search_url, 
                    data=form_data,
                    timeout=10
                )
                response.raise_for_status()
                
                # Fast course parsing
                courses = self.parse_courses_optimized(response.text, subject_code)
                return courses
            
            return []
            
        except Exception as e:
            logger.debug(f"Error scraping subject {subject_code}: {e}")
            raise
    
    def extract_form_data_fast(self, html: str) -> Dict[str, str]:
        """Fast form data extraction using regex"""
        form_data = {}
        
        # Use regex for speed instead of BeautifulSoup
        hidden_pattern = r'<input[^>]*type=["\']hidden["\'][^>]*name=["\']([^"\']+)["\'][^>]*value=["\']([^"\']*)["\'][^>]*>'
        matches = re.findall(hidden_pattern, html, re.IGNORECASE)
        
        for name, value in matches:
            form_data[name] = value
        
        return form_data
    
    def parse_courses_optimized(self, html: str, subject_code: str) -> List[CourseDetails]:
        """Optimized course parsing with minimal overhead"""
        courses = []
        
        # Fast regex-based parsing for showClassDetails links
        pattern = r'javascript:showClassDetails\((\d+),(\d+)\)[^>]*>([^<]+)<'
        matches = re.findall(pattern, html)
        
        for strm, class_nbr, text in matches:
            try:
                course = self.parse_course_text_fast(text, strm, class_nbr, subject_code)
                if course:
                    courses.append(course)
            except Exception as e:
                logger.debug(f"Error parsing course text '{text}': {e}")
        
        # Also check enrollment links as backup
        enroll_pattern = r'SSR_CRSE_INFO_FL[^"]*CLASS_NBR=(\d+)[^"]*[^>]*aria-label="Enroll ([^"]+)"'
        enroll_matches = re.findall(enroll_pattern, html)
        
        for class_nbr, aria_text in enroll_matches:
            try:
                # Check if we already have this course
                if not any(c.class_number == class_nbr for c in courses):
                    course = self.parse_course_text_fast(aria_text, "2258", class_nbr, subject_code)
                    if course:
                        courses.append(course)
            except Exception as e:
                logger.debug(f"Error parsing enrollment text '{aria_text}': {e}")
        
        return courses
    
    def parse_course_text_fast(self, text: str, strm: str, class_nbr: str, subject_code: str) -> Optional[CourseDetails]:
        """Fast course text parsing"""
        try:
            # Clean text
            text = text.strip()
            
            # Parse course code
            course_patterns = [
                r'^([A-Z]+-?[A-Z]+)\s+(\d+[A-Z]*)',
                r'^([A-Z]{2,})\s+(\d+[A-Z]*)',
            ]
            
            subject = ""
            catalog_number = ""
            
            for pattern in course_patterns:
                match = re.match(pattern, text)
                if match:
                    subject = match.group(1)
                    catalog_number = match.group(2)
                    break
            
            if not subject or not catalog_number:
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
                    
                    # Default logic for UP
                    if not campus and not any(c in section_campus.lower() for c in ['world', 'berks', 'y']):
                        campus = 'UP'
            
            course = CourseDetails(
                course_code=f"{subject} {catalog_number}",
                subject=subject,
                catalog_number=catalog_number,
                section=section,
                campus=campus,
                class_number=class_nbr,
                semester="Fall 2025",
                course_url=f"showClassDetails({strm},{class_nbr})"
            )
            
            return course
            
        except Exception as e:
            logger.debug(f"Error in fast parsing: {e}")
            return None
    
    def get_course_details(self, course: CourseDetails) -> CourseDetails:
        """Get detailed course information by making additional requests"""
        session = self.get_session()
        try:
            return self._get_course_details_internal(session, course)
        finally:
            self.return_session(session)
    
    def _get_course_details_internal(self, session: requests.Session, course: CourseDetails) -> CourseDetails:
        """Internal method to get detailed course information"""
        try:
            # Extract parameters from course URL
            if "showClassDetails" in course.course_url:
                match = re.search(r'showClassDetails\((\d+),(\d+)\)', course.course_url)
                if not match:
                    return course
                
                strm, class_nbr = match.groups()
                
                # Try to get detailed course information
                detail_url = f"{self.base_url}/psc/CSPRD/EMPLOYEE/SA/c/SA_LEARNER_SERVICES.SSR_SSENRL_DETAIL.GBL"
                
                params = {
                    'Page': 'SSR_SSENRL_DETAIL',
                    'Action': 'A',
                    'STRM': strm,
                    'CLASS_NBR': class_nbr,
                    'ACAD_CAREER': 'UGRD',  # Undergraduate, could also be GRAD
                }
                
                # Make request for detailed information
                response = self.rate_limited_request(
                    session.get,
                    detail_url,
                    params=params,
                    timeout=8
                )
                
                if response.status_code == 200:
                    detailed_course = self.parse_detailed_course_info(response.text, course)
                    return detailed_course
                
            return course
            
        except Exception as e:
            logger.debug(f"Failed to get details for {course.course_code}: {e}")
            return course
    
    def parse_detailed_course_info(self, html: str, base_course: CourseDetails) -> CourseDetails:
        """Parse detailed course information from the course detail page"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Create a copy of the base course
            detailed_course = CourseDetails(**asdict(base_course))
            
            # Extract course title
            title_patterns = [
                ('span', {'class': 'PALEVEL0SECONDARY'}),
                ('span', {'class': 'PSHYPERLINKDISABLED'}),
                ('h1', {}),
                ('h2', {})
            ]
            
            for tag, attrs in title_patterns:
                title_elem = soup.find(tag, attrs)
                if title_elem and title_elem.get_text(strip=True):
                    title_text = title_elem.get_text(strip=True)
                    # Extract just the title part (after course code)
                    if ' - ' in title_text:
                        detailed_course.course_title = title_text.split(' - ', 1)[1]
                    break
            
            # Extract units/credits
            units_text = self.extract_field_value(soup, ['Units', 'Credits', 'Credit Hours'])
            if units_text:
                units_match = re.search(r'(\d+\.?\d*)', units_text)
                if units_match:
                    detailed_course.units = units_match.group(1)
            
            # Extract grading basis
            detailed_course.grading = self.extract_field_value(soup, ['Grading Basis', 'Grading'])
            
            # Extract instruction mode
            detailed_course.instruction_mode = self.extract_field_value(soup, ['Instruction Mode', 'Instructional Method'])
            
            # Extract component (LEC, LAB, etc.)
            component = self.extract_field_value(soup, ['Component', 'Class Component'])
            if component:
                detailed_course.component = component
            
            # Extract detailed scheduling information
            self.extract_schedule_details(soup, detailed_course)
            
            # Extract enrollment information
            self.extract_enrollment_details(soup, detailed_course)
            
            # Extract course description
            desc = self.extract_course_description(soup)
            if desc:
                detailed_course.course_description = desc
            
            # Extract class notes
            notes = self.extract_field_value(soup, ['Class Notes', 'Notes', 'Additional Information'])
            if notes:
                detailed_course.class_notes = notes
            
            # Extract enrollment requirements
            req = self.extract_field_value(soup, ['Enrollment Requirements', 'Prerequisites', 'Requirements'])
            if req:
                detailed_course.enrollment_requirements = req
            
            # Extract consent requirements
            detailed_course.add_consent = self.extract_field_value(soup, ['Add Consent'])
            detailed_course.drop_consent = self.extract_field_value(soup, ['Drop Consent'])
            
            # Extract status
            status = self.extract_field_value(soup, ['Status', 'Class Status'])
            if status:
                detailed_course.status = status
            
            # Extract additional attributes
            attributes = self.extract_class_attributes(soup)
            if attributes:
                detailed_course.class_attributes = attributes
            
            detailed_course.detail_url = soup.url if hasattr(soup, 'url') else ""
            detailed_course.last_updated = datetime.now().isoformat()
            
            return detailed_course
            
        except Exception as e:
            logger.debug(f"Error parsing detailed course info: {e}")
            return base_course
    
    def extract_field_value(self, soup: BeautifulSoup, field_names: List[str]) -> str:
        """Extract a field value by looking for labels"""
        for field_name in field_names:
            # Look for labels containing the field name
            label_patterns = [
                soup.find('span', string=re.compile(field_name, re.IGNORECASE)),
                soup.find('td', string=re.compile(field_name, re.IGNORECASE)),
                soup.find('label', string=re.compile(field_name, re.IGNORECASE)),
            ]
            
            for label in label_patterns:
                if label:
                    # Find the next element that might contain the value
                    next_elem = label.find_next_sibling()
                    if next_elem:
                        value = next_elem.get_text(strip=True)
                        if value and value != field_name:
                            return value
                    
                    # Try parent's next sibling
                    parent = label.parent
                    if parent:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            value = next_elem.get_text(strip=True)
                            if value and value != field_name:
                                return value
        
        return ""
    
    def extract_schedule_details(self, soup: BeautifulSoup, course: CourseDetails):
        """Extract detailed schedule information"""
        try:
            # Look for time patterns
            time_pattern = r'(\d{1,2}:\d{2}[AP]M)\s*-\s*(\d{1,2}:\d{2}[AP]M)'
            time_text = soup.get_text()
            time_match = re.search(time_pattern, time_text)
            
            if time_match:
                course.start_time = time_match.group(1)
                course.end_time = time_match.group(2)
                course.times = f"{course.start_time} - {course.end_time}"
            
            # Look for days
            days_pattern = r'\b(Mo|Tu|We|Th|Fr|Sa|Su)+\b'
            days_match = re.search(days_pattern, time_text)
            if days_match:
                course.days = days_match.group(0)
            
            # Look for date ranges
            date_pattern = r'(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})'
            date_match = re.search(date_pattern, time_text)
            if date_match:
                course.start_date = date_match.group(1)
                course.end_date = date_match.group(2)
                course.meeting_dates = f"{course.start_date} - {course.end_date}"
            
            # Extract room/location
            room_patterns = [
                r'Room:\s*([A-Z0-9\-\s]+)',
                r'Location:\s*([A-Z0-9\-\s]+)',
                r'\b([A-Z]{2,}\s+\d+[A-Z]?)\b'  # Building code + room number
            ]
            
            for pattern in room_patterns:
                room_match = re.search(pattern, time_text)
                if room_match:
                    room_info = room_match.group(1).strip()
                    if len(room_info) > 2:  # Avoid false positives
                        course.room = room_info
                        break
            
        except Exception as e:
            logger.debug(f"Error extracting schedule details: {e}")
    
    def extract_enrollment_details(self, soup: BeautifulSoup, course: CourseDetails):
        """Extract enrollment capacity and availability information"""
        try:
            text = soup.get_text()
            
            # Patterns for enrollment data
            patterns = {
                'class_capacity': r'Class Capacity[:\s]*(\d+)',
                'enrollment_total': r'Enrollment Total[:\s]*(\d+)',
                'available_seats': r'Available Seats[:\s]*(\d+)',
                'waitlist_capacity': r'Waitlist Capacity[:\s]*(\d+)',
                'waitlist_total': r'Waitlist Total[:\s]*(\d+)',
            }
            
            for field, pattern in patterns.items():
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    setattr(course, field, int(match.group(1)))
            
        except Exception as e:
            logger.debug(f"Error extracting enrollment details: {e}")
    
    def extract_course_description(self, soup: BeautifulSoup) -> str:
        """Extract course description"""
        try:
            # Look for description in various places
            desc_patterns = [
                ('div', {'class': re.compile('description', re.IGNORECASE)}),
                ('span', {'class': 'PSLONGEDITBOX'}),
                ('td', {'class': re.compile('description', re.IGNORECASE)}),
            ]
            
            for tag, attrs in desc_patterns:
                desc_elem = soup.find(tag, attrs)
                if desc_elem:
                    desc_text = desc_elem.get_text(strip=True)
                    if len(desc_text) > 50:  # Ensure it's a substantial description
                        return desc_text
            
            # Look for text after "Description" label
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
            
            # Look for attributes section
            attr_text = soup.get_text()
            
            # Common attribute patterns
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
            
            return list(set(attributes))  # Remove duplicates
            
        except Exception as e:
            logger.debug(f"Error extracting class attributes: {e}")
            return []
    
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
            
            # Fast regex-based subject extraction
            subjects = []
            
            # Pattern to match subject checkboxes and labels
            pattern = r'<input[^>]*id="PTS_SELECT\$(\d+)"[^>]*>.*?<label[^>]*id="PTS_SELECT_LBL\$\1"[^>]*>([^<]+)</label>'
            matches = re.findall(pattern, response.text, re.DOTALL)
            
            for checkbox_num, label_text in matches:
                if '/' in label_text:
                    parts = label_text.strip().split('/', 1)
                    code = parts[0].strip()
                    name = parts[1].strip() if len(parts) > 1 else code
                    
                    subjects.append({
                        'code': code,
                        'name': name,
                        'checkbox_id': f'PTS_SELECT${checkbox_num}',
                        'full_text': label_text.strip()
                    })
            
            subjects.sort(key=lambda x: x['code'])
            return subjects
            
        finally:
            self.return_session(session)
    
    def log_final_stats(self):
        """Log comprehensive final statistics"""
        duration = self.stats['end_time'] - self.stats['start_time']
        
        logger.info("📊 FINAL SCRAPING STATISTICS")
        logger.info("=" * 60)
        logger.info(f"⏱️  Total time: {duration}")
        logger.info(f"📚 Subjects processed: {self.stats['processed_subjects']}/{self.stats['total_subjects']}")
        logger.info(f"📖 Total courses: {self.stats['total_courses']}")
        logger.info(f"🔍 Detailed courses: {self.stats['detailed_courses']}")
        logger.info(f"❌ Failed subjects: {len(self.stats['failed_subjects'])}")
        logger.info(f"❌ Failed details: {self.stats['failed_details']}")
        
        if self.stats['total_courses'] > 0:
            courses_per_second = self.stats['total_courses'] / duration.total_seconds()
            logger.info(f"⚡ Rate: {courses_per_second:.2f} courses/second")

def save_results_optimized(courses: List[CourseDetails], output_file: str, format_type: str = 'jsonl'):
    """Optimized result saving"""
    logger.info(f"💾 Saving {len(courses)} courses to {output_file}...")
    
    if format_type.lower() == 'jsonl':
        with open(output_file, 'w', encoding='utf-8') as f:
            for course in courses:
                json.dump(asdict(course), f, ensure_ascii=False, separators=(',', ':'))
                f.write('\n')
    
    elif format_type.lower() == 'json':
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump([asdict(course) for course in courses], f, ensure_ascii=False, indent=2)
    
    elif format_type.lower() == 'csv':
        import csv
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if courses:
                # Flatten class_attributes for CSV
                flattened_courses = []
                for course in courses:
                    course_dict = asdict(course)
                    course_dict['class_attributes'] = '; '.join(course_dict.get('class_attributes', []))
                    flattened_courses.append(course_dict)
                
                writer = csv.DictWriter(f, fieldnames=flattened_courses[0].keys())
                writer.writeheader()
                writer.writerows(flattened_courses)

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Penn State LionPath Course Scraper - Enhanced High Performance')
    parser.add_argument('--output', '-o', default='psu_courses_enhanced.jsonl', help='Output file')
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
    
    logger.info("🎓 Penn State LionPath Course Scraper - Enhanced High Performance")
    logger.info(f"📁 Output file: {args.output}")
    logger.info(f"📊 Output format: {args.format}")
    logger.info(f"🏫 Campus filter: {args.campus}")
    logger.info(f"⚙️ Subject workers: {args.max_workers}")
    logger.info(f"⚙️ Detail workers: {args.max_detail_workers}")
    logger.info(f"⚡ Rate limit: {args.rate_limit} req/sec")
    logger.info(f"⏱️ Delay: {args.delay}s")
    
    scraper = HighPerformanceLionPathScraper(
        delay=args.delay,
        max_workers=args.max_workers,
        max_detail_workers=args.max_detail_workers,
        retry_attempts=args.retry_attempts,
        rate_limit_per_second=args.rate_limit
    )
    
    try:
        # Run the scraper
        courses = scraper.scrape_all_courses(
            campus_filter=args.campus,
            max_subjects=args.max_subjects
        )
        
        # Save results
        save_results_optimized(courses, args.output, args.format)
        
        logger.info(f"✅ Scraping completed successfully!")
        logger.info(f"💾 Results saved to: {args.output}")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Scraping interrupted by user")
    except Exception as e:
        logger.error(f"💥 Error during scraping: {e}")
        import traceback
        logger.debug(traceback.format_exc())

if __name__ == "__main__":
    main()
