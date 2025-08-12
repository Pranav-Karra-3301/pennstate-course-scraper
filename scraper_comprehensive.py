#!/usr/bin/env python3
"""
Comprehensive Penn State LionPath Course Scraper
Captures ALL available information including descriptions, enrollment details, etc.
Based on the working optimized scraper with enhanced data extraction
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
        logging.FileHandler('psu_scraper_comprehensive.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('scraper_comprehensive')

@dataclass
class ComprehensiveCourseInfo:
    """Comprehensive course-level information with ALL available fields"""
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
class ComprehensiveSectionInfo:
    """Comprehensive section-specific information with ALL available fields"""
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
class ComprehensiveCourseData:
    """Comprehensive course data structure with complete information"""
    course_info: ComprehensiveCourseInfo
    sections: List[ComprehensiveSectionInfo]
    
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

class ComprehensiveLionPathScraper:
    """Comprehensive scraper that captures ALL available course information"""
    
    def __init__(self, delay: float = 0.5, max_workers: int = 10, 
                 max_detail_workers: int = 20, retry_attempts: int = 3,
                 rate_limit_per_second: float = 15):
        # Use the correct URLs from the working scraper
        self.base_url = "https://public.lionpath.psu.edu"
        self.search_url = "https://public.lionpath.psu.edu/psc/CSPRD/EMPLOYEE/SA/c/PE_SR175_PUBLIC.PE_SR175_CLS_SRCH.GBL"
        
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
        """Get all available subject codes using the correct URL"""
        session = self.get_session()
        try:
            response = self.rate_limited_request(
                session.get,
                self.search_url,
                params={'Page': 'PE_SR175_CLS_SRCH', 'Action': 'U'},
                timeout=30
            )
            response.raise_for_status()
            
            subjects = []
            html_text = response.text
            logger.debug(f"HTML length: {len(html_text)}")
            
            # Use a simpler pattern that matches the actual HTML structure
            # Looking for: <label for='PTS_SELECT$31' id='PTS_SELECT_LBL$31' class='ps-label'>A-I / Artificial Intelligence</label>
            pattern = r"<label[^>]*for='PTS_SELECT\$(\d+)'[^>]*>([^<]+)</label>"
            matches = re.findall(pattern, html_text)
            
            if not matches:
                # Try with double quotes
                pattern = r'<label[^>]*for="PTS_SELECT\$(\d+)"[^>]*>([^<]+)</label>'
                matches = re.findall(pattern, html_text)
            
            logger.debug(f"Found {len(matches)} label matches")
            
            for checkbox_num, label_text in matches:
                label_text = label_text.strip()
                if '/' in label_text and len(label_text) > 5:
                    parts = label_text.split('/', 1)
                    code = parts[0].strip()
                    name = parts[1].strip() if len(parts) > 1 else code
                    
                    if re.match(r'^[A-Z]+-?[A-Z]*$', code):
                        subjects.append({
                            'code': code,
                            'name': name,
                            'checkbox_id': f'PTS_SELECT${checkbox_num}'
                        })
            
            # Also try to find subjects from the checkbox input elements themselves
            if not subjects:
                pattern = r"<input[^>]*id='PTS_SELECT\$(\d+)'[^>]*title=\"([^\"]+)\""
                matches = re.findall(pattern, html_text)
                
                for checkbox_num, title_text in matches:
                    if '/' in title_text:
                        parts = title_text.split('/', 1)
                        code = parts[0].strip()
                        name = parts[1].strip() if len(parts) > 1 else code
                        
                        if re.match(r'^[A-Z]+-?[A-Z]*$', code):
                            subjects.append({
                                'code': code,
                                'name': name,
                                'checkbox_id': f'PTS_SELECT${checkbox_num}'
                            })
                            
            
            # Remove duplicates
            seen = set()
            unique_subjects = []
            for s in subjects:
                if s['code'] not in seen:
                    seen.add(s['code'])
                    unique_subjects.append(s)
            
            logger.info(f"Found {len(unique_subjects)} subjects")
            return unique_subjects
            
        except Exception as e:
            logger.error(f"Error getting subjects: {e}")
            return []
        finally:
            self.return_session(session)
    
    def extract_form_data(self, html: str) -> Dict[str, str]:
        """Extract form data from HTML"""
        form_data = {}
        
        # Extract ICSID (handle both single and double quotes)
        icsid_match = re.search(r'<input[^>]*name=["\']ICSID["\'][^>]*value=["\']([^"\']+)["\']', html)
        if icsid_match:
            form_data['ICSID'] = icsid_match.group(1)
        
        # Extract ICStateNum
        state_match = re.search(r'<input[^>]*name=["\']ICStateNum["\'][^>]*value=["\']([^"\']+)["\']', html)
        if state_match:
            form_data['ICStateNum'] = state_match.group(1)
        
        # Extract other important hidden fields
        important_fields = ['ICType', 'ICElementNum', 'ICAction', 'ICXPos', 'ICYPos', 
                           'ResponsetoDiffFrame', 'TargetFrameName', 'FacetPath']
        
        for field in important_fields:
            pattern = rf'<input[^>]*name=["\']{field}["\'][^>]*value=["\']([^"\']*)["\']'
            match = re.search(pattern, html)
            if match:
                form_data[field] = match.group(1)
        
        # Set defaults for some fields if not found
        if 'ICAction' not in form_data:
            form_data['ICAction'] = ''
        if 'ICXPos' not in form_data:
            form_data['ICXPos'] = '0'
        if 'ICYPos' not in form_data:
            form_data['ICYPos'] = '0'
        
        return form_data
    
    def scrape_subject(self, subject: Dict, campus_filter: str = "UP") -> List[ComprehensiveSectionInfo]:
        """Scrape all sections for a subject with detailed information"""
        session = self.get_session()
        try:
            # First get the search page to extract form data
            response = self.rate_limited_request(
                session.get,
                self.search_url,
                params={'Page': 'PE_SR175_CLS_SRCH', 'Action': 'U'},
                timeout=10
            )
            response.raise_for_status()
            
            # Extract form data from the page
            form_data = self.extract_form_data(response.text)
            
            # Add the checkbox selection
            checkbox_id = subject.get('checkbox_id', '')
            if checkbox_id:
                form_data[checkbox_id] = 'Y'
                form_data['ICAction'] = checkbox_id
                
                # Submit the form with the selected subject
                response = self.rate_limited_request(
                    session.post,
                    self.search_url,
                    data=form_data,
                    timeout=30
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to get {subject['code']}: {response.status_code}")
                    return []
                
                sections = self.parse_subject_sections(response.text, subject['code'])
                
                # Get detailed information for each section
                detailed_sections = []
                for section in sections:
                    detailed_section = self.get_section_details(session, section)
                    detailed_sections.append(detailed_section)
                
                return detailed_sections
            
            return []
            
        except Exception as e:
            logger.error(f"Error scraping {subject['code']}: {e}")
            return []
        finally:
            self.return_session(session)
    
    def parse_subject_sections(self, html: str, subject_code: str) -> List[ComprehensiveSectionInfo]:
        """Parse sections from subject page HTML using regex for speed"""
        sections = []
        
        # Use regex pattern to find showClassDetails links (from optimized scraper)
        pattern = r'javascript:showClassDetails\((\d+),(\d+)\)[^>]*>([^<]+)<'
        matches = re.findall(pattern, html)
        
        for strm, class_nbr, text in matches:
            try:
                section = ComprehensiveSectionInfo()
                section.class_number = class_nbr
                section.detail_url = f"showClassDetails({strm},{class_nbr})"
                
                # Parse the text to extract course info
                # Format could be like "A-I 285 - 555V" or variations
                text = text.strip()
                
                # Try to extract course code and section
                course_match = re.match(r'^([A-Z]+-?[A-Z]*\s+\d+[A-Z]*)\s*-\s*(\S+)', text)
                if course_match:
                    section.course_code = course_match.group(1)
                    section.section = course_match.group(2)
                else:
                    # If no section, use whole text as course code
                    section.course_code = text.split('-')[0].strip() if '-' in text else text
                
                # Try to extract additional info from the HTML around this match
                # Look for enrollment info near the class number
                enrollment_pattern = f'CLASS_NBR">{class_nbr}</span>.*?(\d+)/(\d+)'
                enroll_match = re.search(enrollment_pattern, html[:html.find(f'showClassDetails({strm},{class_nbr})')+1000], re.DOTALL)
                if enroll_match:
                    section.enrollment_total = int(enroll_match.group(1))
                    section.class_capacity = int(enroll_match.group(2))
                    section.available_seats = section.class_capacity - section.enrollment_total
                
                # Look for status (Open/Closed)
                status_pattern = f'{class_nbr}.*?(Open|Closed|Wait List)'
                status_match = re.search(status_pattern, html[:html.find(f'showClassDetails({strm},{class_nbr})')+500])
                if status_match:
                    section.status = status_match.group(1)
                
                # Look for instructor
                instructor_pattern = f'{class_nbr}.*?Instructor:.*?>([^<]+)<'
                instructor_match = re.search(instructor_pattern, html[:html.find(f'showClassDetails({strm},{class_nbr})')+1000])
                if instructor_match:
                    section.instructor = instructor_match.group(1).strip()
                
                # Look for days/times
                days_pattern = f'{class_nbr}.*?(Mo|Tu|We|Th|Fr|Sa|Su)[A-Za-z]*.*?(\d{{1,2}}:\d{{2}}[AP]M)'
                days_match = re.search(days_pattern, html[:html.find(f'showClassDetails({strm},{class_nbr})')+1000])
                if days_match:
                    # Extract full days string
                    days_text = re.search(f'{class_nbr}.*?([MoTuWeThFrSaSu][A-Za-z, ]*)', html[:html.find(f'showClassDetails({strm},{class_nbr})')+1000])
                    if days_text:
                        section.days = days_text.group(1).strip()
                
                # Determine campus from section number patterns
                if section.section:
                    if section.section.endswith('W'):
                        section.campus = "World Campus"
                    elif any(x in text.upper() for x in ['ABINGTON', 'ALTOONA', 'BERKS', 'HAZLETON']):
                        for campus in ['Abington', 'Altoona', 'Berks', 'Hazleton', 'Erie', 'Harrisburg']:
                            if campus.upper() in text.upper():
                                section.campus = campus
                                break
                    else:
                        section.campus = "UP"
                
                sections.append(section)
                
            except Exception as e:
                logger.debug(f"Error parsing section: {e}")
                continue
        
        logger.debug(f"Parsed {len(sections)} sections for {subject_code}")
        return sections
    
    def get_section_details(self, session: requests.Session, section: ComprehensiveSectionInfo) -> ComprehensiveSectionInfo:
        """Get comprehensive detailed information for a section"""
        try:
            if not section.detail_url:
                return section
            
            # Make request to detail page
            detail_url = f"{self.base_url}{section.detail_url}" if not section.detail_url.startswith('http') else section.detail_url
            
            response = self.rate_limited_request(
                session.get,
                detail_url,
                timeout=30
            )
            
            if response.status_code == 200:
                return self.parse_detailed_section_info(response.text, section)
            
            return section
            
        except Exception as e:
            logger.debug(f"Error getting section details for {section.class_number}: {e}")
            return section
    
    def parse_detailed_section_info(self, html: str, base_section: ComprehensiveSectionInfo) -> ComprehensiveSectionInfo:
        """Parse comprehensive detailed section information from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Keep all existing data
            section = base_section
            
            # Extract course description (most important!)
            desc_elem = soup.find('span', {'id': re.compile(r'DERIVED_CLSRCH_DESCRLONG')})
            if not desc_elem:
                desc_elem = soup.find('div', {'id': re.compile(r'SSR_CLS_DTL_WRK_DESCRLONG')})
            if desc_elem:
                desc_text = desc_elem.get_text(strip=True)
                if desc_text and len(desc_text) > 10:
                    # Store in section for now, will be moved to course level later
                    section.class_notes = f"COURSE_DESC: {desc_text}"
            
            # Extract enrollment requirements
            req_elem = soup.find('span', {'id': re.compile(r'SSR_CLS_DTL_WRK_SSR_REQUISITE_LONG')})
            if req_elem:
                req_text = req_elem.get_text(strip=True)
                if req_text and req_text != "No Enrollment Requirements":
                    if section.class_notes:
                        section.class_notes += f" | ENROLLMENT_REQ: {req_text}"
                    else:
                        section.class_notes = f"ENROLLMENT_REQ: {req_text}"
            
            # Extract class attributes
            attr_elem = soup.find('span', {'id': re.compile(r'SSR_CLS_DTL_WRK_SSR_CRSE_ATTR_LONG')})
            if attr_elem:
                attr_text = attr_elem.get_text(strip=True)
                if attr_text and attr_text != "No Class Attributes":
                    if section.class_notes:
                        section.class_notes += f" | ATTRIBUTES: {attr_text}"
                    else:
                        section.class_notes = f"ATTRIBUTES: {attr_text}"
            
            # Extract units/credits
            units_elem = soup.find('span', {'id': re.compile(r'SSR_CLS_DTL_WRK_UNITS_RANGE')})
            if units_elem:
                units_text = units_elem.get_text(strip=True)
                if units_text:
                    if section.class_notes:
                        section.class_notes += f" | UNITS: {units_text}"
                    else:
                        section.class_notes = f"UNITS: {units_text}"
            
            # Extract grading basis
            grading_elem = soup.find('span', {'id': re.compile(r'GRADE_BASIS_TBL_DESCRFORMAL')})
            if grading_elem:
                grading_text = grading_elem.get_text(strip=True)
                if grading_text:
                    if section.class_notes:
                        section.class_notes += f" | GRADING: {grading_text}"
                    else:
                        section.class_notes = f"GRADING: {grading_text}"
            
            # Extract component (Lecture/Lab/etc)
            comp_elem = soup.find('span', {'id': re.compile(r'SSR_CLS_DTL_WRK_SSR_COMPONENT_LONG')})
            if comp_elem:
                section.section_type = comp_elem.get_text(strip=True)
            
            # Extract career level
            career_elem = soup.find('span', {'id': re.compile(r'PSXLATITEM_XLATLONGNAME')})
            if career_elem:
                career_text = career_elem.get_text(strip=True)
                if career_text:
                    if section.class_notes:
                        section.class_notes += f" | CAREER: {career_text}"
                    else:
                        section.class_notes = f"CAREER: {career_text}"
            
            # Extract meeting information
            meeting_table = soup.find('table', {'id': re.compile(r'SSR_CLSRCH_MTG')})
            if meeting_table:
                rows = meeting_table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 7:
                        # Days and Times
                        if not section.days:
                            section.days = cells[0].get_text(strip=True)
                        if not section.times:
                            section.times = cells[1].get_text(strip=True)
                        # Room
                        if not section.room:
                            section.room = cells[2].get_text(strip=True)
                        # Instructor
                        if not section.instructor:
                            section.instructor = cells[3].get_text(strip=True)
                        # Dates
                        if not section.meeting_dates:
                            section.meeting_dates = cells[4].get_text(strip=True)
            
            # Extract enrollment details
            enroll_table = soup.find('table', {'id': re.compile(r'SSR_CLS_DTL_WRK_SSR_ENRL_STAT')})
            if enroll_table:
                cells = enroll_table.find_all('span')
                for cell in cells:
                    text = cell.get_text(strip=True)
                    # Class Capacity
                    if 'Class Capacity' in text:
                        next_cell = cell.find_next_sibling('span')
                        if next_cell:
                            try:
                                section.class_capacity = int(next_cell.get_text(strip=True))
                            except:
                                pass
                    # Enrollment Total
                    elif 'Enrollment Total' in text:
                        next_cell = cell.find_next_sibling('span')
                        if next_cell:
                            try:
                                section.enrollment_total = int(next_cell.get_text(strip=True))
                            except:
                                pass
                    # Available Seats
                    elif 'Available Seats' in text:
                        next_cell = cell.find_next_sibling('span')
                        if next_cell:
                            try:
                                section.available_seats = int(next_cell.get_text(strip=True))
                            except:
                                pass
                    # Wait List
                    elif 'Wait List Capacity' in text:
                        next_cell = cell.find_next_sibling('span')
                        if next_cell:
                            try:
                                section.waitlist_capacity = int(next_cell.get_text(strip=True))
                            except:
                                pass
                    elif 'Wait List Total' in text:
                        next_cell = cell.find_next_sibling('span')
                        if next_cell:
                            try:
                                section.waitlist_total = int(next_cell.get_text(strip=True))
                            except:
                                pass
            
            # Extract class notes (actual class notes, not our temporary storage)
            notes_elem = soup.find('span', {'id': re.compile(r'DERIVED_CLSRCH_SSR_CLASSNOTE_LONG')})
            if notes_elem:
                notes_text = notes_elem.get_text(strip=True)
                if notes_text and notes_text != "No Class Notes":
                    if section.class_notes and not section.class_notes.startswith("COURSE_DESC"):
                        section.class_notes = notes_text + " | " + section.class_notes
                    elif not section.class_notes:
                        section.class_notes = notes_text
            
            # Extract consent requirements
            consent_elem = soup.find('span', {'id': re.compile(r'SSR_CLS_DTL_WRK_CONSENT_DESCR')})
            if consent_elem:
                consent_text = consent_elem.get_text(strip=True)
                if consent_text and consent_text != "No Special Consent Required":
                    section.add_consent = consent_text
            
            return section
            
        except Exception as e:
            logger.debug(f"Error parsing detailed section info: {e}")
            return base_section
    
    def organize_courses(self, sections: List[ComprehensiveSectionInfo]) -> Dict[str, ComprehensiveCourseData]:
        """Organize sections into courses with comprehensive information"""
        courses = {}
        
        for section in sections:
            course_code = section.course_code
            
            if course_code not in courses:
                # Create course info from section data
                course_info = ComprehensiveCourseInfo(course_code=course_code)
                
                # Extract course-level info from section notes if available
                if section.class_notes:
                    # Extract course description
                    desc_match = re.search(r'COURSE_DESC: ([^|]+)', section.class_notes)
                    if desc_match:
                        course_info.course_description = desc_match.group(1).strip()
                    
                    # Extract enrollment requirements
                    req_match = re.search(r'ENROLLMENT_REQ: ([^|]+)', section.class_notes)
                    if req_match:
                        course_info.enrollment_requirements = req_match.group(1).strip()
                    
                    # Extract attributes
                    attr_match = re.search(r'ATTRIBUTES: ([^|]+)', section.class_notes)
                    if attr_match:
                        course_info.class_attributes = [attr_match.group(1).strip()]
                    
                    # Extract units
                    units_match = re.search(r'UNITS: ([^|]+)', section.class_notes)
                    if units_match:
                        course_info.units = units_match.group(1).strip()
                    
                    # Extract grading
                    grading_match = re.search(r'GRADING: ([^|]+)', section.class_notes)
                    if grading_match:
                        course_info.grading = grading_match.group(1).strip()
                    
                    # Extract career
                    career_match = re.search(r'CAREER: ([^|]+)', section.class_notes)
                    if career_match:
                        course_info.career = career_match.group(1).strip()
                
                # Parse subject and catalog number
                match = re.match(r'^([A-Z]+-?[A-Z]*)\s+(\d+[A-Z]*)$', course_code)
                if match:
                    course_info.subject = match.group(1)
                    course_info.catalog_number = match.group(2)
                
                # Set component from first section
                if section.section_type:
                    course_info.component = section.section_type
                
                courses[course_code] = ComprehensiveCourseData(
                    course_info=course_info,
                    sections=[]
                )
            
            # Clean up section notes to remove course-level info
            if section.class_notes:
                # Remove the temporary course-level markers
                cleaned_notes = re.sub(r'(COURSE_DESC|ENROLLMENT_REQ|ATTRIBUTES|UNITS|GRADING|CAREER): [^|]+\|?\s*', '', section.class_notes)
                section.class_notes = cleaned_notes.strip()
            
            courses[course_code].sections.append(section)
        
        return courses
    
    def scrape_all_courses(self, campus_filter: str = "UP", max_subjects: int = None) -> Dict[str, ComprehensiveCourseData]:
        """Main method to scrape all courses with comprehensive information"""
        self.stats['start_time'] = datetime.now()
        logger.info("🚀 Starting Comprehensive LionPath scraping...")
        
        try:
            # Get all subjects
            logger.info("📚 Getting all subject codes...")
            subjects = self.get_all_subjects()
            self.stats['total_subjects'] = len(subjects)
            
            if max_subjects:
                subjects = subjects[:max_subjects]
                logger.info(f"Limited to {max_subjects} subjects for testing")
            
            logger.info(f"Found {len(subjects)} subjects to process")
            
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
                            logger.info(f"✅ {subject['code']}: {len(sections)} sections with details")
                        self.stats['processed_subjects'] += 1
                    except Exception as e:
                        self.stats['failed_subjects'].append(subject['code'])
                        logger.error(f"❌ {subject['code']} failed: {e}")
            
            # Organize into courses
            logger.info("📊 Organizing sections by course...")
            self.courses_data = self.organize_courses(all_sections)
            
            # Update statistics
            self.stats['unique_courses'] = len(self.courses_data)
            self.stats['total_sections'] = sum(
                len(course.sections) for course in self.courses_data.values()
            )
            self.stats['detailed_sections'] = sum(
                1 for course in self.courses_data.values()
                for section in course.sections
                if section.class_capacity > 0 or section.instructor
            )
            self.stats['end_time'] = datetime.now()
            
            # Log summary
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            logger.info(f"""
            ============================================
            📊 COMPREHENSIVE SCRAPING COMPLETE
            ============================================
            ⏱️  Duration: {duration:.2f} seconds
            📚 Subjects: {self.stats['processed_subjects']}/{self.stats['total_subjects']}
            🎓 Unique courses: {self.stats['unique_courses']}
            📝 Total sections: {self.stats['total_sections']}
            🔍 Detailed sections: {self.stats['detailed_sections']}
            ❌ Failed subjects: {len(self.stats['failed_subjects'])}
            ============================================
            """)
            
            return self.courses_data
            
        except Exception as e:
            logger.error(f"💥 Scraping failed: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return self.courses_data

def save_comprehensive_results(courses_data: Dict[str, ComprehensiveCourseData], 
                              output_file: str, format: str = "jsonl"):
    """Save comprehensive results to file"""
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
    
    parser = argparse.ArgumentParser(description='Comprehensive Penn State LionPath Course Scraper')
    parser.add_argument('--output', default='psu_courses_comprehensive.jsonl', 
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
    scraper = ComprehensiveLionPathScraper(
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
        
        save_comprehensive_results(courses_data, output_file, args.format)
        logger.info(f"✅ Results saved to {output_file}")
    else:
        logger.warning("⚠️ No data to save")
    
    return 0

if __name__ == "__main__":
    exit(main())