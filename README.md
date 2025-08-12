# Penn State LionPath Course Scraper

⚠️ **WARNING**: This scraper was mainly vibecoded for personal use and is provided as-is. Use at your own risk. This tool likely violates Penn State's Terms of Service and LionPath usage policies. However, students gotta do what they gotta do. The maintainers are not responsible for any consequences of using this tool.

A high-performance course scraper for Penn State's LionPath system that extracts comprehensive course information including enrollment data, schedules, descriptions, and more.

## Features

- **Complete Course Data**: Scrapes all available course information including:
  - Course codes, titles, and descriptions
  - Enrollment capacity and availability
  - Class schedules and locations
  - Instructor information
  - Prerequisites and requirements
  - Class attributes and General Education categories

- **High Performance**: 
  - Aggressive parallelization with configurable worker pools
  - Rate limiting to avoid overwhelming servers
  - Session pooling for optimal connection reuse
  - Fast regex-based parsing where possible

- **University Park Focus**: 
  - Filters for University Park courses by default
  - Identifies and excludes World Campus and branch campus courses
  - Supports all campuses if needed

- **Multiple Output Formats**: 
  - JSONL (default, efficient for large datasets)
  - JSON (pretty formatted)
  - CSV (for spreadsheet analysis)

## Quick Start

### Installation

```bash
git clone https://github.com/yourusername/psu-course-scraper.git
cd psu-course-scraper
pip install -r requirements.txt
```

### Testing

```bash
# Test optimized scraper (recommended)
python test_optimized_scraper.py

# Test original scraper
python test_scraper.py

# Manual test with 5 subjects
python scraper_optimized.py --max-subjects 5 --debug
```

### Basic Usage

#### Optimized Scraper (Recommended)

```bash
# Scrape all University Park courses with optimized data structure
python scraper_optimized.py

# Custom settings with optimized scraper
python scraper_optimized.py --output courses.jsonl --max-workers 16 --max-detail-workers 50 --rate-limit 20

# Test mode
python scraper_optimized.py --max-subjects 10 --debug
```

#### Original Scraper (Legacy)

```bash
# Original scraper (larger file sizes, flat structure)
python scraper.py --output courses_flat.jsonl --max-workers 16 --rate-limit 15
```

### Command Line Options

```
--output, -o          Output file (default: psu_courses_enhanced.jsonl)
--format              Output format: jsonl, json, csv (default: jsonl)
--campus, -c          Campus filter: UP, ALL (default: UP)
--max-workers         Subject scraping workers (default: 16)
--max-detail-workers  Course detail workers (default: 50)
--rate-limit          Requests per second (default: 20)
--delay               Delay between requests (default: 0.2s)
--max-subjects        Limit subjects for testing
--retry-attempts      Retry attempts (default: 2)
--debug               Enable debug logging
```

## Performance

- **Typical Performance**: 15-25 courses per second
- **Full University Park Scrape**: ~9,000 courses in 6-10 minutes
- **Memory Usage**: <100MB for full dataset
- **Network**: Respectful rate limiting (20 req/sec default)

## Data Structure

### Optimized Format (Default)

The scraper uses an optimized data structure that separates course-level information from section-specific details, reducing redundancy and file size:

```json
{
  "course": {
    "course_code": "CMPSC 121",
    "course_title": "Introduction to Programming Techniques",
    "subject": "CMPSC",
    "catalog_number": "121",
    "units": "4.0",
    "course_description": "Basic programming concepts...",
    "enrollment_requirements": "Prerequisites...",
    "class_attributes": ["GenEd: GQ"],
    "academic_organization": "Computer Science and Engineering"
  },
  "sections": [
    {
      "section": "001",
      "class_number": "12345",
      "instructor": "Smith, John",
      "days": "MoWeFr",
      "times": "10:10AM - 11:00AM",
      "campus": "UP",
      "room": "IST 110",
      "class_capacity": 150,
      "enrollment_total": 142,
      "available_seats": 8,
      "status": "Open"
    },
    {
      "section": "002",
      "class_number": "12346",
      "instructor": "Johnson, Mary",
      "days": "TuTh",
      "times": "1:25PM - 2:40PM",
      "campus": "UP",
      "room": "IST 210",
      "class_capacity": 150,
      "enrollment_total": 138,
      "available_seats": 12,
      "status": "Open"
    }
  ],
  "stats": {
    "total_capacity": 300,
    "total_enrollment": 280,
    "available_seats": 20,
    "section_count": 2,
    "campuses": ["UP"]
  }
}
```

### Benefits of Optimized Structure

- **50-70% smaller file sizes** by eliminating redundant course information
- **Easier analysis** of courses with multiple sections
- **Built-in statistics** for capacity and enrollment aggregation
- **Logical separation** of course vs. section data

## Automated Workflow

This repository includes a GitHub Actions workflow that:
- Runs weekly (Sundays at 6 AM UTC)
- Scrapes all University Park courses
- Creates a Pull Request with updated data
- Includes performance statistics and summaries

## Ethical Considerations

- **Rate Limiting**: Configured to be respectful of Penn State's servers
- **Educational Use**: Intended for academic research and student tools
- **No Commercial Use**: Not for commercial applications
- **Data Privacy**: Only scrapes publicly available course information

## Legal Disclaimer

This tool is provided for educational and research purposes only. Users are responsible for:
- Complying with Penn State's Terms of Service
- Respecting LionPath usage policies
- Using scraped data responsibly
- Not overwhelming Penn State's servers

The maintainers assume no liability for any misuse of this tool or consequences thereof.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Support

This is a personal project with no official support. Issues and PRs are welcome but response time is not guaranteed.

## License

MIT License - see LICENSE file for details.

---

**Remember**: With great scraping power comes great responsibility. Use wisely! 🕷️
