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
# Quick test with limited data
python test_scraper.py

# Manual test with 5 subjects
python scraper.py --max-subjects 5 --debug
```

### Basic Usage

```bash
# Scrape all University Park courses (default)
python scraper.py

# Scrape with custom settings
python scraper.py --output courses.jsonl --max-workers 20 --rate-limit 30

# Test with limited subjects
python scraper.py --max-subjects 10 --debug
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

Each course record includes:

```json
{
  "course_code": "CMPSC 121",
  "course_title": "Introduction to Programming Techniques",
  "subject": "CMPSC",
  "catalog_number": "121",
  "section": "001",
  "class_number": "12345",
  "units": "4.0",
  "campus": "UP",
  "instructor": "Smith, John",
  "class_capacity": 150,
  "enrollment_total": 142,
  "available_seats": 8,
  "status": "Open",
  "course_description": "Basic programming concepts...",
  "days": "MoWeFr",
  "times": "10:10AM - 11:00AM",
  "room": "IST 110",
  "class_attributes": ["GenEd: GQ"],
  "semester": "Fall 2025"
}
```

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
