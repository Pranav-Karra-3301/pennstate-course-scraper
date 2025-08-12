# Penn State LionPath Course Scraper - Instructions

## Overview

This scraper extracts comprehensive course information from Penn State's LionPath system. It's designed to be fast, respectful, and comprehensive.

## How It Works

### 1. Subject Discovery
- Loads the LionPath course search page
- Extracts all available subject codes (CMPSC, MATH, etc.)
- Uses regex parsing for speed

### 2. Course List Extraction
- For each subject, submits a filtered search request
- Parses course listings from the results
- Extracts basic course information (code, section, campus)

### 3. Detailed Course Information
- For each course found, makes additional requests to get detailed information
- Extracts comprehensive data including:
  - Full course descriptions
  - Enrollment capacity and current enrollment
  - Instructor information
  - Meeting times and locations
  - Prerequisites and requirements
  - General Education attributes

### 4. Data Processing
- Filters courses by campus (University Park by default)
- Normalizes and structures data
- Outputs in specified format (JSONL, JSON, or CSV)

## Technical Details

### Architecture
- **Multi-threaded**: Uses ThreadPoolExecutor for concurrent processing
- **Session Pooling**: Reuses HTTP sessions for efficiency
- **Rate Limiting**: Configurable requests per second to be respectful
- **Error Handling**: Comprehensive retry logic with exponential backoff

### Performance Optimizations
- **Aggressive Parallelization**: Separate worker pools for subjects and course details
- **Fast Parsing**: Regex-based parsing where possible instead of full DOM parsing
- **Connection Reuse**: HTTP session pooling to avoid connection overhead
- **Memory Efficient**: Streaming processing to handle large datasets

### Rate Limiting
- Default: 20 requests per second
- Configurable via `--rate-limit` parameter
- Uses bounded semaphore for precise control
- Tracks request timing to avoid server overload

## Configuration

### Environment Setup
```bash
# Install dependencies
pip install requests beautifulsoup4 aiohttp

# Or use requirements.txt
pip install -r requirements.txt
```

### Command Line Usage
```bash
# Basic usage - scrape all UP courses
python scraper.py

# High performance mode
python scraper.py --max-workers 20 --max-detail-workers 100 --rate-limit 50

# Conservative mode (slower but more respectful)
python scraper.py --max-workers 5 --max-detail-workers 10 --rate-limit 5 --delay 1.0

# Test mode
python scraper.py --max-subjects 5 --debug

# All campuses
python scraper.py --campus ALL

# Different output formats
python scraper.py --format json --output courses.json
python scraper.py --format csv --output courses.csv
```

## Understanding the Output

### JSONL Format (Default)
- One JSON object per line
- Efficient for large datasets
- Easy to stream and process
- Can be loaded with: `pd.read_json('file.jsonl', lines=True)`

### JSON Format
- Pretty-printed JSON array
- Good for smaller datasets
- Human-readable
- Standard JSON format

### CSV Format
- Spreadsheet-compatible
- All fields flattened
- Arrays (like class_attributes) joined with semicolons

## Data Quality

### Course Identification
- Uses multiple methods to identify courses
- Validates course codes against known patterns
- Handles edge cases (hyphenated subjects, etc.)

### Campus Detection
- Smart campus filtering based on multiple signals:
  - Explicit campus fields
  - Section suffixes (Y = World Campus, etc.)
  - Location indicators
  - Default assumptions

### Error Handling
- Graceful degradation - courses without detailed info still included
- Comprehensive logging for debugging
- Statistics tracking for monitoring

## Troubleshooting

### Common Issues

1. **Rate Limiting Errors**
   - Reduce `--rate-limit` parameter
   - Increase `--delay` parameter
   - Reduce `--max-workers`

2. **Timeout Errors**
   - Check internet connection
   - Penn State servers may be slow/overloaded
   - Try running during off-peak hours

3. **Empty Results**
   - Check if LionPath site is accessible
   - Verify semester is correct (hardcoded to Fall 2025)
   - Run with `--debug` for detailed logging

4. **Memory Issues**
   - Use JSONL format instead of JSON
   - Process in smaller batches with `--max-subjects`
   - Reduce worker counts

### Debugging

```bash
# Enable debug logging
python scraper.py --debug

# Test with small dataset
python scraper.py --max-subjects 3 --debug

# Conservative settings
python scraper.py --max-workers 2 --max-detail-workers 5 --rate-limit 2 --delay 2.0
```

### Log Files
- Main log: `psu_scraper_enhanced.log`
- Contains detailed execution information
- Useful for debugging failures

## Maintenance

### Updating for New Semesters
1. Update semester in `scraper.py` (search for "Fall 2025")
2. Verify LionPath URLs haven't changed
3. Test with small dataset first

### Monitoring Performance
- Check log files for timing information
- Monitor success/failure rates
- Adjust rate limits based on server response

### Legal Compliance
- Respect robots.txt if it exists
- Don't run too frequently (automated workflow runs weekly)
- Monitor for any usage notifications from Penn State
- Be prepared to modify or discontinue if requested

## Integration

### With Other Tools
```python
import pandas as pd
import json

# Load JSONL data
df = pd.read_json('courses.jsonl', lines=True)

# Load JSON data
with open('courses.json') as f:
    courses = json.load(f)

# Process data
up_cmpsc = df[(df['campus'] == 'UP') & (df['subject'] == 'CMPSC')]
```

### API Usage
The scraper can be imported and used programmatically:
```python
from scraper import HighPerformanceLionPathScraper

scraper = HighPerformanceLionPathScraper(
    delay=0.5,
    max_workers=10,
    rate_limit_per_second=10
)

courses = scraper.scrape_all_courses(campus_filter="UP")
```

---

## Final Notes

- This tool is for educational use only
- Be respectful of Penn State's servers
- Monitor your usage and adjust parameters as needed
- Keep the scraper updated as LionPath changes
- Consider the ethical implications of automated scraping
