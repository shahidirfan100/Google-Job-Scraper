# Google Jobs Scraper - Enhanced Apify Actor

A robust and stealth Google Jobs scraper built with Crawlee CheerioCrawler and got-scraping for reliable job data extraction.

## 🚀 Features

### Core Functionality
- **Smart Job Extraction**: Dual extraction strategy using JSON-LD and DOM parsing
- **Advanced Pagination**: Handles Google Jobs pagination automatically
- **Detail Page Processing**: Fetches complete job information from individual job detail pages

### Anti-Detection & Stealth
- **Multiple User Agents**: Rotates between realistic browser user agents
- **Random Delays**: Implements human-like browsing patterns with random delays
- **Session Management**: Uses session pooling with cookie persistence
- **Anti-Bot Detection**: Detects and handles Google's anti-bot challenges
- **Rate Limiting**: Configurable request rates to avoid triggering protections

### Enhanced Selectors
- **Fallback Selectors**: Multiple CSS selector strategies for reliable data extraction
- **Dynamic Content Handling**: Handles Google's dynamic job card structures
- **Robust Pagination**: Multiple pagination detection methods

### Data Quality
- **Salary Extraction**: Detects salary information in multiple currencies
- **Job Type Detection**: Identifies full-time, part-time, contract, remote positions
- **Company Information**: Enhanced company name extraction from multiple sources
- **Apply URLs**: Extracts direct application links from job postings

## 📊 Input Configuration

```json
{
  "keyword": "Software Engineer",
  "location": "London", 
  "posted_date": "week",
  "results_wanted": 50,
  "maxRequestRetries": 5,
  "requestDelay": 3000,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "proxyGroups": ["RESIDENTIAL"],
    "countryCode": "US"
  }
}
```

### Input Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `keyword` | string | ✅ | Job search keyword (e.g., "Software Engineer") |
| `location` | string | ❌ | Job location (e.g., "London", "Remote") |
| `posted_date` | string | ❌ | Filter by posting date: `anytime`, `today`, `3days`, `week`, `month` |
| `results_wanted` | number | ❌ | Maximum number of jobs to scrape (default: 100) |
| `maxRequestRetries` | number | ❌ | Max retries for failed requests (default: 3) |
| `requestDelay` | number | ❌ | Delay between requests in ms (default: 2000) |
| `proxyConfiguration` | object | ❌ | Proxy settings for avoiding IP blocks |

## 📋 Output Format

Each scraped job contains:

```json
{
  "id": "unique_job_id",
  "title": "Senior Software Engineer",
  "company": "Tech Company Inc",
  "location": "San Francisco, CA",
  "date_posted": "2 days ago",
  "salary": "$120,000 - $180,000 per year",
  "job_type": "full-time",
  "description_text": "Clean text description...",
  "description_html": "<div>HTML description...</div>",
  "source": "linkedin.com",
  "url": "https://linkedin.com/jobs/apply/123456",
  "_source": "google.com/jobs",
  "_fetchedAt": "2024-01-15T10:30:00.000Z",
  "_scrapedUrl": "https://google.com/search?..."
}
```

## 🛡️ Anti-Detection Features

### User Agent Rotation
- Rotates between 4 different realistic Chrome user agents
- Includes proper browser headers (Sec-Fetch-*, DNT, etc.)
- Randomized on each request

### Request Patterns
- Random delays between requests (configurable, default 2-5 seconds)
- Human-like browsing behavior simulation
- Configurable rate limiting (default: 20 requests/minute)
- Session pooling with cookie persistence

### Anti-Bot Detection
- Automatic detection of CAPTCHA and "unusual traffic" pages
- Session retirement when blocked
- Automatic retry with new session
- Automatic session rotation on detection
- Proxy integration for IP rotation

### Error Handling
- Detects Google's "unusual traffic" challenges
- Automatic retry with new sessions
- Graceful degradation on failures

## 🔧 Technical Stack

- **Runtime**: Node.js 22 with ESM modules
- **Framework**: Apify SDK v3 + Crawlee v3
- **HTML Parsing**: Cheerio v1.0
- **HTTP Client**: Crawlee's built-in CheerioCrawler
- **Proxy Support**: Apify Proxy with residential IPs

## 🚦 Usage Recommendations

### For Best Results:
1. **Use Residential Proxies**: Essential for avoiding IP blocks
2. **Moderate Request Rates**: Keep `maxRequestsPerMinute` ≤ 60
3. **Add Delays**: Use `requestDelay` of 2000ms or higher
4. **Monitor Sessions**: Watch for anti-bot challenges in logs

### Scaling Considerations:
- Start with small batches (50-100 jobs) to test
- Increase `maxRequestRetries` for better success rates  
- Use `RESIDENTIAL` proxy groups for better success
- Monitor failure rates and adjust delays accordingly

## 📈 Performance Features

### Optimized Crawling
- Concurrent processing with controlled concurrency (max 5)
- Efficient request deduplication
- Smart pagination handling

### Data Extraction
- Multiple fallback selectors for reliability
- Enhanced text cleaning and normalization
- Comprehensive error handling

### Monitoring
- Detailed progress logging
- Success/failure rate tracking
- Performance metrics collection

## 🔍 Troubleshooting

### Common Issues:
1. **"Unusual Traffic" Messages**: Increase delays, use better proxies
2. **Empty Results**: Check if selectors need updating for new Google layout
3. **High Failure Rates**: Reduce concurrency, increase retry limits
4. **Missing Job Details**: Verify detail page handlers are working

### Debug Mode:
Enable detailed logging by setting log level to `DEBUG` in Apify console.

## 📝 Recent Enhancements

- ✅ Enhanced anti-bot detection and handling
- ✅ Multiple fallback selectors for job extraction
- ✅ Improved pagination logic with multiple detection methods  
- ✅ Salary and job type extraction
- ✅ Better error handling and session management
- ✅ Enhanced HTTP handling with Crawlee's built-in capabilities
- ✅ Comprehensive data validation and cleaning

## 🤝 Contributing

This scraper is designed to be robust and maintainable. When contributing:
1. Test with small batches first
2. Update selectors if Google changes their layout
3. Maintain anti-detection best practices
4. Document any new extraction methods

---

**Note**: This scraper respects rate limits and implements ethical scraping practices. Always review and comply with Google's Terms of Service.