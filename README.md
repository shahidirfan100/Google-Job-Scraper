## What does Google Jobs Scraper do?

Google Jobs Scraper collects job listings from the Google Jobs search vertical. Enter a keyword, a location, or a Google Jobs search URL, and the Actor returns structured records with the job title, company, location, salary range, posting date, description, qualifications, source, and application links.

Google Jobs shows up to 10 listings for one search request. When you ask for more than 10 results, the Actor runs related search variations for the same role and location, removes duplicates, and keeps collecting until it reaches your limit or your request budget.

## Why use Google Jobs Scraper?

- **One dataset across job boards.** Google Jobs aggregates listings from company career sites, LinkedIn, Indeed, Glassdoor, ZipRecruiter, Monster, and other sources. The Actor keeps the original source label on each record.
- **Fields you can filter and analyze.** Salary ranges, posting dates, job types, remote flags, qualifications, and apply links are separated, so the dataset is ready for spreadsheets, dashboards, or an applicant tracking workflow.
- **One request returns ten full listings.** Each Google Jobs result page includes the description, qualifications, benefits, and apply links for all ten jobs on that page. The Actor does not open a separate detail page for every job.
- **Repeatable runs.** Schedule the Actor to refresh a role, location, or market list, then export the same fields on every run.
- **Apify platform output.** Results go to the default dataset, where you can download CSV, Excel, JSON, or XML, call the dataset API, or send records to another system with a webhook.

## What data can you extract from Google Jobs?

| Field | Description |
|-------|-------------|
| `id` | Google Jobs document ID for the listing |
| `title` | Job title |
| `company` | Hiring company, when Google shows it |
| `location` | City, region, country, or `Anywhere` |
| `salary` | Salary range from the listing, when published |
| `job_type` | Full time, part time, contract, internship, temporary, freelance, or per diem |
| `date_posted` | Estimated posting date in ISO format |
| `date_posted_text` | Original relative date such as `2 days ago` |
| `description_text` | Job description text |
| `qualifications` | Qualifications section, when present |
| `benefits` | Benefits section, when present |
| `is_remote` | Whether the listing mentions remote work or work from home |
| `apply_url` | Primary application link |
| `apply_options` | All application links with their source labels |
| `source` | Job board or career site Google credits for the listing |
| `url` | Google Jobs link for the specific listing |

## How to use Google Jobs Scraper

1. Open the Actor in Apify Console.
2. Enter a keyword, or paste a Google Jobs search URL.
3. Add a location if you want to narrow the search.
4. Set the number of jobs and the search request budget.
5. Run the Actor and open the dataset.
6. Download the dataset or connect it to your workflow.

The default proxy configuration uses Apify's Google SERP proxy group, the group verified for this target. You can select another Apify group or provide custom proxy URLs instead. Google Jobs blocks direct requests from shared IPs, so keep a proxy enabled.

## Input Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `startUrl` | String | No | - | Google Jobs search URL. Takes precedence over keyword and location. |
| `keyword` | String | No | `software engineer` | Role or search terms, such as `data analyst` or `product manager`. |
| `location` | String | No | - | Location to include in the search, such as `New York, NY`, `London`, or `Remote`. |
| `posted_date` | String | No | `anytime` | Keep jobs from `today`, `last3days`, `last7days`, or `last14days`. |
| `results_wanted` | Integer | No | `20` | Stop after this many jobs have been saved. |
| `max_pages` | Integer | No | `10` | Starting Google Jobs request budget. Each request returns up to 10 listings. If the budget is too low for `results_wanted`, the Actor raises it and logs the change. |
| `proxyConfiguration` | Object | No | Google SERP proxy | Apify Proxy or custom proxy settings. The default uses the Google SERP group; you can select another group or provide custom URLs. |

## Output Data

| Field | Type | Description |
|-------|------|-------------|
| `id` | String | Google Jobs document ID |
| `title` | String | Job title |
| `company` | String or null | Hiring company |
| `location` | String or null | Job location |
| `salary` | String or null | Published salary range |
| `job_type` | String or null | Employment type |
| `employment_type` | String or null | Alias of `job_type` |
| `date_posted` | String or null | ISO date estimated from the listing |
| `date_posted_text` | String or null | Original posting date text |
| `description_text` | String | Description text |
| `qualifications` | String or null | Qualifications section |
| `benefits` | String or null | Benefits section |
| `is_remote` | Boolean | Remote-work signal |
| `apply_url` | String or null | Primary application link |
| `apply_options` | Array | Application links with source labels |
| `source` | String | Job board or career site credited by Google |
| `url` | String | Google Jobs listing URL |

## Usage Examples

### Search by keyword

Collect software engineering jobs across the default market:

```json
{
  "keyword": "software engineer",
  "results_wanted": 50,
  "max_pages": 10
}
```

### Search by keyword and location

Collect product management jobs in New York:

```json
{
  "keyword": "product manager",
  "location": "New York, NY",
  "results_wanted": 40,
  "max_pages": 8
}
```

### Start from a Google Jobs URL

Use a search URL you already built in Google:

```json
{
  "startUrl": "https://www.google.com/search?q=data+engineer&udm=8&hl=en&gl=us",
  "results_wanted": 30,
  "max_pages": 6
}
```

### Use a custom datacenter proxy

Provide your own datacenter proxy URLs instead of an Apify Proxy group:

```json
{
  "keyword": "software engineer",
  "results_wanted": 100,
  "proxyConfiguration": {
    "useApifyProxy": false,
    "proxyUrls": [
      "http://user:password@datacenter-proxy.example.com:8080"
    ]
  }
}
```

Replace the example proxy URL with your own. The Actor uses the proxy URLs exactly as supplied.

### Recent jobs only

Keep listings from the last seven days:

```json
{
  "keyword": "software engineer",
  "location": "United States",
  "posted_date": "last7days",
  "results_wanted": 25,
  "max_pages": 5
}
```

## Sample Output

```json
{
  "id": "wpX3mct44K74t8e8AAAAAA==",
  "title": "Staff Software Engineer, Full Stack Jobs",
  "company": "True Anomaly. Inc.",
  "location": "Denver, CO",
  "salary": "205K-315K a year",
  "job_type": "Full time",
  "employment_type": "Full time",
  "date_posted": "2026-10-30T16:38:13.924Z",
  "date_posted_text": "1 day ago",
  "description_text": "You do not need to have experience building space ground systems or experience in aerospace. You design and champion unified patterns, drive their adoption across teams, and measure their impact on velocity and reliability.",
  "qualifications": "12+ years of software development experience. Expert-level mastery of HTML/CSS and modern TypeScript/JavaScript. Experience with React, Elixir, and large-scale frontend systems.",
  "benefits": "Dental insurance, health insurance, paid time off.",
  "is_remote": false,
  "apply_url": "https://www.google.com/goto?url=EXAMPLE_TOKEN",
  "apply_options": [
    {
      "source": "Clearance Jobs",
      "url": "https://www.google.com/goto?url=EXAMPLE_TOKEN"
    }
  ],
  "source": "Clearance Jobs",
  "url": "https://www.google.com/search?ibp=htl;jobs&q=software+engineer&htidocid=EXAMPLE&hl=en&gl=us"
}
```

The `apply_url` and `url` values above are shortened examples. A real run returns the full Google redirect link and the full document ID.

## Tips for Best Results

- Use a role and a place in the keyword when you want a narrow search, such as `backend engineer Austin`.
- Keep `max_pages` at 20 or higher when you request 100 jobs. Each request returns up to 10 listings, and related searches add overlap.
- For 200 jobs, `max_pages` 40 is a good starting budget. If the budget is too low for the target, the Actor raises it and logs the change.
- Start with 20 to 30 jobs for a test run, then raise `results_wanted` for the full collection.
- `posted_date` filters only listings with a recognizable date. Listings without a date remain in the dataset.
- If a company or salary is blank, Google did not publish that field for the listing.
- Keep the default Google SERP proxy enabled. Direct requests are usually blocked. If that group is blocked for your account, switch to DATACENTER, RESIDENTIAL, or a custom proxy.
- If a run returns fewer jobs than requested, raise `max_pages` or use a broader keyword.

## Integrations

- **Google Sheets** - Send the dataset to a spreadsheet for review or sharing.
- **Webhooks** - Trigger a downstream workflow when a run finishes.
- **Make or Zapier** - Connect job records to a CRM, ATS, or notification flow.
- **Apify API** - Read datasets programmatically from your own application.
- **CSV, Excel, JSON, XML** - Download the dataset in the format your tools accept.

## Frequently Asked Questions

### Does this Actor collect Google Careers jobs or Google Jobs results?

It collects Google Jobs results, the job listings shown in Google Search. It does not collect Google's internal Careers openings.

### Does the Actor request every job detail page?

No. Each Google Jobs result page contains up to 10 full listings, including descriptions, qualifications, benefits, and apply links. The Actor parses all ten from the same response, so the request count follows the number of searches, not the number of jobs.

### Why did the run stop before my target?

A low `max_pages` no longer stops the run early. The Actor raises the request budget when it is too low for `results_wanted` and logs the change. A run can still stop when the related searches run out or when Google blocks requests.

### Why did I get fewer jobs than I requested?

Google Jobs returns up to 10 listings per search request. The Actor runs related searches to add more unique listings. If the role is narrow or the market has few postings, raise `max_pages` or use a broader keyword.

### Why is a company, salary, or location missing?

Google does not publish every field for every listing. The Actor keeps the record and leaves the missing field empty instead of inventing a value.

### Do I need a proxy?

Yes, for reliable results. Google Jobs blocks direct requests from shared IPs. The default uses Apify's Google SERP proxy group. If that group is blocked for your account, select DATACENTER, RESIDENTIAL, or a custom proxy.

### Can I export the dataset to CSV or Excel?

Yes. Apify datasets can be downloaded as CSV, Excel, JSON, XML, and other supported formats.

### Can I run this Actor on a schedule?

Yes. Create a schedule in Apify Console and set the input you want to refresh, such as a role and location.

### Are the application links direct company links?

The application links go through a Google redirect and then to the source Google credits, such as a company career site or a job board. The `source` field names that source.

### Is it legal to collect Google Jobs listings?

The listings are public. You are responsible for following applicable laws, the source site's terms, and your own data-use policies.

## Support

For issues or feature requests, use the Issues tab on the Actor page or contact the developer through Apify.

## Legal Notice

This Actor collects publicly available job listing data. Users are responsible for using the data lawfully and for complying with applicable website terms and privacy rules.
