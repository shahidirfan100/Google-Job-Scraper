// Google Careers Jobs scraper - HTTP-based with Cheerio
// Runtime: Node 22, ESM ("type": "module")
// Uses apify@^3 and crawlee@^3 with CheerioCrawler

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { gotScraping } from 'got-scraping';

await Actor.init();

// ------------------------- INPUT -------------------------
const input = await Actor.getInput() ?? {};
const {
    startUrl = '',
    keyword = '',
    location = '',
    posted_date = 'anytime',
    results_wanted: RESULTS_WANTED_RAW = 100,
    max_pages = 20,
    collectDetails = true,
    proxyConfiguration,
    maxRequestRetries = 3,
    requestDelay = 2000,
} = input;

// Determine if using startUrl or building from keyword/location
const useStartUrl = Boolean(startUrl);
if (!useStartUrl && !keyword) {
    throw new Error('Either "startUrl" or "keyword" is required.');
}

const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;
log.info(`🚀 Starting Google Careers scraper`);
if (useStartUrl) {
    log.info(`📍 Using start URL: ${startUrl}`);
} else {
    log.info(`🔍 Searching for: "${keyword}" ${location ? `in ${location}` : ''}`);
}
log.info(`📊 Target: ${RESULTS_WANTED} jobs, Max pages: ${max_pages}`);

// ------------------------- HELPERS -------------------------

/**
 * Build Google Careers search URL
 */
const buildCareersSearchUrl = (keyword, location, dateFilter, page = 1) => {
    const baseUrl = 'https://www.google.com/about/careers/applications/jobs/results';
    const params = new URLSearchParams();
    
    if (keyword) {
        params.set('q', keyword);
    }
    
    if (location) {
        params.set('location', location);
    }
    
    // Date filter mapping for Google Careers
    if (dateFilter && dateFilter !== 'anytime') {
        const dateFilters = {
            'today': '1',
            'last3days': '3',
            'last7days': '7',
            'last14days': '14'
        };
        if (dateFilters[dateFilter]) {
            params.set('posted_date', dateFilters[dateFilter]);
        }
    }
    
    if (page > 1) {
        params.set('page', page.toString());
    }
    
    const url = params.toString() ? `${baseUrl}?${params.toString()}` : baseUrl;
    return url;
};

/**
 * Simple user agent rotation
 */
const getRandomUserAgent = () => {
    const userAgents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ];
    return userAgents[Math.floor(Math.random() * userAgents.length)];
};

/**
 * Extract JSON-LD data from HTML
 */
const extractJsonLd = ($, crawlerLog) => {
    const jsonLdJobs = [];
    
    $('script[type="application/ld+json"]').each((i, elem) => {
        try {
            const content = $(elem).html();
            if (!content) return;
            
            const data = JSON.parse(content);
            
            // Handle single JobPosting
            if (data['@type'] === 'JobPosting') {
                jsonLdJobs.push(data);
            }
            // Handle array of JobPostings
            else if (Array.isArray(data)) {
                const jobPostings = data.filter(item => item['@type'] === 'JobPosting');
                jsonLdJobs.push(...jobPostings);
            }
            // Handle nested structure
            else if (data['@graph']) {
                const jobPostings = data['@graph'].filter(item => item['@type'] === 'JobPosting');
                jsonLdJobs.push(...jobPostings);
            }
        } catch (e) {
            // Invalid JSON, skip
            crawlerLog.debug(`Failed to parse JSON-LD: ${e.message}`);
        }
    });
    
    return jsonLdJobs;
};

/**
 * Convert JSON-LD JobPosting to our format
 */
const parseJsonLdJob = (jobData) => {
    const location = jobData.jobLocation?.address?.addressLocality || 
                    jobData.jobLocation?.address?.addressRegion ||
                    (Array.isArray(jobData.jobLocation) ? jobData.jobLocation[0]?.address?.addressLocality : null) ||
                    'Not specified';
    
    const company = jobData.hiringOrganization?.name || 'Google';
    
    return {
        id: jobData.identifier?.value || 
            jobData.identifier?.name ||
            `job_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        title: jobData.title || jobData.name || 'Untitled',
        company: company,
        location: location,
        description_text: jobData.description ? jobData.description.substring(0, 500) : '',
        description_full: jobData.description || '',
        employment_type: Array.isArray(jobData.employmentType) 
            ? jobData.employmentType.join(', ') 
            : (jobData.employmentType || 'Not specified'),
        date_posted: jobData.datePosted || null,
        valid_through: jobData.validThrough || null,
        url: jobData.url || jobData.sameAs || null,
        salary: jobData.baseSalary?.value?.value || 
                jobData.baseSalary?.value?.minValue ||
                (jobData.baseSalary?.currency && jobData.baseSalary?.value 
                    ? `${jobData.baseSalary.currency} ${jobData.baseSalary.value}` 
                    : null),
        responsibilities: jobData.responsibilities || null,
        qualifications: jobData.qualifications || null,
        education_requirements: jobData.educationRequirements?.credentialCategory || 
                                jobData.educationRequirements || null,
        experience_requirements: jobData.experienceRequirements?.monthsOfExperience || 
                                jobData.experienceRequirements || null,
        skills_required: jobData.skills || null,
        industry: jobData.industry || null,
        occupational_category: jobData.occupationalCategory || null,
        work_hours: jobData.workHours || null,
        _extractedFrom: 'json-ld'
    };
};

/**
 * Extract jobs from DOM (fallback)
 */
const extractJobsFromDom = ($, crawlerLog) => {
    const jobs = [];
    
    // Try multiple selector strategies
    const selectors = [
        '[data-job-id]',
        'li[role="listitem"]',
        '.VfPpkd-rymPhb',
        '[class*="job-"]',
        'div[class*="result"]',
        'article',
    ];
    
    let jobElements = $();
    for (const selector of selectors) {
        const elements = $(selector);
        if (elements.length > 0) {
            crawlerLog.info(`✓ Found ${elements.length} elements with selector: ${selector}`);
            jobElements = elements;
            break;
        }
    }
    
    if (jobElements.length === 0) {
        crawlerLog.warning('No job elements found with any selector');
        return jobs;
    }
    
    jobElements.each((index, element) => {
        const $element = $(element);
        
        // Extract title
        const titleEl = $element.find('h3, h2, h4, [class*="title"], a[class*="job"]').first();
        const title = titleEl.text().trim();
        
        if (!title || title.length < 3) return; // Skip invalid entries
        
        // Extract company
        const companyEl = $element.find('[class*="company"], [class*="organization"], [class*="employer"]').first();
        const company = companyEl.text().trim() || 'Google';
        
        // Extract location
        const locationEl = $element.find('[class*="location"], [class*="office"], [class*="place"]').first();
        const location = locationEl.text().trim() || 'Not specified';
        
        // Extract description
        const descEl = $element.find('p, [class*="description"], [class*="snippet"]').first();
        const description = descEl.text().trim();
        
        // Extract URL
        const linkEl = $element.find('a[href]').first();
        const url = linkEl.attr('href') || null;
        const fullUrl = url && url.startsWith('http') ? url : (url ? `https://www.google.com${url}` : null);
        
        // Extract job ID
        const jobId = $element.attr('data-job-id') || 
                     (fullUrl ? fullUrl.split('/').pop().split('?')[0] : null) ||
                     `job_${Date.now()}_${index}`;
        
        // Extract employment type if available
        const employmentTypeEl = $element.find('[class*="type"], [class*="employment"]').first();
        const employmentType = employmentTypeEl.text().trim() || 'Not specified';
        
        // Extract date posted if available
        const dateEl = $element.find('[class*="date"], [class*="posted"], time').first();
        const datePosted = dateEl.attr('datetime') || dateEl.text().trim() || null;
        
        jobs.push({
            id: jobId,
            title: title,
            company: company,
            location: location,
            description_text: description.substring(0, 500),
            description_full: description,
            employment_type: employmentType,
            date_posted: datePosted,
            url: fullUrl,
            salary: null,
            responsibilities: null,
            qualifications: null,
            _extractedFrom: 'dom'
        });
    });
    
    return jobs;
};

/**
 * Fetch and extract job details from detail page
 */
const fetchJobDetails = async (jobUrl, proxyUrl, crawlerLog) => {
    try {
        crawlerLog.info(`🔍 Fetching job details from: ${jobUrl}`);
        
        const response = await gotScraping({
            url: jobUrl,
            proxyUrl: proxyUrl,
            headers: {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'User-Agent': getRandomUserAgent(),
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
            },
            timeout: {
                request: 30000,
            },
            retry: {
                limit: 2,
            },
        });
        
        const html = response.body;
        const $ = (await import('cheerio')).load(html);
        
        // Try JSON-LD first
        const jsonLdJobs = extractJsonLd($, crawlerLog);
        if (jsonLdJobs.length > 0) {
            const jobData = parseJsonLdJob(jsonLdJobs[0]);
            crawlerLog.info(`✓ Extracted detailed job info from JSON-LD`);
            return {
                description_full: jobData.description_full,
                salary: jobData.salary,
                responsibilities: jobData.responsibilities,
                qualifications: jobData.qualifications,
                education_requirements: jobData.education_requirements,
                experience_requirements: jobData.experience_requirements,
                skills_required: jobData.skills_required,
                industry: jobData.industry,
                occupational_category: jobData.occupational_category,
                work_hours: jobData.work_hours,
            };
        }
        
        // Fallback to DOM extraction
        const descriptionEl = $('[class*="description"], [role="article"], main, .content').first();
        const description_full = descriptionEl.text().trim();
        
        const salaryEl = $('[class*="salary"], [class*="compensation"], [class*="pay"]').first();
        const salary = salaryEl.text().trim() || null;
        
        const qualificationsEl = $('[class*="qualification"], [class*="requirement"]').first();
        const qualifications = qualificationsEl.text().trim() || null;
        
        crawlerLog.info(`✓ Extracted detailed job info from DOM`);
        return {
            description_full: description_full.substring(0, 5000),
            salary: salary,
            responsibilities: null,
            qualifications: qualifications,
            education_requirements: null,
            experience_requirements: null,
            skills_required: null,
        };
        
    } catch (error) {
        crawlerLog.warning(`Failed to fetch job details: ${error.message}`);
        return null;
    }
};

/**
 * Find pagination links in the page
 */
const findNextPageUrl = ($, currentUrl, crawlerLog) => {
    // Try to find next page link
    const nextSelectors = [
        'a[aria-label*="Next"]',
        'a[aria-label*="next"]',
        'a.next',
        'a[rel="next"]',
        'a:contains("Next")',
        'a:contains("›")',
        'button[aria-label*="Next"]',
        'button[aria-label*="next"]',
    ];
    
    for (const selector of nextSelectors) {
        const nextLink = $(selector).first();
        if (nextLink.length > 0 && !nextLink.attr('disabled')) {
            const href = nextLink.attr('href');
            if (href) {
                const nextUrl = href.startsWith('http') ? href : new URL(href, currentUrl).href;
                crawlerLog.info(`✓ Found next page via selector ${selector}: ${nextUrl}`);
                return nextUrl;
            }
        }
    }
    
    // Try to find pagination by page number
    const currentPageMatch = currentUrl.match(/[?&]page=(\d+)/);
    const currentPage = currentPageMatch ? parseInt(currentPageMatch[1]) : 1;
    
    // Build next page URL
    const url = new URL(currentUrl);
    url.searchParams.set('page', (currentPage + 1).toString());
    const nextUrl = url.toString();
    
    crawlerLog.info(`✓ Built next page URL: page ${currentPage + 1}`);
    return nextUrl;
};

// ------------------------- STATE -------------------------
let jobsCollected = 0;
let pagesVisited = 0;
const processedIds = new Set();
const detailPageQueue = [];

// ------------------------- PROXY -------------------------
const proxyConf = proxyConfiguration
    ? await Actor.createProxyConfiguration(proxyConfiguration)
    : undefined;

// ------------------------- CRAWLER -------------------------
const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConf,
    maxRequestsPerMinute: 20,
    requestHandlerTimeoutSecs: 60,
    navigationTimeoutSecs: 60,
    maxConcurrency: 3,
    useSessionPool: true,
    persistCookiesPerSession: true,
    maxRequestRetries: maxRequestRetries,

    preNavigationHooks: [async ({ request }) => {
        // Random delay to appear more human-like
        const delay = requestDelay + Math.random() * 2000;
        await new Promise(resolve => setTimeout(resolve, delay));
        
        // Set realistic headers
        request.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'User-Agent': getRandomUserAgent(),
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        };
    }],

    async requestHandler({ $, request, log: crawlerLog, session }) {
        if (jobsCollected >= RESULTS_WANTED || pagesVisited >= max_pages) {
            crawlerLog.info('Target reached, stopping.');
            return;
        }

        const isDetailPage = request.userData?.isDetailPage || false;
        
        if (isDetailPage) {
            // This is a job detail page - we shouldn't reach here in HTTP mode
            // Details are fetched separately using gotScraping
            return;
        }

        pagesVisited++;
        crawlerLog.info(`📄 Processing page ${pagesVisited}/${max_pages}: ${request.loadedUrl}`);
        crawlerLog.info(`📄 Page title: "${$('title').text()}"`);
        
        // Check for anti-bot challenges
        const pageText = $('body').text().toLowerCase();
        if (pageText.includes('unusual traffic') || 
            pageText.includes('captcha') || 
            pageText.includes('robot') ||
            pageText.includes('automated')) {
            crawlerLog.warning('⚠️ Anti-bot challenge detected');
            if (session) session.retire();
            throw new Error('CHALLENGE_DETECTED');
        }
        
        // Log basic page info
        crawlerLog.info(`📊 Page stats: ${$('div').length} divs, ${$('a').length} links, ${$('script').length} scripts`);
        
        // Extract jobs - try JSON-LD first
        let jobs = [];
        const jsonLdJobs = extractJsonLd($, crawlerLog);
        
        if (jsonLdJobs.length > 0) {
            crawlerLog.info(`✓ Found ${jsonLdJobs.length} jobs in JSON-LD`);
            jobs = jsonLdJobs.map(parseJsonLdJob);
        } else {
            crawlerLog.info('No JSON-LD found, trying DOM extraction...');
            jobs = extractJobsFromDom($, crawlerLog);
        }
        
        crawlerLog.info(`📋 Extracted ${jobs.length} jobs from page`);
        
        // Process each job
        for (const job of jobs) {
            if (jobsCollected >= RESULTS_WANTED) break;
            
            // Add metadata
            job.source = 'google.com/careers';
            job._fetchedAt = new Date().toISOString();
            job._scrapedUrl = request.loadedUrl;
            job._searchKeyword = keyword || null;
            job._searchLocation = location || null;
            job._postedDateFilter = posted_date;
            
            // Check if we should fetch details
            if (collectDetails && job.url && job.url.startsWith('http') && job._extractedFrom !== 'json-ld') {
                // Get proxy URL for gotScraping
                const proxyUrl = proxyConf ? await proxyConf.newUrl(session?.id) : undefined;
                
                // Fetch job details
                const details = await fetchJobDetails(job.url, proxyUrl, crawlerLog);
                if (details) {
                    Object.assign(job, details);
                }
                
                // Small delay between detail fetches
                await new Promise(resolve => setTimeout(resolve, 1000 + Math.random() * 1000));
            }
            
            // Save job
            if (!processedIds.has(job.id)) {
                await Dataset.pushData(job);
                jobsCollected++;
                processedIds.add(job.id);
                crawlerLog.info(`✅ Job ${jobsCollected}/${RESULTS_WANTED} saved: "${job.title}" at ${job.company}`);
            } else {
                crawlerLog.debug(`⏭️ Skipping duplicate job: ${job.id}`);
            }
        }
        
        // Handle pagination
        if (jobsCollected < RESULTS_WANTED && pagesVisited < max_pages && jobs.length > 0) {
            const nextPageUrl = findNextPageUrl($, request.loadedUrl, crawlerLog);
            
            if (nextPageUrl && nextPageUrl !== request.loadedUrl) {
                await crawler.addRequests([{ url: nextPageUrl }]);
                crawlerLog.info(`📄 Added next page to queue: ${nextPageUrl}`);
            } else {
                crawlerLog.info('📚 No more pages found or reached the end');
            }
        } else if (jobs.length === 0) {
            crawlerLog.warning('⚠️ No jobs found on this page, stopping pagination');
        }
    },

    async failedRequestHandler({ request, log: crawlerLog, session }, error) {
        crawlerLog.error(`❌ Request failed: ${request.url} - ${error.message}`);
        
        if (error.message.includes('CHALLENGE_DETECTED')) {
            crawlerLog.warning('🤖 Anti-bot challenge detected, waiting before retry...');
            await new Promise(resolve => setTimeout(resolve, 15000));
            if (session) session.retire();
        }
    },
});

// ------------------------- RUN -------------------------
const initialUrl = useStartUrl ? startUrl : buildCareersSearchUrl(keyword, location, posted_date);
log.info(`🎯 Starting URL: ${initialUrl}`);

log.info(`Input parameters:`);
if (useStartUrl) {
    log.info(`  - Start URL: "${startUrl}"`);
} else {
    log.info(`  - Keyword: "${keyword}"`);
    log.info(`  - Location: "${location || 'Not specified'}"`);
    log.info(`  - Posted date filter: "${posted_date}"`);
}
log.info(`  - Results wanted: ${RESULTS_WANTED}`);
log.info(`  - Max pages: ${max_pages}`);
log.info(`  - Collect details: ${collectDetails}`);
log.info(`  - Max retries: ${maxRequestRetries}`);
log.info(`  - Request delay: ${requestDelay}ms`);
log.info(`  - Using proxy: ${proxyConf ? 'Yes' : 'No'}`);

try {
    await crawler.run([{ url: initialUrl }]);
    
    log.info(`✅ Scraping completed successfully!`);
    log.info(`📊 Final Results:`);
    log.info(`  - Total jobs collected: ${jobsCollected}/${RESULTS_WANTED}`);
    log.info(`  - Unique jobs processed: ${processedIds.size}`);
    log.info(`  - Pages visited: ${pagesVisited}/${max_pages}`);
    if (!useStartUrl) {
        log.info(`  - Search keyword: "${keyword}"`);
        log.info(`  - Search location: "${location || 'Not specified'}"`);
    }
    
    if (jobsCollected === 0) {
        log.warning(`⚠️ No jobs were collected. This might be due to:`);
        log.warning(`   1. Google's anti-bot measures blocking requests`);
        log.warning(`   2. No jobs matching the search criteria`);
        log.warning(`   3. Changes in Google Careers page structure`);
        log.warning(`   4. The page requiring JavaScript rendering (consider using browser-based scraping)`);
        log.warning(`   5. Proxy or network issues`);
        log.warning(`   Please check the logs above for more details.`);
    }
    
} catch (error) {
    log.error(`❌ Scraping failed with error: ${error.message}`);
    throw error;
} finally {
    await Actor.exit();
}