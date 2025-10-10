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
 * Build Google Jobs search URL
 */
const buildCareersSearchUrl = (keyword, location, dateFilter, start = 0) => {
    const baseUrl = 'https://www.google.com/search';
    const params = new URLSearchParams();
    
    // Build the search query
    let searchQuery = keyword || 'jobs';
    if (location) {
        searchQuery += ` ${location}`;
    }
    
    params.set('q', searchQuery);
    params.set('ibp', 'htl;jobs'); // Critical: This tells Google to show job listings
    params.set('hl', 'en'); // Language
    params.set('gl', 'us'); // Geolocation
    
    // Date filter mapping for Google Jobs
    if (dateFilter && dateFilter !== 'anytime') {
        const dateFilters = {
            'today': 'today',
            'last3days': '3days',
            '3d': '3days',
            'last7days': 'week',
            '7d': 'week',
            'week': 'week',
            'last14days': 'month',
            '14d': 'month',
            'month': 'month'
        };
        const mappedFilter = dateFilters[dateFilter.toLowerCase()] || dateFilter;
        params.set('chips', `date_posted:${mappedFilter}`);
    }
    
    // Pagination (Google Jobs uses 'start' parameter, typically increments by 10)
    if (start > 0) {
        params.set('start', start.toString());
    }
    
    const url = `${baseUrl}?${params.toString()}`;
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
            if (!content || content.trim().length === 0) return;
            
            const data = JSON.parse(content);
            
            // Handle single JobPosting
            if (data['@type'] === 'JobPosting') {
                jsonLdJobs.push(data);
                crawlerLog.debug(`Found JobPosting in JSON-LD script ${i}`);
            }
            // Handle array of JobPostings
            else if (Array.isArray(data)) {
                const jobPostings = data.filter(item => item && item['@type'] === 'JobPosting');
                if (jobPostings.length > 0) {
                    crawlerLog.debug(`Found ${jobPostings.length} JobPostings in array at script ${i}`);
                    jsonLdJobs.push(...jobPostings);
                }
            }
            // Handle nested structure with @graph
            else if (data['@graph'] && Array.isArray(data['@graph'])) {
                const jobPostings = data['@graph'].filter(item => item && item['@type'] === 'JobPosting');
                if (jobPostings.length > 0) {
                    crawlerLog.debug(`Found ${jobPostings.length} JobPostings in @graph at script ${i}`);
                    jsonLdJobs.push(...jobPostings);
                }
            }
            // Handle ItemList containing JobPostings
            else if (data['@type'] === 'ItemList' && data.itemListElement) {
                const items = Array.isArray(data.itemListElement) ? data.itemListElement : [data.itemListElement];
                items.forEach(item => {
                    if (item.item && item.item['@type'] === 'JobPosting') {
                        jsonLdJobs.push(item.item);
                        crawlerLog.debug(`Found JobPosting in ItemList at script ${i}`);
                    } else if (item['@type'] === 'JobPosting') {
                        jsonLdJobs.push(item);
                        crawlerLog.debug(`Found JobPosting in ItemList at script ${i}`);
                    }
                });
            }
        } catch (e) {
            // Invalid JSON, skip
            crawlerLog.debug(`Failed to parse JSON-LD script ${i}: ${e.message}`);
        }
    });
    
    if (jsonLdJobs.length > 0) {
        crawlerLog.info(`✓ Successfully extracted ${jsonLdJobs.length} jobs from JSON-LD`);
    }
    
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
    
    // Google Jobs specific selectors (updated for current Google Jobs markup)
    const selectors = [
        'li.iFjolb',  // Primary Google Jobs listing container
        'div.PwjeAc',  // Alternative job card container
        'li[data-ved]',  // Job listings with data-ved attribute
        'div[jsname][data-job-id]',  // Jobs with data-job-id
        '[role="listitem"]',  // Generic ARIA listitem
        'div.jobCard',  // Job card class
        '.job-listing',  // Generic job listing class
    ];
    
    let jobElements = $();
    let usedSelector = null;
    
    for (const selector of selectors) {
        const elements = $(selector);
        if (elements.length > 0) {
            crawlerLog.info(`✓ Found ${elements.length} elements with selector: ${selector}`);
            jobElements = elements;
            usedSelector = selector;
            break;
        }
    }
    
    if (jobElements.length === 0) {
        crawlerLog.warning('No job elements found with any selector');
        
        // Log sample HTML to help debug
        const bodySnippet = $('body').html()?.substring(0, 1000) || '';
        crawlerLog.debug(`Body HTML snippet (first 1000 chars): ${bodySnippet}`);
        
        // Try to find any div with job-related classes
        const anyJobDivs = $('div[class*="job"], li[class*="job"], div[id*="job"]');
        if (anyJobDivs.length > 0) {
            crawlerLog.info(`Found ${anyJobDivs.length} elements with 'job' in class/id`);
            anyJobDivs.slice(0, 3).each((i, el) => {
                const classes = $(el).attr('class') || '';
                const id = $(el).attr('id') || '';
                crawlerLog.debug(`  Sample ${i+1}: class="${classes}", id="${id}"`);
            });
        }
        
        return jobs;
    }
    
    crawlerLog.info(`Processing ${jobElements.length} job elements with selector: ${usedSelector}`);
    
    jobElements.each((index, element) => {
        const $element = $(element);
        
        // Extract title - Google Jobs typically uses specific heading structure
        let title = '';
        const titleSelectors = [
            'div[role="heading"]',  // ARIA heading
            'h2', 'h3', 'h4',  // Standard headings
            '.BjJfJf',  // Google Jobs title class (may change)
            'div.job-title',
            'a[class*="title"]',
            'span[class*="title"]',
        ];
        
        for (const sel of titleSelectors) {
            const titleEl = $element.find(sel).first();
            if (titleEl.length > 0) {
                title = titleEl.text().trim();
                if (title && title.length >= 3) break;
            }
        }
        
        if (!title || title.length < 3) {
            crawlerLog.debug(`Skipping element ${index}: no valid title found`);
            return; // Skip invalid entries
        }
        
        // Extract company
        let company = 'Not specified';
        const companySelectors = [
            'div.vNEEBe',  // Google Jobs company class (may change)
            'div[class*="company"]',
            'span[class*="company"]',
            'div[class*="employer"]',
            'span[class*="organization"]',
        ];
        
        for (const sel of companySelectors) {
            const companyEl = $element.find(sel).first();
            if (companyEl.length > 0) {
                company = companyEl.text().trim();
                if (company && company.length > 0) break;
            }
        }
        
        // Extract location
        let location = 'Not specified';
        const locationSelectors = [
            'div.Qk80Jf',  // Google Jobs location class (may change)
            'span[class*="location"]',
            'div[class*="location"]',
            'span[class*="place"]',
            'div[class*="office"]',
        ];
        
        for (const sel of locationSelectors) {
            const locationEl = $element.find(sel).first();
            if (locationEl.length > 0) {
                location = locationEl.text().trim();
                if (location && location.length > 0) break;
            }
        }
        
        // Extract description
        let description = '';
        const descSelectors = [
            'div[class*="description"]',
            'div[class*="snippet"]',
            'div[class*="summary"]',
            'p',
            'span[class*="desc"]',
        ];
        
        for (const sel of descSelectors) {
            const descEl = $element.find(sel).first();
            if (descEl.length > 0) {
                description = descEl.text().trim();
                if (description && description.length > 10) break;
            }
        }
        
        // Extract URL
        let fullUrl = null;
        const linkEl = $element.find('a[href]').first();
        const url = linkEl.attr('href');
        
        if (url) {
            if (url.startsWith('http')) {
                fullUrl = url;
            } else if (url.startsWith('/')) {
                fullUrl = `https://www.google.com${url}`;
            } else {
                fullUrl = `https://www.google.com/${url}`;
            }
        }
        
        // Extract job ID from data attribute or URL
        let jobId = $element.attr('data-job-id') || 
                   $element.attr('data-ved') ||
                   $element.attr('id');
        
        if (!jobId && fullUrl) {
            // Try to extract from URL
            const urlMatch = fullUrl.match(/jobs\/([^/?]+)/);
            jobId = urlMatch ? urlMatch[1] : null;
        }
        
        if (!jobId) {
            jobId = `job_${Date.now()}_${index}_${Math.random().toString(36).substr(2, 9)}`;
        }
        
        // Extract employment type if available
        let employmentType = 'Not specified';
        const employmentTypeEl = $element.find('span[class*="type"], div[class*="employment"]').first();
        if (employmentTypeEl.length > 0) {
            employmentType = employmentTypeEl.text().trim();
        }
        
        // Extract date posted if available
        let datePosted = null;
        const dateSelectors = ['time', 'span[class*="date"]', 'div[class*="posted"]', 'span[class*="ago"]'];
        for (const sel of dateSelectors) {
            const dateEl = $element.find(sel).first();
            if (dateEl.length > 0) {
                datePosted = dateEl.attr('datetime') || dateEl.text().trim();
                if (datePosted) break;
            }
        }
        
        // Extract salary if available
        let salary = null;
        const salarySelectors = ['span[class*="salary"]', 'div[class*="compensation"]', 'span[class*="pay"]'];
        for (const sel of salarySelectors) {
            const salaryEl = $element.find(sel).first();
            if (salaryEl.length > 0) {
                salary = salaryEl.text().trim();
                if (salary) break;
            }
        }
        
        const job = {
            id: jobId,
            title: title,
            company: company,
            location: location,
            description_text: description.substring(0, 500),
            description_full: description,
            employment_type: employmentType,
            date_posted: datePosted,
            url: fullUrl,
            salary: salary,
            responsibilities: null,
            qualifications: null,
            _extractedFrom: 'dom'
        };
        
        jobs.push(job);
        crawlerLog.debug(`Extracted job ${index + 1}: "${title}" at ${company}`);
    });
    
    crawlerLog.info(`✓ Successfully extracted ${jobs.length} jobs from DOM`);
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
        'a#pnnext',  // Google's standard "Next" button ID
        'a.nBDE1b',  // Google Jobs next button class
        'a.next',
        'a[rel="next"]',
        'td.b a',  // Google pagination table cell
        'a:contains("Next")',
        'a:contains("›")',
        'button[aria-label*="Next"]',
        'button[aria-label*="next"]',
    ];
    
    for (const selector of nextSelectors) {
        const nextLink = $(selector).first();
        if (nextLink.length > 0 && !nextLink.attr('disabled') && !nextLink.hasClass('disabled')) {
            const href = nextLink.attr('href');
            if (href) {
                const nextUrl = href.startsWith('http') ? href : new URL(href, currentUrl).href;
                crawlerLog.info(`✓ Found next page via selector ${selector}: ${nextUrl}`);
                return nextUrl;
            }
        }
    }
    
    // For Google Jobs, pagination uses 'start' parameter (increments by 10)
    const url = new URL(currentUrl);
    const currentStart = parseInt(url.searchParams.get('start') || '0');
    const nextStart = currentStart + 10;
    
    // Build next page URL
    url.searchParams.set('start', nextStart.toString());
    const nextUrl = url.toString();
    
    crawlerLog.info(`✓ Built next page URL: start=${nextStart}`);
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
        
        // Log JSON-LD scripts count
        const jsonLdScripts = $('script[type="application/ld+json"]');
        crawlerLog.info(`📊 Found ${jsonLdScripts.length} JSON-LD script(s)`);
        
        // Extract jobs - try JSON-LD first
        let jobs = [];
        const jsonLdJobs = extractJsonLd($, crawlerLog);
        
        if (jsonLdJobs.length > 0) {
            crawlerLog.info(`✓ Found ${jsonLdJobs.length} jobs in JSON-LD`);
            jobs = jsonLdJobs.map(parseJsonLdJob);
        } else {
            crawlerLog.info('No JSON-LD jobs found, trying DOM extraction...');
            jobs = extractJobsFromDom($, crawlerLog);
        }
        
        crawlerLog.info(`📋 Extracted ${jobs.length} jobs from page`);
        
        // If no jobs found, log debugging information
        if (jobs.length === 0) {
            crawlerLog.warning('⚠️ No jobs extracted from this page');
            crawlerLog.info('Debugging information:');
            
            // Check for common Google Jobs elements
            const liCount = $('li').length;
            const liWithData = $('li[data-ved]').length;
            const jobCardDivs = $('div[class*="job"]').length;
            
            crawlerLog.info(`  - Total <li> elements: ${liCount}`);
            crawlerLog.info(`  - <li> with data-ved: ${liWithData}`);
            crawlerLog.info(`  - Divs with 'job' in class: ${jobCardDivs}`);
            
            // Log a sample of classes on the page
            const sampleClasses = new Set();
            $('div[class], li[class]').slice(0, 50).each((i, el) => {
                const classes = $(el).attr('class') || '';
                classes.split(' ').forEach(cls => {
                    if (cls.length > 0) sampleClasses.add(cls);
                });
            });
            
            crawlerLog.info(`  - Sample classes found: ${Array.from(sampleClasses).slice(0, 20).join(', ')}`);
            
            // Save HTML for debugging (first 5000 chars)
            const htmlSnippet = $('body').html()?.substring(0, 5000) || '';
            crawlerLog.debug(`HTML snippet: ${htmlSnippet}`);
        }
        
        // Process each job
        for (const job of jobs) {
            if (jobsCollected >= RESULTS_WANTED) break;
            
            // Add metadata
            job.source = 'Google Jobs';
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