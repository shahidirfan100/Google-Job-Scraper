
// Google Jobs scraper (CheerioCrawler)
// Runtime: Node 22, ESM ("type": "module")
// Uses apify@^3 and crawlee@^3

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

await Actor.init();

// ------------------------- INPUT -------------------------
const input = await Actor.getInput() ?? {};
const {
    keyword = '',
    location = '',
    posted_date = 'anytime',
    results_wanted: RESULTS_WANTED_RAW = 100,
    proxyConfiguration,
    maxRequestRetries = 3,
    requestDelay = 2000,
} = input;

if (!keyword) {
    throw new Error('Input "keyword" is required.');
}

const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;

// ------------------------- HELPERS -------------------------

/**
 * Constructs multiple Google Jobs search URL variants for better success rate.
 * @param {string} kw - Keyword.
 * @param {string} loc - Location.
 * @param {'anytime'|'24h'|'7d'|'30d'} date - Posted date filter.
 * @param {number} start - Starting position for pagination.
 * @returns {string[]} Array of URLs to try
 */
const buildStartUrls = (kw, loc, date, start = 0) => {
    const urls = [];
    
    // URL Variant 1: Standard Google Jobs search
    const searchParams1 = new URLSearchParams();
    const queryParts1 = [];
    if (kw) queryParts1.push(kw);
    if (loc) queryParts1.push(`in ${loc}`);
    queryParts1.push('jobs');
    
    searchParams1.set('q', queryParts1.join(' '));
    searchParams1.set('ibp', 'htl;jobs');
    searchParams1.set('sa', 'X');
    searchParams1.set('ved', '0ahUKEwjX');
    
    if (start > 0) searchParams1.set('start', start.toString());
    if (date && date !== 'anytime') {
        const dateMap = {
            '24h': 'date_posted:r_86400',
            '7d': 'date_posted:r_604800', 
            '30d': 'date_posted:r_2592000',
        };
        if (dateMap[date]) {
            searchParams1.set('chips', dateMap[date]);
        }
    }
    
    searchParams1.set('sourceid', 'chrome');
    searchParams1.set('hl', 'en');
    searchParams1.set('gl', 'us');
    urls.push(`https://www.google.com/search?${searchParams1.toString()}`);
    
    // URL Variant 2: Alternative job search format
    const searchParams2 = new URLSearchParams();
    searchParams2.set('q', `${kw} ${loc ? `"${loc}"` : ''} job openings`.trim());
    searchParams2.set('ibp', 'htl;jobs');
    if (start > 0) searchParams2.set('start', start.toString());
    urls.push(`https://www.google.com/search?${searchParams2.toString()}`);
    
    // URL Variant 3: Direct jobs.google.com approach (if available)
    if (start === 0) { // Only for first page
        const jobsParams = new URLSearchParams();
        jobsParams.set('q', kw);
        if (loc) jobsParams.set('l', loc);
        if (date && date !== 'anytime') {
            const dateMap = { '24h': 1, '7d': 7, '30d': 30 };
            if (dateMap[date]) jobsParams.set('date', dateMap[date]);
        }
        urls.push(`https://jobs.google.com/search?${jobsParams.toString()}`);
    }
    
    // URL Variant 4: Simple Google search with job site operators
    const searchParams4 = new URLSearchParams();
    const siteOperators = ['site:linkedin.com/jobs', 'site:indeed.com', 'site:glassdoor.com', 'site:monster.com'];
    const operatorQuery = `${kw} ${loc ? `"${loc}"` : ''} (${siteOperators.join(' OR ')})`;
    searchParams4.set('q', operatorQuery);
    if (start > 0) searchParams4.set('start', start.toString());
    urls.push(`https://www.google.com/search?${searchParams4.toString()}`);
    
    return urls;
};

/**
 * Legacy function for backward compatibility
 */
const buildStartUrl = (kw, loc, date, start = 0) => {
    return buildStartUrls(kw, loc, date, start)[0];
};

/**
 * Generates randomized user agents to avoid detection
 * @returns {string}
 */
const getRandomUserAgent = () => {
    const userAgents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ];
    return userAgents[Math.floor(Math.random() * userAgents.length)];
};

/**
 * Adds random delay to mimic human behavior
 * @returns {Promise<void>}
 */
const randomDelay = async () => {
    const delay = Math.random() * 3000 + 1000; // 1-4 seconds
    await new Promise(resolve => setTimeout(resolve, delay));
};

/**
 * Extracts text and sanitizes it.
 * @param {import('cheerio').CheerioAPI} $ - Cheerio instance.
 * @param {import('cheerio').Cheerio<import('cheerio').Element>} el - Cheerio element.
 * @returns {string|null}
 */
const getText = ($, el) => {
    return el?.text().trim().replace(/\s+/g, ' ') || null;
};

/**
 * Converts HTML to clean plain text.
 * @param {string} html
 * @returns {string}
 */
const htmlToText = (html) => (html || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/(p|div|li|h\d)>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

// ------------------------- STATE -------------------------
let jobsCollected = 0;
const processedIds = new Set();

// ------------------------- PROXY -------------------------
const proxyConf = proxyConfiguration
    ? await Actor.createProxyConfiguration(proxyConfiguration)
    : undefined;

// ------------------------- CRAWLER -------------------------
const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConf,
    maxRequestsPerMinute: 30, // Further reduced to avoid detection
    requestHandlerTimeoutSecs: 120, // Increased timeout for complex pages
    navigationTimeoutSecs: 150, // Increased navigation timeout
    maxConcurrency: 3, // Reduced concurrency for better stealth
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: {
        maxPoolSize: 20,
        sessionOptions: {
            maxUsageCount: 10, // Rotate sessions frequently
            maxErrorScore: 3,
        },
    },
    maxRequestRetries: maxRequestRetries,
    
    // Additional gotScraping options for better anti-detection
    additionalMimeTypes: ['application/json', 'text/plain'],
    suggestResponseEncoding: 'utf8',
    ignoreSslErrors: false,

    preNavigationHooks: [async ({ request, session }) => {
        // Add random delay before each request to mimic human behavior
        const delay = Math.random() * 3000 + 2000; // 2-5 seconds
        await new Promise(resolve => setTimeout(resolve, delay));
        
        const userAgent = getRandomUserAgent();
        
        // Enhanced headers to better mimic real browser requests
        request.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Arch': '"x86"',
            'Sec-Ch-Ua-Bitness': '"64"',
            'Sec-Ch-Ua-Full-Version-List': '"Not_A Brand";v="8.0.0.0", "Chromium";v="120.0.6099.109", "Google Chrome";v="120.0.6099.109"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Model': '""',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Ch-Ua-Platform-Version': '"15.0.0"',
            'Sec-Ch-Ua-Wow64': '?0',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': request.userData.label === 'DETAIL' ? 'same-origin' : 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': userAgent,
            'X-Client-Data': 'CIu2yQEIo7bJAQipncoBCMDdygEIk6HLAQiFoM0BCNy9zQEIuuHNAQi5ys0BCIrTzQEI2dPNAQjpncoBCKegzQEI66DNAQj0oM0BCPuCzgEIm4LOAQjakM4BCKSQzgEI1JDOAQjWkM4B',
            ...request.headers,
        };
        
        // Add some cookies to look more legitimate
        if (!request.headers.Cookie) {
            const cookies = [
                'CONSENT=YES+cb.20220301-17-p0.en+FX+',
                `SOCS=CAISHAgBEhJnd3NfMjAyNDA4MTMtMF9SQzIaAmVuIAEaBgiAo-GwBg`,
                `NID=${Math.random().toString(36).substring(2, 15)}`,
                `1P_JAR=${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}-${String(new Date().getDate()).padStart(2, '0')}-${String(new Date().getHours()).padStart(2, '0')}`,
            ];
            request.headers.Cookie = cookies.join('; ');
        }
    }],

    async requestHandler({ $, request, log: crawlerLog, enqueueLinks, session, body }) {
        if (jobsCollected >= RESULTS_WANTED) {
            crawlerLog.info('Target number of jobs reached, skipping further requests.');
            return;
        }

        crawlerLog.info(`Processing page: ${request.loadedUrl}`);
        crawlerLog.info(`Page title: ${$('title').text()}`);
        crawlerLog.info(`Body length: ${body ? body.length : 'N/A'}`);

        // Enhanced anti-bot detection and handling
        const challengeIndicators = [
            'form[action*="sorry"]',
            'div:contains("unusual traffic")',
            'div:contains("automated queries")',
            'div:contains("robot")',
            'div:contains("captcha")',
            '#captcha',
            '.g-recaptcha',
            'div:contains("verify")',
            'div:contains("suspicious")',
            'form[action*="sorry.google.com"]',
            'div:contains("blocked")',
        ];
        
        let challengeDetected = false;
        for (const selector of challengeIndicators) {
            if ($(selector).length > 0) {
                challengeDetected = true;
                crawlerLog.warning(`Anti-bot challenge detected: ${selector}`);
                break;
            }
        }
        
        // Also check for redirect patterns
        const currentUrl = request.loadedUrl || '';
        if (currentUrl.includes('sorry.google.com') || currentUrl.includes('consent.google.com')) {
            challengeDetected = true;
            crawlerLog.warning('Challenge detected via URL redirect');
        }
        
        if (challengeDetected) {
            crawlerLog.warning('Anti-bot challenge detected, rotating session and retrying...');
            if (session) {
                session.retire();
            }
            // Add longer delay before retrying
            await new Promise(resolve => setTimeout(resolve, 5000 + Math.random() * 5000));
            throw new Error('CHALLENGE_DETECTED');
        }

        // APPROACH 1: Extract jobs from JSON-LD structured data
        let jobsFromJsonLd = [];
        try {
            const jsonLdScripts = $('script[type="application/ld+json"]');
            crawlerLog.info(`Found ${jsonLdScripts.length} JSON-LD scripts`);
            
            jsonLdScripts.each((_, script) => {
                try {
                    const jsonContent = $(script).html();
                    if (jsonContent) {
                        const data = JSON.parse(jsonContent);
                        
                        // Handle different JSON-LD structures
                        const extractJobsFromData = (obj) => {
                            if (Array.isArray(obj)) {
                                obj.forEach(extractJobsFromData);
                            } else if (obj && typeof obj === 'object') {
                                if (obj['@type'] === 'JobPosting' || obj.type === 'JobPosting') {
                                    const job = {
                                        title: obj.title || obj.name,
                                        company: obj.hiringOrganization?.name || obj.company?.name,
                                        location: obj.jobLocation?.address?.addressLocality || obj.location,
                                        description_text: obj.description,
                                        salary: obj.baseSalary?.value || obj.salary,
                                        job_type: obj.employmentType,
                                        date_posted: obj.datePosted,
                                        url: obj.url || obj.applyUrl,
                                        id: `jsonld_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
                                    };
                                    
                                    if (job.title) {
                                        jobsFromJsonLd.push(job);
                                        crawlerLog.info(`Found job from JSON-LD: ${job.title}`);
                                    }
                                } else {
                                    // Recursively search nested objects
                                    Object.values(obj).forEach(extractJobsFromData);
                                }
                            }
                        };
                        
                        extractJobsFromData(data);
                    }
                } catch (error) {
                    crawlerLog.debug(`Failed to parse JSON-LD: ${error.message}`);
                }
            });
        } catch (error) {
            crawlerLog.debug(`JSON-LD extraction failed: ${error.message}`);
        }

        // Process JSON-LD jobs
        for (const job of jobsFromJsonLd) {
            if (jobsCollected >= RESULTS_WANTED) break;
            
            if (!processedIds.has(job.id)) {
                const finalJob = {
                    ...job,
                    _source: 'google.com/jobs',
                    _fetchedAt: new Date().toISOString(),
                    _scrapedUrl: request.loadedUrl,
                    _searchKeyword: keyword,
                    _searchLocation: location || null,
                    _postedDateFilter: posted_date,
                    _extractionMethod: 'json-ld',
                };

                await Dataset.pushData(finalJob);
                jobsCollected++;
                processedIds.add(job.id);
                crawlerLog.info(`✅ Job ${jobsCollected}/${RESULTS_WANTED} saved from JSON-LD: "${finalJob.title}"`);
            }
        }

        // APPROACH 2: Try to extract jobs from React/Vue app state
        let jobsFromAppState = [];
        try {
            const scriptTags = $('script:not([src])');
            scriptTags.each((_, script) => {
                const scriptContent = $(script).html() || '';
                
                // Look for job data in various JavaScript patterns
                const patterns = [
                    /window\.__INITIAL_STATE__\s*=\s*({.+?});/s,
                    /window\.__STATE__\s*=\s*({.+?});/s,
                    /window\.initialData\s*=\s*({.+?});/s,
                    /"jobs":\s*(\[.+?\])/s,
                    /"jobPostings":\s*(\[.+?\])/s,
                ];
                
                for (const pattern of patterns) {
                    const match = scriptContent.match(pattern);
                    if (match) {
                        try {
                            const data = JSON.parse(match[1]);
                            crawlerLog.info(`Found app state data, analyzing...`);
                            
                            // Recursively search for job-like objects
                            const findJobs = (obj, path = '') => {
                                if (Array.isArray(obj)) {
                                    obj.forEach((item, index) => findJobs(item, `${path}[${index}]`));
                                } else if (obj && typeof obj === 'object') {
                                    // Check if this object looks like a job
                                    if (obj.title && (obj.company || obj.companyName) && typeof obj.title === 'string') {
                                        const job = {
                                            title: obj.title,
                                            company: obj.company || obj.companyName || obj.organization,
                                            location: obj.location || obj.jobLocation,
                                            description_text: obj.description || obj.summary,
                                            salary: obj.salary || obj.compensation,
                                            job_type: obj.jobType || obj.employmentType,
                                            date_posted: obj.datePosted || obj.postedDate,
                                            url: obj.url || obj.link || obj.applyUrl,
                                            id: `appstate_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
                                        };
                                        
                                        jobsFromAppState.push(job);
                                        crawlerLog.info(`Found job from app state: ${job.title}`);
                                    }
                                    
                                    // Continue searching nested objects
                                    Object.keys(obj).forEach(key => {
                                        if (typeof obj[key] === 'object') {
                                            findJobs(obj[key], `${path}.${key}`);
                                        }
                                    });
                                }
                            };
                            
                            findJobs(data);
                        } catch (error) {
                            crawlerLog.debug(`Failed to parse app state JSON: ${error.message}`);
                        }
                    }
                }
            });
        } catch (error) {
            crawlerLog.debug(`App state extraction failed: ${error.message}`);
        }

        // Process app state jobs
        for (const job of jobsFromAppState) {
            if (jobsCollected >= RESULTS_WANTED) break;
            
            if (!processedIds.has(job.id)) {
                const finalJob = {
                    ...job,
                    _source: 'google.com/jobs',
                    _fetchedAt: new Date().toISOString(),
                    _scrapedUrl: request.loadedUrl,
                    _searchKeyword: keyword,
                    _searchLocation: location || null,
                    _postedDateFilter: posted_date,
                    _extractionMethod: 'app-state',
                };

                await Dataset.pushData(finalJob);
                jobsCollected++;
                processedIds.add(job.id);
                crawlerLog.info(`✅ Job ${jobsCollected}/${RESULTS_WANTED} saved from app state: "${finalJob.title}"`);
            }
        }

        // APPROACH 3: Enhanced DOM-based job extraction with multiple selector strategies
        crawlerLog.info('Starting DOM-based job extraction...');
        
        // Log page structure for debugging
        crawlerLog.info(`Page elements count: div=${$('div').length}, span=${$('span').length}, a=${$('a').length}`);
        crawlerLog.info(`Data attributes: data-hveid=${$('[data-hveid]').length}, data-ved=${$('[data-ved]').length}`);
        crawlerLog.info(`Google classes: .g=${$('.g').length}, .tF2Cxc=${$('.tF2Cxc').length}`);
        
        // Enhanced job card selectors with more comprehensive patterns
        const jobCardSelectors = [
            // Primary Google Jobs selectors (2024-2025 patterns)
            'div[data-hveid]:has(div[role="heading"])', // Job cards with heading role
            'div[jscontroller*="UzbKLd"]', // Google Jobs specific controller
            'div[jscontroller*="jobs"]', // Jobs controller variations
            'div[data-ved]:has(h3)', // Cards with h3 headings
            'g-card', // Google card components
            'div[data-sokoban-container]', // Sokoban layout containers
            'div.VjDLd', // Jobs result container
            'div.g:has(div[role="heading"])', // Standard result with job heading
            'div[data-hveid]:has(a[href*="search?q="])', // Search result links
            'li[data-ved]:has(div[role="heading"])', // List items with job headings
            '.g .tF2Cxc', // Standard Google result structure
            'div[jsaction*="job"]', // Elements with job-related actions
            
            // Additional 2025 selectors
            'div[class*="job"]', // Any div with job in class name
            'article[data-ved]', // Article elements with tracking
            'div[role="listitem"]', // List items
            '[data-testid*="job"]', // Test ID selectors
            'div[aria-label*="job"]', // Accessibility labels
            '.srg > div', // Search result group children
            '#search div[data-hveid]', // Search area results
        ];

        let jobCards = $();
        for (const selector of jobCardSelectors) {
            try {
                const cards = $(selector);
                crawlerLog.debug(`Selector "${selector}" found ${cards.length} elements`);
                
                if (cards.length > 0) {
                    // Enhanced filtering with multiple criteria
                    const jobCardsCandidates = cards.filter((_, el) => {
                        const $el = $(el);
                        const text = $el.text().toLowerCase();
                        const html = $el.html() || '';
                        
                        // Check for job-related keywords
                        const hasJobKeywords = text.includes('job') || text.includes('position') || 
                                              text.includes('career') || text.includes('hiring') ||
                                              text.includes('apply') || text.includes('salary') ||
                                              text.includes('company') || text.includes('employment') ||
                                              text.includes('work') || text.includes('role');
                        
                        // Check for structural elements
                        const hasStructure = $el.find('h3, div[role="heading"], a[href]').length > 0;
                        
                        // Check for links (but be more flexible)
                        const hasLinks = $el.find('a[href]').length > 0;
                        
                        // Avoid navigation elements
                        const isNotNavigation = !text.includes('next page') && 
                                               !text.includes('previous') &&
                                               !text.includes('search') &&
                                               !html.includes('nav');
                        
                        // Size check (avoid tiny elements)
                        const hasContent = text.length > 20;
                        
                        return hasJobKeywords && hasStructure && hasLinks && isNotNavigation && hasContent;
                    });
                    
                    if (jobCardsCandidates.length > 0) {
                        jobCards = jobCardsCandidates;
                        crawlerLog.info(`✓ Using selector: "${selector}", found ${jobCards.length} job cards`);
                        break;
                    }
                }
            } catch (error) {
                crawlerLog.debug(`Selector failed: ${selector} - ${error.message}`);
            }
        }

        // Enhanced fallback with even broader search
        if (jobCards.length === 0) {
            crawlerLog.warning('Primary selectors failed, trying comprehensive fallback approach...');
            
            // Try ultra-broad approaches
            const fallbackApproaches = [
                // Approach 1: Any div with job-related content and links
                () => $('div').filter((_, el) => {
                    const $el = $(el);
                    const text = $el.text().toLowerCase();
                    return (text.includes('job') || text.includes('position') || text.includes('hiring')) && 
                           $el.find('a[href]').length > 0 &&
                           text.length > 50 && text.length < 2000;
                }),
                
                // Approach 2: Any element with Google tracking and job content
                () => $('[data-hveid], [data-ved]').filter((_, el) => {
                    const $el = $(el);
                    const text = $el.text().toLowerCase();
                    return text.includes('job') && $el.find('a[href]').length > 0;
                }),
                
                // Approach 3: Links that might be job postings
                () => $('a[href*="search"], a[href*="google.com"]').parent().filter((_, el) => {
                    const $el = $(el);
                    const text = $el.text().toLowerCase();
                    return text.includes('job') || text.includes('position');
                }),
                
                // Approach 4: Standard Google result structure
                () => $('.g, .tF2Cxc').filter((_, el) => {
                    const $el = $(el);
                    const text = $el.text().toLowerCase();
                    return (text.includes('job') || text.includes('career')) && 
                           $el.find('h3, a[href]').length > 0;
                }),
            ];
            
            for (let i = 0; i < fallbackApproaches.length; i++) {
                try {
                    const cards = fallbackApproaches[i]();
                    crawlerLog.info(`Fallback approach ${i + 1} found ${cards.length} candidates`);
                    
                    if (cards.length > 0) {
                        jobCards = cards;
                        crawlerLog.info(`✓ Fallback approach ${i + 1} succeeded with ${cards.length} job cards`);
                        break;
                    }
                } catch (error) {
                    crawlerLog.debug(`Fallback approach ${i + 1} failed: ${error.message}`);
                }
            }
        }

        crawlerLog.info(`Found ${jobCards.length} potential job cards on page: ${request.loadedUrl}`);
        
        // Enhanced debugging for failed job detection
        if (jobCards.length === 0) {
            crawlerLog.warning('❌ No job cards found. Comprehensive page analysis:');
            crawlerLog.info(`Page title: "${$('title').text()}"`);
            crawlerLog.info(`URL: ${request.loadedUrl}`);
            crawlerLog.info(`Body size: ${$('body').text().length} characters`);
            crawlerLog.info(`Page contains "job": ${$('body').text().toLowerCase().includes('job')}`);
            crawlerLog.info(`Page contains "position": ${$('body').text().toLowerCase().includes('position')}`);
            crawlerLog.info(`Page contains "apply": ${$('body').text().toLowerCase().includes('apply')}`);
            
            // Log element counts
            crawlerLog.info(`Element counts:`);
            crawlerLog.info(`  - div[data-hveid]: ${$('div[data-hveid]').length}`);
            crawlerLog.info(`  - .g: ${$('.g').length}`);
            crawlerLog.info(`  - div[jscontroller]: ${$('div[jscontroller]').length}`);
            crawlerLog.info(`  - h3: ${$('h3').length}`);
            crawlerLog.info(`  - a[href]: ${$('a[href]').length}`);
            
            // Log sample text from key elements
            $('div[data-hveid]').slice(0, 3).each((i, el) => {
                const text = $(el).text().trim().substring(0, 100);
                crawlerLog.info(`  - data-hveid[${i}]: "${text}"`);
            });
            
            $('.g').slice(0, 3).each((i, el) => {
                const text = $(el).text().trim().substring(0, 100);
                crawlerLog.info(`  - .g[${i}]: "${text}"`);
            });
            
            // Try to save page HTML for debugging
            try {
                const pageHtml = $.html();
                crawlerLog.info(`Full page HTML length: ${pageHtml.length}`);
                
                // Save a sample of the HTML to understand the structure
                if (pageHtml.length > 1000) {
                    const sample = pageHtml.substring(0, 2000);
                    crawlerLog.debug(`HTML sample: ${sample}`);
                }
            } catch (error) {
                crawlerLog.debug(`Could not analyze HTML: ${error.message}`);
            }
        }

        // Process DOM-extracted job cards
        for (let i = 0; i < jobCards.length && jobsCollected < RESULTS_WANTED; i++) {
            const $el = $(jobCards[i]);
            
            // Multiple ID extraction methods
            let jobId = $el.attr('data-hveid') || 
                       $el.attr('data-ved') || 
                       $el.attr('data-id') ||
                       $el.closest('[data-hveid]').attr('data-hveid') ||
                       `job_${i}_${Date.now()}`;

            if (processedIds.has(jobId)) {
                continue;
            }

            // Enhanced title extraction with better specificity
            const titleSelectors = [
                'div[role="heading"] h3', // Most specific for Google Jobs
                'div[role="heading"]', // General heading role
                'h3.LC20lb', // Classic Google result title
                'h3 a span', // Linked title span
                'h3 > div', // Title in div under h3
                'a[data-ved] > div:first-child', // First div in link
                'div[jscontroller] h3', // Controller-based heading
                '.tF2Cxc h3', // Standard result structure
                'div[style*="font-size"] > div:first-child', // Styled title
                '[data-hveid] div:first-child h3', // Nested title
            ];
            
            let title = null;
            for (const selector of titleSelectors) {
                const titleElement = $el.find(selector);
                if (titleElement.length > 0) {
                    title = getText($, titleElement);
                    if (title && title.length > 3 && title.length < 200) {
                        // Ensure it's not a company name or other metadata
                        const lowerTitle = title.toLowerCase();
                        if (!lowerTitle.includes('google.com') && !lowerTitle.includes('search') && 
                            !lowerTitle.match(/^\d+\s*(minutes?|hours?|days?|weeks?|months?)\s+ago$/)) {
                            break;
                        }
                    }
                    title = null; // Reset if not valid
                }
            }

            // Enhanced company extraction with better filtering
            const companySelectors = [
                'div[role="heading"] ~ div:first', // First div after heading
                '.TbwUpd.NJjxre', // Google Jobs company class
                '.vvjwJb .BNeawe', // Company in result snippet
                'span[style*="color:#006621"]', // Green colored company names
                'span[style*="color:#70757a"]', // Gray colored company names
                'div[style*="color"] span:not(:empty)', // Non-empty colored spans
                'cite', // Citation elements often contain company info
                '.UPmit.AP7Wnd', // Company metadata classes
                '.tF2Cxc .yuRUbf ~ div span', // Company in result structure
                'div:not([role="heading"]) > div:first-child:not(:empty)', // First non-heading child
            ];
            
            let company = null;
            for (const selector of companySelectors) {
                const companyElement = $el.find(selector);
                if (companyElement.length > 0) {
                    const companyText = getText($, companyElement);
                    if (companyText && companyText !== title && companyText.length > 1 && companyText.length < 100) {
                        // Filter out common non-company text
                        const lowerCompany = companyText.toLowerCase();
                        if (!lowerCompany.includes('ago') && !lowerCompany.includes('google.com') &&
                            !lowerCompany.includes('search') && !lowerCompany.includes('http') &&
                            !lowerCompany.match(/^\d+\s*(minutes?|hours?|days?|weeks?|months?)/) &&
                            !lowerCompany.includes('•') && !lowerCompany.includes('...')) {
                            company = companyText;
                            break;
                        }
                    }
                }
            }

            // Enhanced location extraction with better pattern matching
            const locationSelectors = [
                'span:contains("•")', // Separator-based location
                '.rllt__details div:nth-child(2)', // Google Maps style location
                'div:contains("km")', // Distance-based location
                'span[style*="color"]:contains(",")', // Colored location with comma
                'cite', // Citation elements often contain location
                '.UPmit.AP7Wnd:contains(",")', // Location metadata with comma
                'div[style*="color"] span:contains(",")', // Colored spans with location
                '.tF2Cxc .yuRUbf ~ div:contains(",")', // Location in result structure
                'span:not(:empty)', // Non-empty spans (broad fallback)
            ];
            
            let location = null;
            for (const selector of locationSelectors) {
                $el.find(selector).each((_, locEl) => {
                    const locationText = getText($, $(locEl));
                    if (locationText && !location) {
                        // Check if text looks like a location
                        const hasLocationMarkers = locationText.includes(',') || 
                                                 locationText.includes('km') || 
                                                 locationText.includes('mi') ||
                                                 locationText.includes('•') ||
                                                 /\b(Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|City|State|Country)\b/i.test(locationText);
                        
                        if (hasLocationMarkers && locationText.length < 100 && locationText !== title && locationText !== company) {
                            // Further validation
                            const lowerLocation = locationText.toLowerCase();
                            if (!lowerLocation.includes('ago') && !lowerLocation.includes('job') &&
                                !lowerLocation.includes('apply') && !lowerLocation.includes('$') &&
                                !lowerLocation.match(/^\d+\s*(minutes?|hours?|days?|weeks?|months?)/)) {
                                location = locationText.replace(/•/g, '').trim();
                                return false; // Break the each loop
                            }
                        }
                    }
                });
                if (location) break;
            }

            // Enhanced date extraction with pattern matching
            const dateSelectors = [
                'span:contains("ago")', // Most reliable
                'span:contains("day")',
                'span:contains("hour")',  
                'span:contains("week")',
                'span:contains("month")',
                '.f', // Google's date class
                '.s3v9rd', // Another Google date class
                '.UPmit.AP7Wnd', // Metadata classes
                'div[style*="color"] span', // Colored metadata
            ];
            
            let datePosted = null;
            for (const selector of dateSelectors) {
                $el.find(selector).each((_, dateEl) => {
                    const dateText = getText($, $(dateEl));
                    if (dateText && !datePosted) {
                        // Check if text matches date pattern
                        const datePattern = /\d+\s*(minute|hour|day|week|month|year)s?\s+ago/i;
                        const relativePattern = /(yesterday|today|just now|recently)/i;
                        
                        if (datePattern.test(dateText) || relativePattern.test(dateText)) {
                            datePosted = dateText;
                            return false; // Break the each loop
                        }
                    }
                });
                if (datePosted) break;
            }

            // Enhanced detail link extraction with priority and validation
            const linkSelectors = [
                'h3 a[href*="search?q="]', // Highest priority: title links to job details
                'a[href*="google.com/search?q="]', // Google search job detail links
                'a[data-ved][href*="search"]', // Links with ved tracking
                'h3 a[href]', // Any title link
                'a[href*="search"][href*="ibp=htl;jobs"]', // Jobs-specific search links
                'a[data-ved]:first', // First tracked link
                '.tF2Cxc h3 a', // Standard result structure link
                'a[href]:first-child', // First link in card
            ];
            
            let detailLink = null;
            for (const selector of linkSelectors) {
                const linkElement = $el.find(selector).first();
                if (linkElement.length > 0) {
                    const link = linkElement.attr('href');
                    if (link && link.trim()) {
                        // Validate link quality
                        if (link.includes('search') || link.includes('google.com')) {
                            detailLink = link;
                            break;
                        }
                    }
                }
            }
            
            // Fallback: look for any reasonable link
            if (!detailLink) {
                $el.find('a[href]').each((_, linkEl) => {
                    const link = $(linkEl).attr('href');
                    if (link && (link.includes('search') || link.includes('google.com')) && !detailLink) {
                        detailLink = link;
                        return false; // Break the each loop
                    }
                });
            }

            // Final validation before processing
            if (detailLink && title && title.length > 3) {
                // Additional quality checks
                const titleLower = title.toLowerCase();
                const isValidJobTitle = !titleLower.includes('google.com') && 
                                      !titleLower.includes('search') &&
                                      !titleLower.includes('next page') &&
                                      !titleLower.includes('previous page') &&
                                      title.length < 200;
                
                if (isValidJobTitle) {
                    const fullUrl = detailLink.startsWith('http') 
                        ? detailLink 
                        : new URL(detailLink, request.loadedUrl).href;

                    await crawler.addRequests([{ 
                        url: fullUrl,
                        userData: {
                            label: 'DETAIL',
                            jobData: { 
                                title, 
                                company: company || 'Not specified', 
                                location: location || 'Not specified', 
                                date_posted: datePosted || 'Not specified', 
                                id: jobId 
                            },
                        },
                    }]);
                    
                    crawlerLog.info(`✓ Enqueued job ${i + 1}: "${title}" at ${company || 'Unknown company'}`);
                } else {
                    crawlerLog.debug(`✗ Skipped invalid job title: "${title}"`);
                }
            } else {
                crawlerLog.debug(`✗ Skipped job card ${i + 1}: missing title or link`);
            }
        }

        // APPROACH 4: Fallback text extraction if no structured data found
        if (jobsCollected < RESULTS_WANTED && jobsFromJsonLd.length === 0 && jobsFromAppState.length === 0 && jobCards.length === 0) {
            crawlerLog.warning('No structured data found, attempting text-based extraction...');
            
            try {
                const bodyText = $('body').text();
                const lines = bodyText.split('\n').filter(line => line.trim().length > 0);
                
                // Look for job-like patterns in text
                const jobPatterns = [
                    /(.+?)\s+at\s+(.+?)\s+•?\s*(.+?)(?:\n|$)/gi, // "Job Title at Company • Location"
                    /(.+?)\s+-\s+(.+?)\s+-\s+(.+?)(?:\n|$)/gi,  // "Job Title - Company - Location"
                    /Job:\s*(.+?)\s+Company:\s*(.+?)(?:\s+Location:\s*(.+?))?(?:\n|$)/gi,
                ];
                
                let textBasedJobs = [];
                for (const pattern of jobPatterns) {
                    let match;
                    while ((match = pattern.exec(bodyText)) !== null && textBasedJobs.length < 10) {
                        const job = {
                            title: match[1]?.trim(),
                            company: match[2]?.trim(),
                            location: match[3]?.trim(),
                            id: `text_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
                        };
                        
                        if (job.title && job.title.length > 3 && job.title.length < 100) {
                            textBasedJobs.push(job);
                            crawlerLog.info(`Found job from text: ${job.title}`);
                        }
                    }
                }
                
                // Process text-based jobs
                for (const job of textBasedJobs) {
                    if (jobsCollected >= RESULTS_WANTED) break;
                    
                    if (!processedIds.has(job.id)) {
                        const finalJob = {
                            ...job,
                            _source: 'google.com/jobs',
                            _fetchedAt: new Date().toISOString(),
                            _scrapedUrl: request.loadedUrl,
                            _searchKeyword: keyword,
                            _searchLocation: location || null,
                            _postedDateFilter: posted_date,
                            _extractionMethod: 'text-parsing',
                        };

                        await Dataset.pushData(finalJob);
                        jobsCollected++;
                        processedIds.add(job.id);
                        crawlerLog.info(`✅ Job ${jobsCollected}/${RESULTS_WANTED} saved from text: "${finalJob.title}"`);
                    }
                }
            } catch (error) {
                crawlerLog.debug(`Text-based extraction failed: ${error.message}`);
            }
        }

        // Enhanced pagination handling with better detection
        if (request.userData.label !== 'DETAIL' && jobsCollected < RESULTS_WANTED) {
            const paginationSelectors = [
                // Google Jobs specific pagination
                'a[href*="start="][href*="ibp=htl;jobs"]',
                'a[aria-label*="Next page"]',
                'a[aria-label*="Next"]',
                'a[id="pnnext"]',
                '#pnnext',
                'a[href*="start="]:contains("Next")',
                'a[href*="start="]:contains(">")',
                'td[style*="text-align:left"] a[href*="start="]',
                '.d6cvqb a[href*="start="]', // Google pagination container
                'a[href*="start="][data-ved]', // Tracked pagination links
                '.AaVjTc a[href*="start="]', // Another pagination container
            ];
            
            let nextPageLink = null;
            let currentStart = 0;
            
            // Try to extract current start parameter
            try {
                const currentUrl = new URL(request.loadedUrl);
                currentStart = parseInt(currentUrl.searchParams.get('start') || '0', 10);
            } catch (error) {
                crawlerLog.debug('Could not parse current start parameter');
            }
            
            for (const selector of paginationSelectors) {
                const link = $(selector);
                if (link.length > 0) {
                    const href = link.attr('href');
                    if (href) {
                        try {
                            // Validate that this is actually a next page
                            const nextUrl = new URL(href, request.loadedUrl);
                            const nextStart = parseInt(nextUrl.searchParams.get('start') || '0', 10);
                            
                            if (nextStart > currentStart) {
                                nextPageLink = href;
                                crawlerLog.info(`Found next page: start=${nextStart} (current=${currentStart})`);
                                break;
                            }
                        } catch (error) {
                            crawlerLog.debug(`Invalid pagination URL: ${href}`);
                        }
                    }
                }
            }

            // Fallback: construct next page URL manually
            if (!nextPageLink && currentStart >= 0) {
                try {
                    const currentUrl = new URL(request.loadedUrl);
                    const nextStart = currentStart + 10; // Google typically uses 10 results per page
                    currentUrl.searchParams.set('start', nextStart.toString());
                    
                    // Verify we haven't exceeded reasonable pagination limits
                    if (nextStart <= 1000) { // Reasonable limit to avoid infinite loops
                        nextPageLink = currentUrl.toString();
                        crawlerLog.info(`Manually constructed next page URL: start=${nextStart}`);
                    }
                } catch (error) {
                    crawlerLog.debug('Could not construct manual pagination URL');
                }
            }

            if (nextPageLink) {
                const fullNextUrl = nextPageLink.startsWith('http') 
                    ? nextPageLink 
                    : new URL(nextPageLink, request.loadedUrl).href;
                
                await crawler.addRequests([{
                    url: fullNextUrl,
                    userData: { 
                        label: 'LIST',
                        urlVariant: request.userData.urlVariant || 1,
                    },
                }]);
                crawlerLog.info('Enqueued next page of job listings.');
            } else {
                crawlerLog.info('No more pages found or pagination limit reached.');
                
                // If this was the first page and we didn't find many jobs, try other URL variants
                if (request.userData.isInitialRequest && jobsCollected < 5 && request.userData.urlVariant === 1) {
                    crawlerLog.info('Few jobs found with primary URL, trying alternative URL formats...');
                    
                    const alternativeUrls = buildStartUrls(keyword, location, posted_date).slice(1); // Skip first URL
                    const alternativeRequests = alternativeUrls.map((url, index) => ({
                        url,
                        userData: { 
                            label: 'LIST',
                            urlVariant: index + 2, // Start from variant 2
                            isInitialRequest: true,
                        },
                    }));
                    
                    if (alternativeRequests.length > 0) {
                        await crawler.addRequests(alternativeRequests);
                        crawlerLog.info(`Added ${alternativeRequests.length} alternative URL variants to queue`);
                    }
                }
            }
        }
    },

    async failedRequestHandler({ request, log: crawlerLog, session }, error) {
        crawlerLog.error(`Request ${request.url} failed: ${error.message}`);
        
        // Handle specific error types with appropriate delays and actions
        if (error.message.includes('CHALLENGE_DETECTED')) {
            crawlerLog.warning('Detected anti-bot challenge, will retry with new session after delay');
            if (session) {
                session.retire();
            }
            // Longer delay for challenges
            await new Promise(resolve => setTimeout(resolve, 10000 + Math.random() * 10000));
        } else if (error.message.includes('timeout') || error.message.includes('ETIMEDOUT')) {
            crawlerLog.warning('Request timeout, will retry after delay');
            await new Promise(resolve => setTimeout(resolve, 3000 + Math.random() * 3000));
        } else if (error.message.includes('403') || error.message.includes('blocked') || error.message.includes('429')) {
            crawlerLog.warning('Request blocked/rate limited, rotating session and adding delay');
            if (session) {
                session.retire();
            }
            await new Promise(resolve => setTimeout(resolve, 15000 + Math.random() * 15000));
        } else if (error.message.includes('500') || error.message.includes('502') || error.message.includes('503')) {
            crawlerLog.warning('Server error, will retry after delay');
            await new Promise(resolve => setTimeout(resolve, 5000 + Math.random() * 5000));
        } else if (error.message.includes('ECONNRESET') || error.message.includes('ENOTFOUND')) {
            crawlerLog.warning('Connection error, will retry');
            await new Promise(resolve => setTimeout(resolve, 2000 + Math.random() * 3000));
        }
        
        // Log additional context for debugging
        crawlerLog.info(`Session state: ${session ? 'active' : 'none'}, Retry attempt will follow`);
    },
});

// Detail page handler is separate for clarity
crawler.router.addHandler('DETAIL', async ({ $, request, log: crawlerLog, session }) => {
    if (jobsCollected >= RESULTS_WANTED) {
        crawlerLog.info('Target number of jobs reached, skipping detail processing.');
        return;
    }

    const { jobData } = request.userData;

    if (processedIds.has(jobData.id)) {
        return;
    }

    // Check for anti-bot challenges on detail pages too
    if ($('form[action*="sorry"]').length > 0 || $('div:contains("unusual traffic")').length > 0) {
        crawlerLog.warning('Anti-bot challenge detected on detail page, skipping...');
        if (session) {
            session.retire();
        }
        return;
    }

    // Enhanced title extraction from detail page
    const titleSelectors = [
        'h2',
        'h1',
        '[role="heading"]',
        'div[style*="font-size"] > div:first',
        '.LC20lb',
    ];
    
    let detailTitle = jobData.title;
    for (const selector of titleSelectors) {
        const title = $(selector).first().text().trim();
        if (title && title.length > 5) {
            detailTitle = title;
            break;
        }
    }

    // Enhanced description extraction with better selectors and filtering
    const descriptionSelectors = [
        // Primary Google Jobs description selectors
        'div[data-content-feature="1"]', // Job description content
        '.yp', // Google's description class
        '.s', // Snippet class
        'div[role="main"] span:not(:empty)', // Main content spans
        'div[style*="line-height"] span', // Styled description spans
        '.g .s', // Result snippet
        'div[jscontroller] div:not([role="heading"])', // Content in JS controllers
        '.tF2Cxc .VwiC3b', // Standard result snippet
        'div[data-hveid] div:not([role="heading"])', // Content in result cards
        '.ULSxyf', // Another Google description class
        '.hJNv6b', // Job snippet class
        '.Qk80Jf', // Additional snippet class
    ];
    
    let description_html = '';
    let description_text = '';
    
    for (const selector of descriptionSelectors) {
        try {
            const elements = $(selector);
            
            // Try each element to find the best description
            elements.each((_, el) => {
                const $el = $(el);
                const html = $el.html() || '';
                const text = htmlToText(html);
                
                // Quality checks for description content
                if (text && text.length > 50 && text.length < 10000) {
                    const lowerText = text.toLowerCase();
                    
                    // Check if it contains job-related content
                    const hasJobContent = lowerText.includes('job') || lowerText.includes('position') ||
                                        lowerText.includes('role') || lowerText.includes('responsibilities') ||
                                        lowerText.includes('requirements') || lowerText.includes('experience') ||
                                        lowerText.includes('skills') || lowerText.includes('qualifications') ||
                                        lowerText.includes('duties') || lowerText.includes('candidate');
                    
                    // Avoid non-description content
                    const isNotDescription = lowerText.includes('search') || 
                                           lowerText.includes('google.com') ||
                                           lowerText.includes('next page') ||
                                           lowerText.includes('previous page') ||
                                           text.length < 100;
                    
                    if (hasJobContent && !isNotDescription && !description_text) {
                        description_html = html;
                        description_text = text;
                        return false; // Break the each loop
                    }
                }
            });
            
            if (description_text) break;
        } catch (error) {
            crawlerLog.debug(`Description selector failed: ${selector} - ${error.message}`);
        }
    }

    // Enhanced fallback: if no good description found, try broader selectors
    if (!description_text || description_text.length < 50) {
        const broadSelectors = [
            'div[role="main"]',
            '.g:has(h3)',
            '#main',
            '[data-ved] > div',
            '.tF2Cxc',
            'div[jscontroller]:not(:empty)',
            'div[data-hveid]:not(:empty)',
        ];
        
        for (const selector of broadSelectors) {
            try {
                const element = $(selector).first();
                if (element.length > 0) {
                    const fullText = element.text().trim();
                    
                    // Extract meaningful parts and clean up
                    if (fullText.length > 100) {
                        // Try to extract job description portion
                        const sentences = fullText.split(/[.!?]+/).filter(s => s.trim().length > 20);
                        const jobSentences = sentences.filter(sentence => {
                            const lower = sentence.toLowerCase();
                            return lower.includes('job') || lower.includes('position') ||
                                   lower.includes('role') || lower.includes('responsibilities') ||
                                   lower.includes('experience') || lower.includes('skills');
                        });
                        
                        if (jobSentences.length > 0) {
                            description_text = jobSentences.join('. ').trim();
                            description_html = element.html() || '';
                            break;
                        } else if (fullText.length > 200 && !description_text) {
                            // Use full text as fallback
                            description_text = fullText.substring(0, 2000); // Limit length
                            description_html = element.html() || '';
                            break;
                        }
                    }
                }
            } catch (error) {
                crawlerLog.debug(`Broad description selector failed: ${selector} - ${error.message}`);
            }
        }
    }

    // Enhanced source and URL extraction
    let source = null;
    let url = null;
    let salary = null;
    let jobType = null;

    // Extract apply URLs
    const urlSelectors = [
        'a[href*="/url?q="]',
        'a[href*="apply"]',
        'a[href^="http"]:not([href*="google.com"])',
        'a[ping]',
    ];
    
    for (const selector of urlSelectors) {
        $(selector).each((_, el) => {
            const $el = $(el);
            const link = $el.attr('href');
            
            if (link && link.includes('/url?q=')) {
                try {
                    const urlParams = new URLSearchParams(link.split('?')[1]);
                    const decodedUrl = urlParams.get('q');
                    if (decodedUrl && !url && !decodedUrl.includes('google.com')) {
                        url = decodedUrl;
                        source = getText($, $el) || new URL(decodedUrl).hostname.replace(/^www\./, '');
                    }
                } catch (e) {
                    crawlerLog.debug(`Failed to parse URL: ${link}`);
                }
            } else if (link && link.startsWith('http') && !link.includes('google.com') && !url) {
                url = link;
                source = getText($, $el) || new URL(link).hostname.replace(/^www\./, '');
            }
        });
    }

    // Extract salary information
    const salaryPatterns = [
        /\$[\d,]+(?:\s*-\s*\$[\d,]+)?(?:\s*(?:per|\/)\s*(?:hour|year|month|week))?/gi,
        /£[\d,]+(?:\s*-\s*£[\d,]+)?(?:\s*(?:per|\/)\s*(?:hour|year|month|week))?/gi,
        /€[\d,]+(?:\s*-\s*€[\d,]+)?(?:\s*(?:per|\/)\s*(?:hour|year|month|week))?/gi,
        /[\d,]+(?:\s*-\s*[\d,]+)?\s*(?:USD|GBP|EUR)\s*(?:per|\/)\s*(?:hour|year|month|week)/gi,
    ];
    
    const fullText = $('body').text();
    for (const pattern of salaryPatterns) {
        const matches = fullText.match(pattern);
        if (matches && matches.length > 0) {
            salary = matches[0].trim();
            break;
        }
    }

    // Extract job type
    const jobTypeKeywords = ['full-time', 'part-time', 'contract', 'temporary', 'internship', 'freelance', 'remote'];
    const lowerText = fullText.toLowerCase();
    for (const keyword of jobTypeKeywords) {
        if (lowerText.includes(keyword)) {
            jobType = keyword;
            break;
        }
    }

    // Enhanced company extraction from detail page
    let detailCompany = jobData.company;
    const companySelectors = [
        'div:contains("Company:") + div',
        'span[style*="color"]:not(:contains("$"))',
        '.vvjwJb',
        '.TbwUpd',
    ];
    
    for (const selector of companySelectors) {
        const companyText = getText($, $(selector));
        if (companyText && companyText.length > 1 && companyText.length < 100) {
            detailCompany = companyText;
            break;
        }
    }

    const finalJob = {
        ...jobData,
        title: detailTitle || jobData.title,
        company: detailCompany || jobData.company,
        description_html: description_html || '',
        description_text: description_text || '',
        salary: salary || null,
        job_type: jobType || null,
        source: source || 'google.com',
        url: url || null,
        _source: 'google.com/jobs',
        _fetchedAt: new Date().toISOString(),
        _scrapedUrl: request.loadedUrl,
        _searchKeyword: keyword,
        _searchLocation: location || null,
        _postedDateFilter: posted_date,
    };

    // Final validation before saving
    if (finalJob.title && finalJob.title.length > 3) {
        await Dataset.pushData(finalJob);
        jobsCollected++;
        processedIds.add(jobData.id);
        crawlerLog.info(`✅ Job ${jobsCollected}/${RESULTS_WANTED} saved: "${finalJob.title}" at ${finalJob.company || 'Unknown'}`);
        
        // Log additional details for debugging
        crawlerLog.debug(`Job details - Salary: ${finalJob.salary || 'N/A'}, Type: ${finalJob.job_type || 'N/A'}, Description length: ${finalJob.description_text.length}`);
    } else {
        crawlerLog.warning(`❌ Skipped invalid job data: ${JSON.stringify(jobData)}`);
    }
});

// ------------------------- RUN -------------------------
const startUrls = buildStartUrls(keyword, location, posted_date);
log.info(`Starting scrape with ${startUrls.length} URL variants:`);
startUrls.forEach((url, index) => {
    log.info(`  ${index + 1}. ${url}`);
});

// Create requests for all URL variants
const initialRequests = startUrls.map((url, index) => ({
    url,
    userData: { 
        label: 'LIST',
        urlVariant: index + 1,
        isInitialRequest: true,
    },
}));

log.info(`Input parameters:`);
log.info(`  - Keyword: "${keyword}"`);
log.info(`  - Location: "${location || 'Not specified'}"`);
log.info(`  - Posted date filter: "${posted_date}"`);
log.info(`  - Results wanted: ${RESULTS_WANTED}`);
log.info(`  - Max retries: ${maxRequestRetries}`);
log.info(`  - Using proxy: ${proxyConf ? 'Yes' : 'No'}`);

try {
    await crawler.run(initialRequests);
    
    log.info(`✅ Scraping completed successfully!`);
    log.info(`📊 Final Results:`);
    log.info(`  - Total jobs collected: ${jobsCollected}/${RESULTS_WANTED}`);
    log.info(`  - Unique jobs processed: ${processedIds.size}`);
    log.info(`  - Search keyword: "${keyword}"`);
    log.info(`  - Search location: "${location || 'Not specified'}"`);
    
    if (jobsCollected === 0) {
        log.warning(`⚠️  No jobs were collected. This might be due to:`);
        log.warning(`   1. Google's anti-bot measures blocking requests`);
        log.warning(`   2. No jobs matching the search criteria`);
        log.warning(`   3. Changes in Google's page structure`);
        log.warning(`   4. Proxy or network issues`);
        log.warning(`   Please check the logs above for more details.`);
    }
    
} catch (error) {
    log.error(`❌ Scraping failed with error: ${error.message}`);
    throw error;
} finally {
    await Actor.exit();
}
