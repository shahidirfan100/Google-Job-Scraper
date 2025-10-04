// Google Jobs scraper - Simple and Reliable Version
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
log.info(`🚀 Starting Google Jobs scraper for "${keyword}" ${location ? `in ${location}` : ''}`);
log.info(`📊 Target: ${RESULTS_WANTED} jobs, Date filter: ${posted_date}`);

// ------------------------- HELPERS -------------------------

/**
 * Build a simple, reliable Google search URL for jobs
 */
const buildJobSearchUrl = (keyword, location, dateFilter, start = 0) => {
    const params = new URLSearchParams();
    
    // Simple, effective query
    let query = keyword;
    if (location) {
        query += ` ${location}`;
    }
    query += ' jobs';
    
    params.set('q', query);
    
    // Essential parameters for job results
    params.set('ibp', 'htl;jobs');
    params.set('hl', 'en');
    
    if (start > 0) {
        params.set('start', start.toString());
    }
    
    // Date filter
    if (dateFilter && dateFilter !== 'anytime') {
        const dateFilters = {
            '24h': 'qdr:d',
            '7d': 'qdr:w', 
            '30d': 'qdr:m'
        };
        if (dateFilters[dateFilter]) {
            params.set('tbs', dateFilters[dateFilter]);
        }
    }
    
    return `https://www.google.com/search?${params.toString()}`;
};

/**
 * Simple user agent rotation
 */
const getRandomUserAgent = () => {
    const userAgents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ];
    return userAgents[Math.floor(Math.random() * userAgents.length)];
};

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
    maxRequestsPerMinute: 20,
    requestHandlerTimeoutSecs: 60,
    navigationTimeoutSecs: 60,
    maxConcurrency: 2,
    useSessionPool: true,
    persistCookiesPerSession: true,
    maxRequestRetries: maxRequestRetries,

    preNavigationHooks: [async ({ request }) => {
        // Simple delay
        await new Promise(resolve => setTimeout(resolve, 3000 + Math.random() * 2000));
        
        // Simple headers
        request.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'User-Agent': getRandomUserAgent(),
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        };
    }],

    async requestHandler({ $, request, log: crawlerLog }) {
        if (jobsCollected >= RESULTS_WANTED) {
            crawlerLog.info('Target number of jobs reached, stopping.');
            return;
        }

        crawlerLog.info(`🔍 Processing: ${request.loadedUrl}`);
        crawlerLog.info(`📄 Page title: "${$('title').text()}"`);
        
        // Check for challenges
        const pageText = $('body').text().toLowerCase();
        if (pageText.includes('unusual traffic') || pageText.includes('captcha') || pageText.includes('robot')) {
            crawlerLog.warning('⚠️ Anti-bot challenge detected');
            throw new Error('CHALLENGE_DETECTED');
        }
        
        // Log basic page info
        crawlerLog.info(`📊 Page stats: ${$('div').length} divs, ${$('a').length} links, ${$('h3').length} h3s`);

        // Simple job extraction approach
        const jobs = [];
        
        // Try to find job listings in the most common Google result patterns
        const selectors = [
            '.g',           // Standard Google result
            '.tF2Cxc',      // Another common result class
            'div[data-hveid]', // Results with tracking
            '.srg div',     // Search result group items
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
        
        crawlerLog.info(`🔍 Processing ${jobElements.length} potential job elements`);
        
        jobElements.each((index, element) => {
            if (jobsCollected >= RESULTS_WANTED) return false;
            
            const $element = $(element);
            const elementText = $element.text().toLowerCase();
            
            // Check if this element contains job-related content
            const isJobResult = elementText.includes('job') || 
                               elementText.includes('position') || 
                               elementText.includes('career') ||
                               elementText.includes('hiring') ||
                               elementText.includes('employment');
            
            if (!isJobResult) return; // Skip non-job results
            
            // Extract basic job information
            const titleElement = $element.find('h3').first();
            const title = titleElement.text().trim();
            
            if (!title || title.length < 3) return; // Skip if no valid title
            
            // Create a simple job object
            const job = {
                id: `job_${Date.now()}_${index}`,
                title: title,
                company: 'Not specified',
                location: 'Not specified', 
                description_text: $element.text().trim().substring(0, 500),
                source: 'google.com',
                _source: 'google.com/jobs',
                _fetchedAt: new Date().toISOString(),
                _scrapedUrl: request.loadedUrl,
                _searchKeyword: keyword,
                _searchLocation: location || null,
                _postedDateFilter: posted_date,
            };
            
            // Try to extract company and location from the element text
            const lines = $element.text().split('\n').map(line => line.trim()).filter(line => line.length > 0);
            
            for (const line of lines) {
                const lowerLine = line.toLowerCase();
                
                // Try to identify company line (usually comes after title)
                if (job.company === 'Not specified' && 
                    line !== title && 
                    line.length > 2 && 
                    line.length < 100 &&
                    !lowerLine.includes('ago') &&
                    !lowerLine.includes('http')) {
                    job.company = line;
                }
                
                // Try to identify location (usually contains comma or location keywords)
                if (job.location === 'Not specified' &&
                    (line.includes(',') || lowerLine.includes('remote') || lowerLine.includes('office'))) {
                    job.location = line;
                }
                
                if (job.company !== 'Not specified' && job.location !== 'Not specified') {
                    break;
                }
            }
            
            jobs.push(job);
            crawlerLog.info(`📝 Extracted job ${jobs.length}: "${job.title}" at ${job.company}`);
        });
        
        // Save jobs to dataset
        for (const job of jobs) {
            if (jobsCollected >= RESULTS_WANTED) break;
            
            if (!processedIds.has(job.id)) {
                await Dataset.pushData(job);
                jobsCollected++;
                processedIds.add(job.id);
                crawlerLog.info(`✅ Job ${jobsCollected}/${RESULTS_WANTED} saved: "${job.title}"`);
            }
        }
        
        // Simple pagination - look for next page link
        if (jobsCollected < RESULTS_WANTED) {
            const nextPageSelectors = [
                'a[aria-label*="Next"]',
                'a#pnnext', 
                'a[href*="start="]',
                'a:contains("Next")'
            ];
            
            let nextUrl = null;
            for (const selector of nextPageSelectors) {
                const nextLink = $(selector).first();
                if (nextLink.length > 0) {
                    const href = nextLink.attr('href');
                    if (href && href.includes('start=')) {
                        nextUrl = href.startsWith('http') ? href : new URL(href, request.loadedUrl).href;
                        crawlerLog.info(`🔗 Found next page: ${nextUrl}`);
                        break;
                    }
                }
            }
            
            if (nextUrl) {
                await crawler.addRequests([{ url: nextUrl }]);
            } else {
                crawlerLog.info('🔚 No more pages found');
            }
        }
    },

    async failedRequestHandler({ request, log: crawlerLog, session }, error) {
        crawlerLog.error(`❌ Request failed: ${request.url} - ${error.message}`);
        
        if (error.message.includes('CHALLENGE_DETECTED')) {
            crawlerLog.warning('🤖 Challenge detected, waiting before retry...');
            await new Promise(resolve => setTimeout(resolve, 10000));
            if (session) session.retire();
        }
    },
});

// ------------------------- RUN -------------------------
const startUrl = buildJobSearchUrl(keyword, location, posted_date);
log.info(`🎯 Starting URL: ${startUrl}`);

log.info(`Input parameters:`);
log.info(`  - Keyword: "${keyword}"`);
log.info(`  - Location: "${location || 'Not specified'}"`);
log.info(`  - Posted date filter: "${posted_date}"`);
log.info(`  - Results wanted: ${RESULTS_WANTED}`);
log.info(`  - Max retries: ${maxRequestRetries}`);
log.info(`  - Using proxy: ${proxyConf ? 'Yes' : 'No'}`);

try {
    await crawler.run([{ url: startUrl }]);
    
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