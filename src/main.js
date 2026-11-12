import { Actor, log } from 'apify';

import { buildQueryVariants, GoogleJobsClient } from './google-jobs-client.js';
import { resolveGl } from './markets.js';
import { restoreProgress, saveProgress } from './progress.js';

const DEFAULT_KEYWORD = 'software engineer';
const POSTED_DAYS = {
    today: 1,
    last3days: 3,
    last7days: 7,
    last14days: 14,
};

function readString(value) {
    return typeof value === 'string' ? value.trim() : '';
}

function integer(value, fallback, min) {
    const parsed = Number(value);
    if (!Number.isInteger(parsed)) return fallback;
    return Math.max(min, parsed);
}

function normalizePostedDate(value) {
    return value === 'anytime' || Object.hasOwn(POSTED_DAYS, value) ? value : 'anytime';
}

function isWithinPostedDate(datePosted, filter) {
    const days = POSTED_DAYS[filter];
    if (!days || !datePosted) return true;

    const timestamp = Date.parse(datePosted);
    return !Number.isFinite(timestamp) || Date.now() - timestamp <= days * 86_400_000;
}

function normalizeSearchUrl(rawUrl) {
    let url;
    try {
        url = new URL(rawUrl);
    } catch {
        throw new Error('startUrl must be a valid Google Jobs URL');
    }

    if (!['http:', 'https:'].includes(url.protocol)) {
        throw new Error('startUrl must use http or https');
    }
    if (!/(^|\.)google\.com$/i.test(url.hostname)) {
        throw new Error('startUrl must point to google.com');
    }
    if (!/^\/search\/?$/.test(url.pathname)) {
        throw new Error('startUrl must be a Google search results URL');
    }

    url.protocol = 'http:';
    url.searchParams.delete('ibp');
    url.searchParams.set('udm', '8');
    if (!url.searchParams.has('hl')) url.searchParams.set('hl', 'en');
    if (!url.searchParams.has('gl')) url.searchParams.set('gl', 'us');

    return url;
}

function resolveSearch(input) {
    const startUrl = readString(input.startUrl);

    if (startUrl) {
        const url = normalizeSearchUrl(startUrl);
        const keyword = readString(url.searchParams.get('q'))
            || readString(url.searchParams.get('htiq'))
            || '';
        const location = readString(url.searchParams.get('location'));

        return {
            mode: 'url',
            keyword,
            location,
            hl: readString(url.searchParams.get('hl')) || 'en',
            gl: readString(url.searchParams.get('gl')) || 'us',
            startUrl: url.toString(),
        };
    }

    return {
        mode: 'keyword',
        keyword: readString(input.keyword) || DEFAULT_KEYWORD,
        location: readString(input.location),
        hl: 'en',
        gl: resolveGl(readString(input.location)),
        startUrl: null,
    };
}

async function createProxyConfiguration(proxyInput) {
    const input = proxyInput && typeof proxyInput === 'object' ? proxyInput : {};
    const hasCustomUrls = Array.isArray(input.proxyUrls) && input.proxyUrls.length > 0;

    if (hasCustomUrls) return Actor.createProxyConfiguration(input);
    if (input.useApifyProxy === false) return undefined;

    const groups = Array.isArray(input.apifyProxyGroups) && input.apifyProxyGroups.length
        ? input.apifyProxyGroups
        : ['GOOGLESERP'];

    return Actor.createProxyConfiguration({
        ...input,
        useApifyProxy: true,
        apifyProxyGroups: groups,
    });
}

function jobSignature(job) {
    return `${job.title}|${job.company ?? ''}|${job.location ?? ''}`
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .trim();
}

function finalizeJob(job, context) {
    return {
        ...job,
        employment_type: job.job_type,
        source: job.source || 'Google Jobs',
        _source: 'google.com/search?udm=8',
        _fetchedAt: context.fetchedAt,
        _scrapedUrl: context.scrapedUrl.replace(/^http:/, 'https:'),
        _searchKeyword: context.keyword,
        _searchLocation: context.location,
        _postedDateFilter: context.postedDate,
        _searchQuery: context.query,
        _extractedFrom: 'google-jobs-card',
    };
}

await Actor.init();

let exitCode = 0;

try {
    const input = (await Actor.getInput()) ?? {};
    const search = resolveSearch(input);
    const resultsWanted = integer(input.results_wanted, 20, 1);
    const maxPages = integer(input.max_pages, 10, 1);
    const postedDate = normalizePostedDate(input.posted_date);
    const proxyConfiguration = await createProxyConfiguration(input.proxyConfiguration);
    // Requests spend 30-70 s each, so more in flight cuts wall time without more requests.
    const maxConcurrency = 5;
    const minimumPagesForTarget = Math.ceil(resultsWanted / 5);
    const requestBudget = Math.max(maxPages, minimumPagesForTarget);

    if (!proxyConfiguration) {
        log.warning('No proxy is configured. Google Jobs usually blocks direct requests from shared IPs.');
    }
    if (requestBudget !== maxPages) {
        log.warning(
            `max_pages=${maxPages} is too low for ${resultsWanted} jobs; using a ${requestBudget}-request budget.`,
        );
    }

    const client = new GoogleJobsClient({
        proxyConfiguration,
        hl: search.hl,
        gl: search.gl,
        maxAttempts: 4,
    });

    const variants = buildQueryVariants(search.keyword, search.location);
    const queries = search.startUrl
        ? [{ query: search.keyword, url: search.startUrl }, ...variants.map((query) => ({ query }))]
        : variants.map((query) => ({ query }));

    log.info(
        `Start | mode=${search.mode} | query="${search.keyword}" | location="${search.location || 'any'}" | `
        + `gl=${search.gl} | target=${resultsWanted} | max_requests=${requestBudget} | `
        + `concurrency=${maxConcurrency} | proxy=${proxyConfiguration ? 'enabled' : 'disabled'}`,
    );

    const runId = Actor.getEnv().actorRunId;
    const store = runId ? await Actor.openKeyValueStore() : null;
    const resumed = runId ? restoreProgress(await store.getValue('PROGRESS'), runId) : null;

    const seenIds = new Set(resumed?.seenIds ?? []);
    const seenSignatures = new Set(resumed?.seenSignatures ?? []);
    let saved = resumed?.saved ?? 0;
    let requests = resumed?.requests ?? 0;
    let filtered = 0;
    let duplicates = 0;
    let failedRequests = 0;
    let lowYieldStreak = 0;
    let stopReason = 'variants_exhausted';

    if (resumed) {
        log.info(`Resumed after container migration | saved=${saved} | requests=${requests} | next=${resumed.nextIndex}`);
    }

    for (let index = resumed?.nextIndex ?? 0; index < queries.length; index += maxConcurrency) {
        if (saved >= resultsWanted || requests >= requestBudget) break;

        const remaining = requestBudget - requests;
        const batchQueries = queries.slice(index, index + Math.min(maxConcurrency, remaining));
        requests += batchQueries.length;

        const outcomes = await Promise.allSettled(
            batchQueries.map((variant) => client.search(variant.query, { url: variant.url })),
        );

        for (let offset = 0; offset < outcomes.length; offset += 1) {
            if (saved >= resultsWanted) break;

            const outcome = outcomes[offset];
            const variant = batchQueries[offset];

            if (outcome.status === 'rejected') {
                failedRequests += 1;
                log.warning(
                    `Search failed for "${variant.query}": ${String(outcome.reason?.message).replace(/\s+/g, ' ').slice(0, 180)}`,
                );
                continue;
            }

            const result = outcome.value;
            if (result.jobs.length === 0) {
                lowYieldStreak += 1;
                continue;
            }

            const fetchedAt = new Date().toISOString();
            const batch = [];

            for (const job of result.jobs) {
                if (saved + batch.length >= resultsWanted) break;

                const signature = jobSignature(job);
                if (!job.id || seenIds.has(job.id) || seenSignatures.has(signature)) {
                    duplicates += 1;
                    continue;
                }

                if (!isWithinPostedDate(job.date_posted, postedDate)) {
                    filtered += 1;
                    continue;
                }

                seenIds.add(job.id);
                seenSignatures.add(signature);
                batch.push(finalizeJob(job, {
                    fetchedAt,
                    scrapedUrl: result.url,
                    keyword: search.keyword,
                    location: search.location,
                    postedDate,
                    query: variant.query,
                }));
            }

            if (batch.length === 0) {
                lowYieldStreak += 1;
                continue;
            }

            await Actor.pushData(batch);
            saved += batch.length;
            log.info(`Saved ${batch.length} jobs from "${variant.query}" | total ${saved}/${resultsWanted}`);
            lowYieldStreak = batch.length < 2 ? lowYieldStreak + 1 : 0;
            await saveProgress(store, runId, { nextIndex: index, saved, requests, seenIds, seenSignatures });
        }

        await saveProgress(store, runId, {
            nextIndex: index + batchQueries.length, saved, requests, seenIds, seenSignatures,
        });

        if (saved >= resultsWanted) {
            stopReason = 'results_wanted';
            break;
        }
        if (lowYieldStreak >= maxConcurrency) {
            stopReason = 'low_yield';
            break;
        }
        if (requests >= requestBudget) {
            stopReason = 'max_pages';
            break;
        }
    }

    if (saved < resultsWanted && requests >= requestBudget) stopReason = 'max_pages';

    if (saved === 0) {
        throw new Error(
            'Google Jobs returned no listings. Confirm that the GOOGLESERP proxy group is available and retry. '
            + 'If it is blocked, try DATACENTER, RESIDENTIAL, or a custom proxy.',
        );
    }

    if (saved < resultsWanted) {
        log.warning(
            `Saved ${saved} of ${resultsWanted} jobs (stop=${stopReason}). `
            + 'Raise max_pages or broaden the keyword to reach the target.',
        );
    }

    log.info(
        `Done | saved=${saved} | requests=${requests} | failed=${failedRequests} | `
        + `filtered=${filtered} | duplicates=${duplicates} | stop=${stopReason}`,
    );
} catch (error) {
    log.error(`Run failed: ${String(error.message).replace(/\s+/g, ' ').slice(0, 240)}`);
    exitCode = 1;
}

await Actor.exit({ exitCode });
