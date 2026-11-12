import { log } from 'apify';
import { Impit } from 'impit';

import { parseGoogleJobsPage } from './parse-google-jobs.js';

const GOOGLE_HOST = 'http://www.google.com';
const SEARCH_PATH = '/search';
const DEFAULT_PROFILES = [
    'chrome',
    'chrome142',
    'chrome151',
    'ios18',
    'chrome100',
    'firefox158',
    'okhttp4',
    'chrome136',
    'firefox135',
];
// The Jobs vertical through GOOGLE_SERP answers in 27-70 s (plain search: ~6 s). A 30 s timeout
// failed every attempt, so the request budget has to outlast the slow render.
const REQUEST_TIMEOUT_MS = 90_000;
const BLOCK_TEXT = /trouble accessing|unusual traffic|not a robot|before you continue|verify you are human/i;

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

function firstLine(message) {
    return String(message ?? '').replace(/\s+/g, ' ').slice(0, 180);
}

function backoff(attempt, retryAfter) {
    if (Number.isFinite(retryAfter) && retryAfter > 0) return Math.min(retryAfter, 15_000);
    return Math.min(600 * 2 ** attempt, 8_000) + Math.floor(Math.random() * 400);
}

function cleanQuery(value) {
    return String(value ?? '').replace(/\s+/g, ' ').trim();
}

function uniqueQueries(values) {
    const seen = new Set();
    const result = [];

    for (const value of values) {
        const query = cleanQuery(value);
        const key = query.toLowerCase();
        if (!query || seen.has(key)) continue;
        seen.add(key);
        result.push(query);
    }

    return result;
}

export function buildQueryVariants(keyword, location) {
    const base = cleanQuery(keyword);
    if (!base) return [];

    const place = cleanQuery(location);
    const variants = [
        place ? `${base} near ${place}` : base,
        `${base} jobs`,
        `remote ${base}`,
        `${base} remote`,
        `${base} hybrid`,
        `${base} onsite`,
        `${base} entry level`,
        `${base} senior`,
        `${base} contract`,
        `${base} part time`,
        `${base} intern`,
        `${base} new grad`,
        `${base} developer`,
        `${base} engineer`,
        place ? `${base} in ${place}` : '',
        place ? `${base} ${place} jobs` : '',
        `${base} python`,
        `${base} java`,
        `${base} full stack`,
        `${base} backend`,
        `${base} frontend`,
        `${base} data`,
        `${base} cloud`,
        `${base} security`,
        `${base} ai`,
        `${base} machine learning`,
        `${base} principal`,
        `${base} lead`,
        `${base} staff`,
        `${base} junior`,
        `${base} mid level`,
        `${base} linkedin`,
        `${base} indeed`,
        `${base} glassdoor`,
        `${base} work from home`,
        `${base} remote usa`,
        `${base} no experience`,
        `${base} full time`,
        `${base} temporary`,
        `${base} internship`,
        `${base} freelance`,
        `${base} hiring`,
        `${base} openings`,
        `${base} vacancies`,
        `${base} roles`,
        `${base} positions`,
    ];

    return uniqueQueries(variants);
}

export class GoogleJobsClient {
    constructor({
        proxyConfiguration,
        hl = 'en',
        gl = 'us',
        maxAttempts = 4,
        profiles = DEFAULT_PROFILES,
    }) {
        this.proxyConfiguration = proxyConfiguration;
        this.hl = hl;
        this.gl = gl;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.profiles = profiles.length > 0 ? profiles : DEFAULT_PROFILES;
        this.profileIndex = 0;
    }

    nextProfile() {
        const profile = this.profiles[this.profileIndex % this.profiles.length];
        this.profileIndex += 1;
        return profile;
    }

    async createClient(attempt) {
        const profile = this.nextProfile();
        const proxyUrl = this.proxyConfiguration
            ? await this.proxyConfiguration.newUrl(`google_jobs_${Date.now()}_${attempt}`)
            : undefined;

        return {
            client: new Impit({ browser: profile, proxyUrl, timeout: REQUEST_TIMEOUT_MS, vanillaFallback: true }),
            profile,
        };
    }

    buildUrl(query) {
        const url = new URL(`${GOOGLE_HOST}${SEARCH_PATH}`);
        url.searchParams.set('q', cleanQuery(query));
        url.searchParams.set('udm', '8');
        url.searchParams.set('hl', this.hl);
        url.searchParams.set('gl', this.gl);
        return url.toString();
    }

    async fetchPage(url, query) {
        let lastError;

        for (let attempt = 0; attempt < this.maxAttempts; attempt += 1) {
            const { client, profile } = await this.createClient(attempt);

            try {
                const response = await client.fetch(url);
                if (response.status === 429) {
                    const retryAfter = Number(response.headers.get('retry-after'));
                    const error = new Error('Google Jobs rate limited the request');
                    error.retryAfter = Number.isFinite(retryAfter) ? retryAfter * 1_000 : undefined;
                    throw error;
                }
                if (response.status >= 500) throw new Error(`Google Jobs returned HTTP ${response.status}`);
                if (!response.ok) throw new Error(`Google Jobs returned HTTP ${response.status}`);

                const html = await response.text();
                const visible = html
                    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
                    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
                    .replace(/<[^>]+>/g, ' ');

                if (response.url.includes('/sorry/') || BLOCK_TEXT.test(visible)) {
                    const error = new Error('Google Jobs served a blocked or consent page');
                    error.maxAttempts = 2;
                    throw error;
                }

                const jobs = parseGoogleJobsPage(html, { query, hl: this.hl, gl: this.gl });
                // About half of the Jobs answers come back as a plain SERP with no cards. Treating
                // that as an empty variant is faster than retrying it for another 60 s; the next
                // query variant is the retry.
                return { jobs, profile, url };
            } catch (error) {
                lastError = error;
                log.warning(`Search request attempt ${attempt + 1} failed (${profile}): ${firstLine(error.message)}`);
                // A timeout or a block page means the session is bad; one more try with a fresh
                // session is the useful limit, more attempts only burn the run budget.
                const attemptCap = error.name === 'TimeoutError' ? 2 : (error.maxAttempts ?? this.maxAttempts);
                if (attempt + 1 >= attemptCap) break;
                await sleep(backoff(attempt, error.retryAfter));
            }
        }

        throw lastError ?? new Error('Google Jobs search failed');
    }

    async search(query, { url } = {}) {
        return this.fetchPage(url || this.buildUrl(query), query);
    }
}
