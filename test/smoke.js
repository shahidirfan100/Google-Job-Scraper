import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { buildQueryVariants } from '../src/google-jobs-client.js';
import { resolveGl } from '../src/markets.js';
import { parseGoogleJobsPage } from '../src/parse-google-jobs.js';
import { restoreProgress } from '../src/progress.js';

const sampleHtml = `
<div data-preview-id="jABC123" id="ABC123==">
  <h3 class="tNxQIb">Senior Software Engineer</h3>
  <div class="wHYlTd">Acme Corp</div>
  <div class="wHYlTd">Remote &bull; via Indeed</div>
  <div class="wHYlTd">2 days ago</div>
</div>
<template id="jABC123">
  <div data-encoded-docid="ABC123==" data-title="Senior Software Engineer">
    <div class="knLYHc">Acme Corp</div>
    <div>Acme Corp &middot; Remote &middot; via Indeed</div>
    <div>2 days ago $120,000 - $150,000 a year Full-time</div>
    <a href="/goto?url=token123">Apply on Indeed</a>
    <div>Job highlights</div>
    <div>Build reliable services.</div>
    <div>Qualifications</div>
    <div>Five years of experience.</div>
  </div>
</template>`;

test('parses a Google Jobs card and detail template', () => {
    const jobs = parseGoogleJobsPage(sampleHtml, { query: 'software engineer', hl: 'en', gl: 'us' });
    assert.equal(jobs.length, 1);

    const job = jobs[0];
    assert.equal(job.id, 'ABC123==');
    assert.equal(job.title, 'Senior Software Engineer');
    assert.equal(job.company, 'Acme Corp');
    assert.equal(job.location, 'Remote');
    assert.equal(job.source, 'Indeed');
    assert.equal(job.salary, '$120,000 - $150,000');
    assert.equal(job.job_type, 'Full time');
    assert.equal(job.date_posted_text, '2 days ago');
    assert.ok(job.date_posted);
    assert.match(job.apply_url, /goto\?url=token123/);
    assert.equal(job.apply_options.length, 1);
    assert.match(job.description_text, /Build reliable services/);
    assert.match(job.qualifications, /Five years of experience/);
    assert.match(job.url, /htidocid=ABC123%3D%3D/);
});

test('builds unique, relevant Google Jobs query variants', () => {
    const variants = buildQueryVariants('software engineer', 'New York, NY');
    assert.equal(variants[0], 'software engineer near New York, NY');
    assert.ok(variants.includes('remote software engineer'));
    assert.ok(variants.includes('software engineer entry level'));
    assert.equal(new Set(variants.map((value) => value.toLowerCase())).size, variants.length);
});

test('maps the location to the Google country index', () => {
    assert.equal(resolveGl('Dubai'), 'ae');
    assert.equal(resolveGl('Abu Dhabi, UAE'), 'ae');
    assert.equal(resolveGl('Berlin, Germany'), 'de');
    assert.equal(resolveGl('New York, NY'), 'us');
    assert.equal(resolveGl('London'), 'gb');
    assert.equal(resolveGl(''), 'us');
    assert.equal(resolveGl('Somewhere Unlisted'), 'us');
});

test('resumes only a checkpoint written by the same run', () => {
    const checkpoint = { runId: 'run1', nextIndex: 5, saved: 12, requests: 5, seenIds: ['a'], seenSignatures: ['x'] };
    const resumed = restoreProgress(checkpoint, 'run1');
    assert.equal(resumed.nextIndex, 5);
    assert.equal(resumed.saved, 12);
    assert.deepEqual(resumed.seenIds, ['a']);

    assert.equal(restoreProgress(checkpoint, 'run2'), null);
    assert.equal(restoreProgress(checkpoint, undefined), null);
    assert.equal(restoreProgress(null, 'run1'), null);
});

test('keeps one prefill and no removed controls in the input schema', () => {
    const schema = JSON.parse(readFileSync(new URL('../.actor/input_schema.json', import.meta.url), 'utf8'));
    const prefilled = Object.entries(schema.properties)
        .filter(([, value]) => Object.hasOwn(value, 'prefill'))
        .map(([key]) => key);

    assert.deepEqual(prefilled, ['keyword']);
    assert.equal(schema.properties.keyword.default, schema.properties.keyword.prefill);
    assert.equal(Object.hasOwn(schema.properties, 'maxRequestRetries'), false);
    assert.equal(Object.hasOwn(schema.properties, 'requestDelay'), false);
    assert.equal(Object.hasOwn(schema.properties.results_wanted, 'maximum'), false);
    assert.equal(Object.hasOwn(schema.properties.max_pages, 'maximum'), false);
    assert.equal(Object.hasOwn(schema.properties, 'collectDetails'), false);
    assert.equal(schema.properties.results_wanted.default, 20);
    assert.equal(schema.properties.max_pages.default, 10);
    assert.deepEqual(schema.properties.proxyConfiguration.default, {
        useApifyProxy: true,
        apifyProxyGroups: ['GOOGLESERP'],
    });
});
