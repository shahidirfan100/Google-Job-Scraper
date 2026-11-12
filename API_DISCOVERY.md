# Google Jobs API Discovery

Read this file before changing the request flow or debugging a failed run. It records the only verified Google Jobs path for this Actor and the checks that explain most failures.

## Target

This Actor collects the **Google Jobs** vertical (`udm=8`), not Google Careers. A Google Jobs URL looks like:

```text
https://www.google.com/search?q=software+engineer&udm=8&hl=en&gl=us
```

The legacy `ibp=htl;jobs` parameter redirects to the same vertical. The Actor normalizes it to `udm=8`.

## Selected request

| Item | Value |
|---|---|
| Endpoint | `http://www.google.com/search` |
| Query | `q=<keyword>`, `udm=8`, `hl=en`, `gl=us` |
| Method | `GET` |
| Proxy | Default `GOOGLESERP`; override with `apifyProxyGroups` or `proxyUrls` |
| Country | `gl` derives from the Location input (`src/markets.js`), default `us`; `hl=en` because the parser matches English labels |
| Transport | HTTP for `GOOGLESERP`; redirects are followed |
| Request timeout | 90 s. The Jobs vertical takes 27-70 s through `GOOGLE_SERP`; a 30 s timeout failed every request |
| Response marker | Title ends with `- Google Search`, page is about 1.0-1.2 MB |
| Job marker | `div[data-preview-id]` and matching `template#<preview-id>` |
| Jobs per request | 10 |
| Auth | None beyond the proxy |
| Bot gate | Google `/sorry` or consent page for direct and some proxy traffic |

The default and verified fast path is the `GOOGLESERP` group over HTTP. `DATACENTER` also worked in the user's run and is used when supplied. Direct traffic and some proxy groups get a blocked or consent page, so keep the default group or use one that works for your account. Custom proxy URLs are supported: set `useApifyProxy` to `false` and provide `proxyUrls`.

## Evidence matrix

| Candidate | Transport | Result | Decision |
|---|---|---|---|
| `google.com/search?...&udm=8` direct | HTTPS | 200, about 94 KB, `/sorry` or "trouble accessing" shell | Rejected |
| `ibp=htl;jobs` direct | HTTPS | Redirects to `udm=8`, then blocked | Rejected |
| `googlesearch` with RESIDENTIAL US/GB/DE | HTTPS | 200, about 94 KB blocked shell | Rejected |
| `google.com/search?...&udm=8` with GOOGLESERP | HTTP | 200, about 1.1 MB, 10 job cards and 10 detail templates | Selected for verification |
| `google.com/search?...&udm=8` with DATACENTER | HTTP/HTTPS | User-confirmed working, slower | Supported, not forced |
| `googlesearch` with GOOGLESERP | HTTPS | 400 or blocked | Rejected |
| `start=10` / `start=20` variants | HTTP | 200, about 210 KB, 0 cards | Rejected |
| `data-async-fc` replay to `/async/jobs` or `/async/search` | HTTP | 400 `Google query failed: Unsupported Google service or resource` | Rejected |
| `ibp=htl;jobs&htidocid=<docid>` | HTTP | Timeout or 0 cards | Rejected |

## Page structure

Each result has two parts:

1. A visible card: `div[data-preview-id="j..."]` containing `.tNxQIb` (title), `.wHYlTd` (company and location/date lines), salary, date, job type, and benefits.
2. A hidden detail template: `template#<preview-id>`. The template contains:
   - `data-encoded-docid` and `data-title` on the inner `div`.
   - Company in `.knLYHc`.
   - Header text with company, location, `via <source>`, relative date, salary, job type.
   - Apply links as `a[href^="/goto?url="]` with text `Apply on <source>`.
   - Job description, qualifications, benefits, and responsibilities.

One response contains 10 complete records. The Actor does not request a separate detail page for each job. The request budget is therefore measured in search requests, not jobs.

The template ID is exactly the card's `data-preview-id`. The card's `id` is the docid, usually ending in `==`. Do not strip the `j` prefix.

## Field mapping

| Output field | Source |
|---|---|
| `id` | `data-encoded-docid` or card `id` |
| `title` | card `.tNxQIb` or template `data-title` |
| `company` | first card `.wHYlTd` when it is not a date, or `.knLYHc` |
| `location` | card location line before `• via` |
| `salary` | currency range, K/M range, or labeled salary range in the card/template header |
| `job_type` | `Full-time`, `Part-time`, `Contract`, `Internship`, `Temporary`, `Freelance`, `Per diem` in the card/header |
| `date_posted` | relative date (`today`, `yesterday`, `N days ago`) or `DD-MMM` converted to ISO |
| `description_text` | cleaned template text after `Job description`, `Job highlights`, or `About the job` |
| `qualifications` | text between `Qualifications` and `Benefits`, `Responsibilities`, or `About the job` |
| `apply_url` | first `Apply on ...` link, resolved against `https://www.google.com` |
| `apply_options` | all apply links with their source labels |
| `url` | Google Jobs job URL built from `htidocid=<docid>` |
| `source` | text after `via` in the location line |

## Pagination

Google Jobs exposes 10 cards per request. The page includes an `<infinity-scrolling>` element and a `data-async-fc` token, but the token is for the job detail viewer, not a replayable next-page API. Replaying it returns HTTP 400. `start` and `num` do not produce a usable next page.

The Actor therefore fills the requested count with deterministic related-query variants:

```text
<keyword> near <location>
<keyword> jobs
remote <keyword>
<keyword> remote
<keyword> senior
<keyword> contract
<keyword> entry level
<keyword> in <location>
<keyword> python
<keyword> full stack
...
```

Each variant returns up to 10 jobs. The Actor deduplicates by docid and by `title|company|location`, and stops at `results_wanted`, the request budget, or when a whole batch of variants adds fewer than 2 new jobs each. `max_pages` is the starting request budget, not a page-number cap. When `results_wanted` is larger than `max_pages * 10`, the Actor raises the budget to `ceil(results_wanted / 5)` and logs the change. It fetches up to 5 variants concurrently, each with a fresh proxy session.

A 200-job run on Apify used 33 requests and finished in 103 seconds after the budget expanded from 10 to 40. A 100-job run used 15 requests and finished in 72 seconds.

## Header and profile behavior

Impit generates the browser headers. Do not set a manual `User-Agent`, `sec-ch-ua`, or `accept-language` on the search request. The Actor rotates these Impit profiles on failure:

```text
chrome, chrome142, chrome151, ios18, chrome100, firefox158, okhttp4, chrome136, firefox135
```

All of these produced 10 jobs in the live profile test. Profiles that failed in the same test and are not used by default: `chrome101`, `chrome104`, `chrome110`, `chrome125`, `chrome131`, `firefox`, `firefox144`, `okhttp3`, `okhttp5`. Failures were proxy/session-specific and are handled by rotation, not by hardcoding a different UA.

## Failure playbook

1. **Read the run log first.** Look for `blocked or consent page`, `HTTP 429`, `did not contain job cards`, or `no listings`.
2. **Blocked or consent page**
   - Confirm the configured proxy group is available. The default is `GOOGLESERP`.
   - Try `DATACENTER`, `RESIDENTIAL`, or a custom proxy if the default is blocked.
   - Retry with a fresh proxy session and the next Impit profile.
3. **HTTP 429**
   - Use the `Retry-After` header when present.
   - Reduce the request budget or wait; do not lower `results_wanted` for the user.
4. **0 cards but a large page**
   - The page layout changed. Check that cards still use `data-preview-id` and templates still use the same ID.
   - Run `npm test`; the parser fixture covers the current structure.
5. **0 cards and a small page**
   - The query returned no listings. The Actor treats this as an empty variant and continues.
6. **No proxy configured**
   - The run fails with a clear message. Google Jobs does not work from shared direct IPs.
7. **Changed template ID or card class**
   - The parser falls back to parsing every `template` with `data-encoded-docid`.
   - If both card and template parsing fail, save the page title and length in the warning, not the full response body.

## Failure log

### 2026-11-12 — every request timed out at 30 s

A run with `keyword=admin`, `target=20`, `max_requests=10`, proxy `GOOGLESERP` logged `Request timeout (30000 ms) exceeded` on all six requests and saved nothing.

Measured with the same proxy and profile:

| Request | Latency | Result |
|---|---|---|
| `udm=8` (Jobs) | 27-70 s over six probes | 200, 10 cards when the Jobs block is rendered |
| plain `search?q=` (no `udm`) | 5-6 s | 200, 0 cards |

The Jobs vertical is not broken; the proxy answers it slowly. The 30 s client timeout cut every attempt before the page arrived, and the profile rotation retried the same slow path four times. Fix: `REQUEST_TIMEOUT_MS = 90_000`, and a timeout stops after two attempts so a dead route cannot consume the run budget.

Two further observations from the same probes, both handled by the existing retry loop:

- About half the `udm=8` responses come back as a plain search page (200, 0 `data-preview-id`, no `htl;jobs`) even though the request carried `udm=8`. The next attempt with a fresh session and profile returns the Jobs page.
- `GOOGLE_SERP` answers non-Google hosts and non-Jobs Google services immediately with `Google query failed: ...` (HTTP 400). It also rejects `https://` with a 400 CONNECT, so keep the `http://` target.

Post-fix runs on the same proxy: 10/10 jobs in 3 requests, 13/20 in 5 requests (budget exhausted), 20/20 in 9 requests in 256 s locally, and 20/20 in 10 requests in 363 s on Apify build 3.0.12 (run `aUi6rBZPc7iBsA71s`). The cloud run spent five attempts on the first variant before that whole variant failed; that is the 0-card behaviour above, not a timeout.

### 2026-11-12 — slow runs: country index, retries, container migration

Measurements that explain most of the run time:

| Change | Evidence |
|---|---|
| `gl` from the location | `q=admin near Dubai&udm=8` returned 3 cards with `gl=us` and 10 cards with `gl=ae` |
| 5 concurrent variants | each request costs 30-70 s, so wall time is batch count × latency |
| No retry for a cardless page | about half the Jobs answers are a plain SERP; retrying one cost another 30-70 s for the same result, so the next variant is the retry |
| 2 attempts for a block page | the block is IP-based; a third and fourth attempt only burned 40-60 s |
| Progress checkpoint | Apify migrated one run to another host (`migrationCount: 1`); the actor restarted from zero, re-fetched finished variants, and wrote 88 dataset items for a reported 53. `src/progress.js` now stores saved counts, seen ids and the next variant index in the run's key-value store and continues after a migration |

Measured on the same input (`admin`, location `Dubai`, 100 jobs, `GOOGLESERP`):

| | Before | After |
|---|---|---|
| First jobs saved | 3 jobs at 65 s | 10 jobs at 75 s |
| Jobs per second | 0.12 (13 jobs in 108 s) | 0.20 (30 jobs in 151 s) |
| Timeouts | none (after the timeout fix) | none |
| Duplicate items after a migration | 88 items for 53 saved | checkpoint prevents it |

A 100-job run still needs 10 variant requests and about 5-9 minutes on this proxy; the target is request-bound, not client-bound.

## Local testing

The local runtime needs an Apify token to create the configured Apify Proxy URL:

```powershell
$env:APIFY_TOKEN = (npx apify auth token)
npm start
```

The default local input is `storage/key_value_stores/default/INPUT.json`. A root `INPUT.json` is not read by `Actor.getInput()`.

## Security

Do not log proxy credentials, cookies, authorization headers, or full response bodies. The Actor logs only request counts, saved counts, and the first line of recoverable errors.
