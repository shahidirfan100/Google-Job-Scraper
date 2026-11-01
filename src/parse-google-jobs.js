import * as cheerio from 'cheerio';

const DATE_TAG = /^(?:today|yesterday|\d{1,2}-[A-Za-z]{3,}|\d+\s+(?:minute|hour|day|week|month)s?\s+ago)$/i;
const RELATIVE_DATE = /(\d+)\s+(minute|hour|day|week|month)s?\s+ago|today|yesterday/i;
const SALARY_PATTERNS = [
    /(?:\$|€|£)\s?\d[\d.,]*(?:\s?[KMB])?(?:\s*[–-]\s*(?:\$|€|£)?\s?\d[\d.,]*(?:\s?[KMB])?)?/i,
    /\d[\d.,]*\s?[KMB]\s*[–-]\s*\d[\d.,]*\s?[KMB](?:\s*(?:a year|per year|\/year))?/i,
    /\d[\d.,]*\s*[–-]\s*\d[\d.,]*\s*(?:a year|per year|\/year|a month|\/month|an hour|\/hour|a day|\/day)/i,
];
const EMPLOYMENT_TYPE = /(full[- ]?time|part[- ]?time|contract(?:or)?|intern(?:ship)?|temporary|freelance|per diem)(?![a-z])/i;

const MONTHS = {
    jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
    jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
    mag: 4, mei: 4, agu: 7, okt: 9, des: 11,
};

function cleanText(value) {
    return String(value ?? '').replace(/\s+/g, ' ').trim();
}

function cleanBlockText(value) {
    return String(value ?? '')
        .replace(/\u00a0/g, ' ')
        .replace(/[ \t]+/g, ' ')
        .replace(/\s*\n\s*/g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim();
}

function escapeRegex(value) {
    return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function stripScripts(html) {
    return String(html ?? '')
        .replace(/<script[\s\S]*?<\/script>/gi, ' ')
        .replace(/<style[\s\S]*?<\/style>/gi, ' ')
        .replace(/<br\s*\/?>/gi, '\n')
        .replace(/<\/(?:li|tr)>/gi, '\n')
        .replace(/<\/(?:p|div|h[1-6]|ul|ol|section|article)>/gi, '\n\n');
}

function normalizeEmploymentType(value) {
    const match = cleanText(value).match(EMPLOYMENT_TYPE);
    if (!match) return null;

    const label = match[1].toLowerCase().replace(/[-\s]+/g, ' ');
    if (label.startsWith('full')) return 'Full time';
    if (label.startsWith('part')) return 'Part time';
    if (label.startsWith('intern')) return 'Internship';
    if (label.startsWith('contract')) return 'Contract';
    if (label.startsWith('per diem')) return 'Per diem';
    return label.charAt(0).toUpperCase() + label.slice(1);
}

function parseDateText(value) {
    const text = cleanText(value);
    if (!text) return { text: null, iso: null };

    const now = Date.now();
    if (/^today$/i.test(text)) return { text, iso: new Date(now).toISOString() };
    if (/^yesterday$/i.test(text)) return { text, iso: new Date(now - 86_400_000).toISOString() };

    const relative = text.match(RELATIVE_DATE);
    if (relative) {
        const units = { minute: 60_000, hour: 3_600_000, day: 86_400_000, week: 604_800_000, month: 2_592_000_000 };
        const amount = Number(relative[1]);
        const unit = relative[2]?.toLowerCase();
        if (units[unit]) return { text, iso: new Date(now - amount * units[unit]).toISOString() };
    }

    const dayMonth = text.match(/^(\d{1,2})-([A-Za-z]{3,})$/);
    if (dayMonth && MONTHS[dayMonth[2].toLowerCase()] !== undefined) {
        const year = new Date(now).getUTCFullYear();
        const date = new Date(Date.UTC(year, MONTHS[dayMonth[2].toLowerCase()], Number(dayMonth[1])));
        if (date.getTime() > now) date.setUTCFullYear(year - 1);
        return { text, iso: date.toISOString() };
    }

    return { text, iso: null };
}

function extractSalary(text) {
    const value = cleanText(text);
    for (const pattern of SALARY_PATTERNS) {
        const match = value.match(pattern);
        if (match) return cleanText(match[0]);
    }
    return null;
}

function extractCurrencyRange(text) {
    const match = cleanText(text).match(
        /(?:\$|€|£)\s?\d[\d.,]*(?:\s?[KMB])?\s*[–-]\s*(?:\$|€|£)\s?\d[\d.,]*(?:\s?[KMB])?/i,
    );
    return match ? cleanText(match[0]) : null;
}

function extractLabeledSalary(text) {
    const value = cleanText(text);
    const match = value.match(/(\d[\d.,]*\s*[–-]\s*\d[\d.,]*)/);
    if (!match) return null;

    const index = match.index ?? 0;
    const nearby = value.slice(Math.max(0, index - 140), index + match[0].length + 180);
    return /salary|compensation|pay range/i.test(nearby) ? cleanText(match[0]) : null;
}

function extractApplyLinks($, root) {
    const links = [];

    $(root).find('a[href]').each((index, element) => {
        const text = cleanText($(element).text());
        const href = $(element).attr('href');
        if (!href || !/apply/i.test(text)) return;

        const url = href.startsWith('http') ? href : `https://www.google.com${href}`;
        const source = cleanText(text.replace(/^apply (?:directly )?on\s+/i, '')) || null;
        if (!links.some((link) => link.url === url)) links.push({ source, url });
    });

    return links;
}

function extractSections(text, applyLinks) {
    let body = cleanBlockText(text);

    const descriptionStart = body.search(/(?:job description|about the job)/i);
    const highlightsStart = body.search(/job highlights/i);
    const start = descriptionStart !== -1 ? descriptionStart : highlightsStart;

    if (start !== -1) {
        body = body.slice(start).replace(/^\s*(?:job description|about the job|job highlights)\s*/i, '');
    }

    body = body.replace(/identified by google from the original job post/gi, ' ');
    body = body.replace(/\b\d+ more items?\(s\)/gi, ' ');
    body = body.replace(/\b(?:more job highlights|show full description|report this listing|create a job alert|create alert)\b/gi, ' ');
    for (const link of applyLinks) {
        if (link.source) {
            body = body.replace(new RegExp(`apply (?:directly )?on\\s+${escapeRegex(link.source)}`, 'gi'), ' ');
        }
    }
    body = body.replace(/^\s*(?:qualifications|benefits|responsibilities|additional information)\s*/i, '');
    body = cleanBlockText(body);

    const qualificationsStart = body.search(/\bqualifications\b/i);
    let qualifications = null;
    if (qualificationsStart !== -1) {
        let value = body.slice(qualificationsStart).replace(/^\s*qualifications\s*:?\s*/i, '');
        const end = value.search(/\b(?:benefits|about the job|responsibilities|job description|additional information)\b/i);
        if (end > 0) value = value.slice(0, end);
        qualifications = cleanBlockText(value) || null;
    }

    const benefitsStart = body.search(/\bbenefits\b/i);
    let benefits = null;
    if (benefitsStart !== -1) {
        let value = body.slice(benefitsStart).replace(/^\s*benefits\s*:?\s*/i, '');
        const end = value.search(/\b(?:responsibilities|about the job|job description|additional information|apply)\b/i);
        if (end > 0) value = value.slice(0, end);
        benefits = cleanBlockText(value) || null;
    }

    return {
        description: body,
        qualifications,
        benefits,
    };
}

function parseTemplate($, templateId, card, context) {
    const template = templateId ? $(`template#${templateId}`) : $();
    const templateHtml = template.html() || '';
    if (!templateHtml) return null;

    const $t = cheerio.load(`<div id="job-root">${stripScripts(templateHtml)}</div>`);
    const root = $t('#job-root');
    const title = cleanText($t('[data-title]').attr('data-title'))
        || cleanText(card?.find('.tNxQIb').first().text());
    if (!title) return null;

    const docid = cleanText($t('[data-encoded-docid]').attr('data-encoded-docid'))
        || cleanText(card?.attr('id'))
        || templateId;
    const meta = card
        ? card.find('.wHYlTd').map((index, element) => cleanText($(element).text())).get().filter(Boolean)
        : [];
    const headerText = cleanText(root.text());
    const headerLine = cleanText(headerText.match(/([^.]{3,160}?·[^.]{3,160}?via\s+[^.]{2,80})/i)?.[1] || '');
    const locationLine = meta.find((value) => value.includes('•')) || headerLine;
    const companyFromCard = meta[0] && !DATE_TAG.test(meta[0]) ? meta[0] : null;
    const companyFromTemplate = cleanText($t('.knLYHc').first().text());
    const company = companyFromCard || (companyFromTemplate && !DATE_TAG.test(companyFromTemplate) ? companyFromTemplate : null);
    const location = cleanText(locationLine.split('•')[0].split('·')[0]) || null;
    const source = cleanText(locationLine.match(/via\s+(.+)$/i)?.[1] || '') || null;
    const cardText = card ? cleanText(card.text()) : '';
    const templateText = cleanBlockText(root.text());
    const templateHeader = cleanText(templateText).slice(0, 700);
    const visibleText = `${cardText} ${templateHeader}`;

    const dateText = visibleText.match(/(?:\d+\s+(?:minute|hour|day|week|month)s?\s+ago|today|yesterday)/i)?.[0]
        || meta.find((value) => DATE_TAG.test(value))
        || null;
    const date = parseDateText(dateText);
    const applyLinks = extractApplyLinks($t, '#job-root');
    const sections = extractSections(templateText, applyLinks);
    const salary = extractSalary(cardText)
        || extractSalary(templateHeader)
        || extractCurrencyRange(templateText)
        || extractLabeledSalary(templateText);
    const jobType = normalizeEmploymentType(cardText) || normalizeEmploymentType(templateHeader);
    const isRemote = /remote|work from home/i.test(visibleText);
    const cleanSource = source && !/^(?:sign in|signin|google|search|jobs)$/i.test(source) ? source : null;

    return {
        id: docid || null,
        title,
        company,
        location,
        salary,
        job_type: jobType,
        employment_type: jobType,
        date_posted: date.iso,
        date_posted_text: date.text,
        description_text: sections.description.slice(0, 30_000),
        qualifications: sections.qualifications,
        benefits: sections.benefits,
        is_remote: isRemote,
        apply_url: applyLinks[0]?.url ?? null,
        apply_options: applyLinks,
        source: cleanSource,
        url: docid
            ? `https://www.google.com/search?ibp=htl;jobs&q=${encodeURIComponent(context.query ?? '')}`
                + `&htidocid=${encodeURIComponent(docid)}&hl=${encodeURIComponent(context.hl ?? 'en')}`
                + `&gl=${encodeURIComponent(context.gl ?? 'us')}`
            : null,
    };
}

function parseCard($, element, context) {
    const card = $(element);
    const previewId = cleanText(card.attr('data-preview-id'));
    if (!previewId) return null;

    return parseTemplate($, previewId, card, context);
}

function parseTemplateNodes($, context) {
    const jobs = [];

    $('template').each((index, element) => {
        const templateId = cleanText($(element).attr('id'));
        const job = parseTemplate($, templateId, null, context);
        if (job) jobs.push(job);
    });

    return jobs;
}

export function parseGoogleJobsPage(html, context = {}) {
    const $ = cheerio.load(html);
    const cards = $('[data-preview-id]');
    let jobs = [];

    if (cards.length > 0) {
        jobs = cards.map((index, element) => parseCard($, element, context)).get().filter(Boolean);
    }

    if (jobs.length === 0) {
        jobs = parseTemplateNodes($, context);
    }

    return jobs;
}
