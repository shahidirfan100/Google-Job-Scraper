// Google's `gl` picks the country index that answers the query. Hardcoding `us` returned 3 cards
// for "admin near Dubai"; `gl=ae` returned 10. `hl` stays `en` because the parser matches English
// date and section labels, so the market table only maps to a country.
const MARKETS = {
    us: 'united states|usa|new york|nyc|san francisco|seattle|austin|boston|chicago|los angeles|denver|atlanta'
        + '|dallas|miami|houston|phoenix|philadelphia|san diego|washington dc',
    gb: 'united kingdom|uk|england|scotland|wales|london|manchester|birmingham|edinburgh|leeds|glasgow|bristol|liverpool',
    ie: 'ireland|dublin|cork|galway',
    ca: 'canada|toronto|vancouver|montreal|calgary|ottawa|edmonton|quebec',
    au: 'australia|sydney|melbourne|brisbane|perth|adelaide|canberra',
    nz: 'new zealand|auckland|wellington|christchurch',
    ae: 'united arab emirates|uae|dubai|abu dhabi|sharjah|ajman',
    sa: 'saudi arabia|riyadh|jeddah|dammam|khobar',
    qa: 'qatar|doha',
    kw: 'kuwait|kuwait city',
    bh: 'bahrain|manama',
    om: 'oman|muscat',
    eg: 'egypt|cairo|alexandria',
    ma: 'morocco|casablanca|rabat',
    za: 'south africa|johannesburg|cape town|durban|pretoria',
    ng: 'nigeria|lagos|abuja',
    ke: 'kenya|nairobi',
    il: 'israel|tel aviv|jerusalem|haifa',
    tr: 'turkey|istanbul|ankara|izmir',
    de: 'germany|berlin|munich|münchen|hamburg|frankfurt|cologne|köln|stuttgart|düsseldorf|leipzig',
    at: 'austria|vienna|graz|linz',
    ch: 'switzerland|zurich|zürich|geneva|basel|bern',
    fr: 'france|paris|lyon|marseille|toulouse|nice|nantes',
    be: 'belgium|brussels|antwerp|ghent',
    nl: 'netherlands|amsterdam|rotterdam|the hague|utrecht|eindhoven',
    lu: 'luxembourg',
    es: 'spain|madrid|barcelona|valencia|seville|bilbao|malaga',
    pt: 'portugal|lisbon|lisboa|porto',
    it: 'italy|milan|milano|rome|roma|turin|torino|naples|napoli|bologna',
    gr: 'greece|athens|thessaloniki',
    se: 'sweden|stockholm|gothenburg|malmo',
    no: 'norway|oslo|bergen',
    dk: 'denmark|copenhagen|aarhus',
    fi: 'finland|helsinki|tampere',
    pl: 'poland|warsaw|warszawa|krakow|kraków|wroclaw|gdansk',
    cz: 'czechia|czech republic|prague|praha|brno',
    sk: 'slovakia|bratislava',
    hu: 'hungary|budapest',
    ro: 'romania|bucharest|cluj',
    bg: 'bulgaria|sofia',
    hr: 'croatia|zagreb',
    rs: 'serbia|belgrade',
    si: 'slovenia|ljubljana',
    ua: 'ukraine|kyiv|kiev|lviv',
    ru: 'russia|moscow|saint petersburg',
    in: 'india|bangalore|bengaluru|mumbai|delhi|new delhi|hyderabad|chennai|pune|noida|gurgaon|gurugram|kolkata|ahmedabad',
    pk: 'pakistan|karachi|lahore|islamabad|rawalpindi',
    bd: 'bangladesh|dhaka|chittagong',
    lk: 'sri lanka|colombo',
    np: 'nepal|kathmandu',
    sg: 'singapore',
    my: 'malaysia|kuala lumpur|penang|johor',
    id: 'indonesia|jakarta|surabaya|bandung',
    th: 'thailand|bangkok|chiang mai',
    vn: 'vietnam|hanoi|ho chi minh|saigon|da nang',
    ph: 'philippines|manila|cebu|makati|taguig',
    jp: 'japan|tokyo|osaka|kyoto|yokohama',
    kr: 'south korea|korea|seoul|busan',
    cn: 'china|beijing|shanghai|shenzhen|guangzhou|hangzhou',
    hk: 'hong kong',
    tw: 'taiwan|taipei|kaohsiung',
    mx: 'mexico|mexico city|guadalajara|monterrey',
    br: 'brazil|sao paulo|são paulo|rio de janeiro|brasilia',
    ar: 'argentina|buenos aires|cordoba',
    cl: 'chile|santiago',
    co: 'colombia|bogota|bogotá|medellin|medellín',
    pe: 'peru|lima',
    uy: 'uruguay|montevideo',
};

const BY_NAME = new Map();
const MULTI_WORD = [];
for (const [gl, names] of Object.entries(MARKETS)) {
    for (const name of names.split('|')) {
        BY_NAME.set(name, gl);
        if (name.includes(' ')) MULTI_WORD.push([name, gl]);
    }
}

export function resolveGl(location) {
    const text = String(location ?? '').toLowerCase().replace(/[^\p{L}\s,-]/gu, ' ').replace(/\s+/g, ' ').trim();
    if (!text) return 'us';
    if (BY_NAME.has(text)) return BY_NAME.get(text);

    for (const part of text.split(/[,\s]+/)) {
        if (BY_NAME.has(part)) return BY_NAME.get(part);
    }
    for (const [name, gl] of MULTI_WORD) {
        if (text.includes(name)) return gl;
    }

    // shortcut: unknown place falls back to the US index; add the name to MARKETS when it matters.
    return 'us';
}
