const express = require('express');
const path = require('path');
const http = require('http');
const socketIO = require('socket.io');
const https = require('https');
const app = express();
const server = http.createServer(app);
const io = socketIO(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});
const PORT = process.env.PORT || 3000;
const localStations = require('./public/stations.json');
const localStationData = require('./public/stations-data.json');
const localTimetables = require('./public/timetables.json');
const localTrainsTracking = require('./public/trains-tracking.json');

// TfL API Configuration
const TFL_API_BASE = 'https://api.tfl.gov.uk';
// Using public API without authentication, adding user agent for better compatibility
const TFL_OPTIONS = {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
  }
};

// Strict rate limiting with serialized requests
let lastApiCall = 0;
const API_RATE_LIMIT = 1000; // 1000ms (increased to prevent 429 errors with parallel operations)
const MAX_API_RETRY = 3;
const API_RETRY_DELAY = 2000;
const API_REQUEST_TIMEOUT = 10000;
let lastQueuePromise = Promise.resolve();
const inFlightRequests = new Map();
let retryAfterUntil = 0; // Track rate limit backoff until timestamp

function queueApiRequest(path, options = {}) {
  if (inFlightRequests.has(path)) {
    return inFlightRequests.get(path);
  }

  const requestPromise = lastQueuePromise = lastQueuePromise.catch(() => {}).then(async () => {
    const result = await executeApiRequest(path);
    return result;
  });

  const trackedPromise = requestPromise.then(
    (result) => {
      inFlightRequests.delete(path);
      return result;
    },
    (error) => {
      inFlightRequests.delete(path);
      throw error;
    }
  );

  inFlightRequests.set(path, trackedPromise);
  trackedPromise.catch(() => {});

  return trackedPromise;
}

function makeApiRequest(path, options = {}) {
  return queueApiRequest(path, options);
}

async function executeApiRequest(path, attempt = 0) {
  const now = Date.now();
  
  // Handle global rate limit backoff (when we receive 429)
  if (now < retryAfterUntil) {
    const backoffDelay = retryAfterUntil - now;
    console.warn(`[${new Date().toLocaleTimeString()}] ⏸️ Global rate limit backoff for ${backoffDelay}ms`);
    await new Promise(res => setTimeout(res, backoffDelay));
  }
  
  // Add delay to respect rate limits between requests
  const delay = Math.max(0, API_RATE_LIMIT - (now - lastApiCall));
  if (delay > 0) {
    await new Promise(res => setTimeout(res, delay));
  }
  lastApiCall = Date.now();
  const url = `${TFL_API_BASE}${path}`;
  console.log(`[${new Date().toLocaleTimeString()}] API Request (attempt ${attempt + 1}): ${path}`);

  const doRequest = () => new Promise((resolve, reject) => {
    const req = https.get(url, TFL_OPTIONS, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode === 200) {
          try {
            const parsed = JSON.parse(data);
            console.log(`[${new Date().toLocaleTimeString()}] ✓ Success: ${path} (${Array.isArray(parsed) ? parsed.length : 'object'} items)`);
            // Reset backoff on success
            retryAfterUntil = 0;
            resolve(parsed);
          } catch (parseError) {
            const error = new Error('Invalid JSON response from TfL API');
            error.isRetryable = true;
            reject(error);
          }
          return;
        }

        const message = `API returned status ${res.statusCode}`;
        const error = new Error(message);
        error.statusCode = res.statusCode;
        error.isRetryable = res.statusCode === 429 || res.statusCode >= 500;
        
        // Handle Retry-After header for 429 responses
        if (res.statusCode === 429) {
          const retryAfter = res.headers['retry-after'];
          if (retryAfter) {
            const backoffMs = isNaN(retryAfter) ? new Date(retryAfter).getTime() - Date.now() : parseInt(retryAfter) * 1000;
            retryAfterUntil = Math.max(retryAfterUntil, Date.now() + backoffMs);
            console.error(`[${new Date().toLocaleTimeString()}] 🚫 Rate limit hit! Backing off until ${new Date(retryAfterUntil).toLocaleTimeString()}`);
          }
        }
        
        console.log(`[${new Date().toLocaleTimeString()}] ✗ Failed: ${path} (${message})`);
        reject(error);
      });
    });

    req.on('error', (e) => {
      const error = new Error(`Request error for ${path}: ${e.message}`);
      error.isRetryable = true;
      reject(error);
    });

    req.setTimeout(API_REQUEST_TIMEOUT, () => {
      const timeoutError = new Error('Request timed out');
      timeoutError.isRetryable = true;
      req.destroy(timeoutError);
    });
  });

  try {
    return await doRequest();
  } catch (error) {
    if (attempt < MAX_API_RETRY && (error.isRetryable || error.statusCode === 429 || error.statusCode >= 500)) {
      const retryDelay = error.statusCode === 429 
        ? Math.max(2000, API_RETRY_DELAY * Math.pow(2, attempt)) 
        : API_RETRY_DELAY * Math.pow(2, attempt);
      console.warn(`[${new Date().toLocaleTimeString()}] ⚠️ Retrying ${path} after ${retryDelay}ms (attempt ${attempt + 1}/${MAX_API_RETRY})`);
      await new Promise(res => setTimeout(res, retryDelay));
      return executeApiRequest(path, attempt + 1);
    }
    throw error;
  }
}

// Cache for API data
let cachedLines = null;
let cachedLineStatuses = {};
let cachedAllLineStatuses = null;
let cachedAllArrivals = null;
let cachedStopSearches = {};
let cachedJourneyPlans = {};
let cachedStationLookup = {};
let lastLinesUpdate = 0;
let lastLineStatusUpdate = {};
let lastAllLineStatusesUpdate = 0;
let lastAllArrivalsUpdate = 0;
let lastStopSearchUpdate = {};
let lastJourneyPlanUpdate = {};
const CACHE_DURATION = 600000; // 10 minutes (increased from 5 to reduce API calls)
const LINE_STATUS_CACHE_DURATION = 120000; // 2 minutes (increased from 1 minute)
const STOP_SEARCH_CACHE_DURATION = 600000; // 10 minutes (unchanged - already 5 minutes)
const ALL_ARRIVALS_CACHE_DURATION = 30000; // 30 seconds (increased from 15s)
const JOURNEY_PLAN_CACHE_DURATION = 120000; // 2 minutes

async function fetchAllLineStatuses(forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && cachedAllLineStatuses && (now - lastAllLineStatusesUpdate) < LINE_STATUS_CACHE_DURATION) {
    return cachedAllLineStatuses;
  }

  try {
    const data = await makeApiRequest('/Line/Mode/tube/Status');
    if (Array.isArray(data) && data.length > 0) {
      const statusMap = {};
      data.forEach(item => {
        const lineId = (item.id || item.lineId || item.name || '').toString().toLowerCase().replace(/\s+/g, '-');
        if (lineId) {
          statusMap[lineId] = item;
          cachedLineStatuses[lineId] = item;
          lastLineStatusUpdate[lineId] = now;
        }
      });
      cachedAllLineStatuses = statusMap;
    } else if (data && typeof data === 'object') {
      cachedAllLineStatuses = data;
      Object.entries(data).forEach(([lineId, status]) => {
        cachedLineStatuses[lineId] = status;
        lastLineStatusUpdate[lineId] = now;
      });
    }

    lastAllLineStatusesUpdate = now;
    return cachedAllLineStatuses;
  } catch (error) {
    console.warn('Batch line status fetch failed, falling back to individual line requests:', error.message);
  }

  // Fallback: serialize individual line requests instead of parallel to avoid rate limiting
  const fallbackLineIds = Array.from(new Set(Object.values(lineNameToId)));
  const statusMap = {};
  
  for (const lineId of fallbackLineIds) {
    try {
      const status = await makeApiRequest(`/Line/${lineId}/Status`);
      statusMap[lineId] = status;
      cachedLineStatuses[lineId] = status;
      lastLineStatusUpdate[lineId] = now;
      // Add delay between requests to avoid rate limiting
      await new Promise(res => setTimeout(res, 300));
    } catch (error) {
      console.warn(`Fallback status request failed for ${lineId}:`, error.message);
    }
  }

  cachedAllLineStatuses = statusMap;
  lastAllLineStatusesUpdate = now;
  return cachedAllLineStatuses;
}

async function fetchAllArrivals(forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && cachedAllArrivals && (now - lastAllArrivalsUpdate) < ALL_ARRIVALS_CACHE_DURATION) {
    return cachedAllArrivals;
  }

  const lineIds = ['bakerloo','central','circle','district','hammersmith-city','jubilee','metropolitan','northern','piccadilly','victoria','waterloo-city'];
  const allArrivals = [];

  // Serialize requests to avoid rate limiting - fetch one line at a time with delays
  for (const lineId of lineIds) {
    try {
      const arrivals = await makeApiRequest(`/Line/${lineId}/Arrivals`);
      if (Array.isArray(arrivals)) {
        allArrivals.push(...arrivals);
      }
      // Add delay between consecutive line requests to avoid rate limiting
      await new Promise(res => setTimeout(res, 500));
    } catch (error) {
      console.warn(`Arrival request failed for ${lineId}:`, error.message);
    }
  }

  cachedAllArrivals = allArrivals;
  lastAllArrivalsUpdate = now;
  return cachedAllArrivals;
}

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Line color mapping
const lineColorMap = {
  'Bakerloo': '#B36305',
  'Central': '#E32017',
  'Circle': '#FFD300',
  'District': '#00782A',
  'Hammersmith': '#F3A9BB',
  'Jubilee': '#A0A5A9',
  'Metropolitan': '#9B0056',
  'Northern': '#000000',
  'Piccadilly': '#003688',
  'Victoria': '#0098D4',
  'Waterloo': '#95CDBA'
};

function getLineColor(lineName) {
  if (!lineName) return '#667eea';
  const normalized = lineName.toLowerCase();
  for (const [line, color] of Object.entries(lineColorMap)) {
    if (normalized.includes(line.toLowerCase())) {
      return color;
    }
  }
  return '#667eea';
}

// TfL line name mapping - comprehensive mapping for all tube lines
const tflLineNameMap = {
  'waterloo-city': 'Waterloo',
  'hammersmith-city': 'Hammersmith',
  'piccadilly': 'Piccadilly',
  'victoria': 'Victoria',
  'metropolitan': 'Metropolitan',
  'district': 'District',
  'central': 'Central',
  'bakerloo': 'Bakerloo',
  'circle': 'Circle',
  'northern': 'Northern',
  'jubilee': 'Jubilee',
  'dlr': 'DLR',
  'tflrail': 'TFL Rail'
};

// Reverse mapping for normalizing line names
const lineNameToId = {
  'Waterloo': 'waterloo-city',
  'Hammersmith': 'hammersmith-city',
  'Piccadilly': 'piccadilly',
  'Victoria': 'victoria',
  'Metropolitan': 'metropolitan',
  'District': 'district',
  'Central': 'central',
  'Bakerloo': 'bakerloo',
  'Circle': 'circle',
  'Northern': 'northern',
  'Jubilee': 'jubilee'
};

let localStationGraph = null;

function canonicalizeStationName(value) {
  return (value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/'/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function createLocalStationGraph() {
  if (localStationGraph) {
    return localStationGraph;
  }

  localStationGraph = {};
  Object.entries(localTimetables).forEach(([lineName, data]) => {
    const stations = Array.isArray(data?.stations) ? data.stations : [];
    stations.forEach((station, index) => {
      localStationGraph[station] = localStationGraph[station] || [];

      const addEdge = (neighbor) => {
        if (!neighbor) return;
        if (!localStationGraph[station].some(edge => edge.station === neighbor && edge.line === lineName)) {
          localStationGraph[station].push({ station: neighbor, line: lineName });
        }
      };

      addEdge(stations[index - 1]);
      addEdge(stations[index + 1]);
    });
  });

  return localStationGraph;
}

function normalizeStationName(query) {
  if (!query || !query.trim()) return null;
  const normalized = canonicalizeStationName(query);
  const stationNames = Object.keys(createLocalStationGraph());
  const exactMatch = stationNames.find(station => canonicalizeStationName(station) === normalized);
  if (exactMatch) return exactMatch;
  const prefixMatch = stationNames.find(station => canonicalizeStationName(station).startsWith(normalized));
  if (prefixMatch) return prefixMatch;
  return stationNames.find(station => canonicalizeStationName(station).includes(normalized)) || null;
}

function findLocalRoutesBetweenStations(origin, destination, maxRoutes = 4) {
  const graph = createLocalStationGraph();
  const start = normalizeStationName(origin);
  const end = normalizeStationName(destination);
  if (!start || !end) {
    return [];
  }

  const compareRoutes = (a, b) => {
    if (a.transfers !== b.transfers) return a.transfers - b.transfers;
    if (a.stops !== b.stops) return a.stops - b.stops;
    return 0;
  };

  const queue = [{
    station: start,
    line: null,
    path: [{ station: start, line: null }],
    transfers: 0,
    stops: 0
  }];
  const visited = new Map();
  const solutions = [];
  const maxStops = 80;

  while (queue.length > 0 && solutions.length < maxRoutes) {
    queue.sort(compareRoutes);
    const current = queue.shift();
    if (current.stops > maxStops) {
      continue;
    }

    if (current.station === end) {
      solutions.push(current);
      continue;
    }

    for (const edge of graph[current.station] || []) {
      const nextTransfers = current.line === null || current.line === edge.line ? current.transfers : current.transfers + 1;
      const nextStops = current.stops + 1;
      const key = `${edge.station}|${edge.line}`;
      const best = visited.get(key);

      if (best && best.transfers <= nextTransfers && best.stops <= nextStops) {
        continue;
      }

      visited.set(key, { transfers: nextTransfers, stops: nextStops });
      queue.push({
        station: edge.station,
        line: edge.line,
        path: [...current.path, { station: edge.station, line: edge.line }],
        transfers: nextTransfers,
        stops: nextStops
      });
    }
  }

  return solutions.sort(compareRoutes);
}

function buildSegmentsFromPath(path) {
  if (!Array.isArray(path) || path.length < 2) {
    return [];
  }

  const segments = [];
  let segment = { line: path[1]?.line || 'Unknown', stations: [path[0].station] };

  for (let i = 1; i < path.length; i += 1) {
    const step = path[i];
    if (step.line !== segment.line) {
      segments.push(segment);
      segment = { line: step.line || 'Unknown', stations: [path[i - 1].station] };
    }
    segment.stations.push(step.station);
  }

  if (segment.stations.length) {
    segments.push(segment);
  }

  return segments.map(item => ({
    line: item.line,
    stations: item.stations,
    stopCount: Math.max(0, item.stations.length - 1),
    instruction: `${item.line} line from ${item.stations[0]} to ${item.stations[item.stations.length - 1]}`
  }));
}

function buildLocalJourneyResponse(origin, destination) {
  const routes = findLocalRoutesBetweenStations(origin, destination, 4).map((route, index) => ({
    id: `local-${index + 1}`,
    source: 'local',
    origin: route.path[0]?.station || origin,
    destination: route.path[route.path.length - 1]?.station || destination,
    durationMinutes: Math.max(1, route.stops * 2),
    transfers: route.transfers,
    stops: route.stops,
    summary: route.transfers === 0 ? 'Direct route from timetable data' : `${route.transfers} transfer${route.transfers === 1 ? '' : 's'} from timetable data`,
    warnings: ['Live TfL journey data unavailable. Showing timetable-based route.'],
    segments: buildSegmentsFromPath(route.path),
    path: route.path
  }));

  return {
    routes,
    source: 'local',
    origin: normalizeStationName(origin) || origin,
    destination: normalizeStationName(destination) || destination,
    warnings: routes.length ? ['Live TfL journey data unavailable. Showing timetable-based route.'] : []
  };
}

async function resolveStopPoint(query) {
  const normalizedQuery = canonicalizeStationName(query);
  if (!normalizedQuery) {
    return null;
  }

  if (cachedStationLookup[normalizedQuery]) {
    return cachedStationLookup[normalizedQuery];
  }

  const results = await makeApiRequest(`/StopPoint/Search?query=${encodeURIComponent(query)}&modes=tube`);
  const matches = Array.isArray(results?.matches) ? results.matches : [];
  const exactMatch = matches.find(match => canonicalizeStationName(match.name) === normalizedQuery);
  const prefixMatch = matches.find(match => canonicalizeStationName(match.name).startsWith(normalizedQuery));
  const includesMatch = matches.find(match => canonicalizeStationName(match.name).includes(normalizedQuery));
  const preferredMatch = exactMatch || prefixMatch || includesMatch || matches[0] || null;

  if (!preferredMatch) {
    return null;
  }

  cachedStationLookup[normalizedQuery] = preferredMatch;
  return preferredMatch;
}

function mapTfLJourney(journey, index) {
  const legs = Array.isArray(journey?.legs) ? journey.legs : [];
  const segments = legs.map((leg, legIndex) => {
    const routeName = leg.routeOptions?.[0]?.name || leg.lineString || leg.mode?.name || 'Tube';
    const departure = leg.departurePoint?.commonName || leg.instruction?.summary || 'Start';
    const arrival = leg.arrivalPoint?.commonName || 'End';
    const pathStops = Array.isArray(leg.path?.stopPoints) && leg.path.stopPoints.length > 0
      ? leg.path.stopPoints.map(stop => stop.name).filter(Boolean)
      : [departure, arrival];
    const stations = pathStops[0] === departure ? pathStops : [departure, ...pathStops];
    if (stations[stations.length - 1] !== arrival) {
      stations.push(arrival);
    }

    return {
      line: routeName,
      stations,
      stopCount: Math.max(0, stations.length - 1),
      instruction: leg.instruction?.summary || `${routeName} from ${departure} to ${arrival}`,
      mode: leg.mode?.name || null,
      departureTime: leg.departureTime || null,
      arrivalTime: leg.arrivalTime || null,
      legIndex
    };
  });

  const origin = legs[0]?.departurePoint?.commonName || null;
  const destination = legs[legs.length - 1]?.arrivalPoint?.commonName || null;
  const warnings = Array.isArray(journey?.fare?.fareZones) && journey.fare.fareZones.length === 0
    ? ['Fare information unavailable for this option.']
    : [];

  return {
    id: `tfl-${index + 1}`,
    source: 'tfl',
    origin,
    destination,
    durationMinutes: Number(journey?.duration) || 0,
    transfers: Math.max(0, segments.filter(segment => (segment.mode || '').toLowerCase() !== 'walking').length - 1),
    stops: segments.reduce((total, segment) => total + segment.stopCount, 0),
    summary: journey?.summary || `${Number(journey?.duration) || 0} min journey`,
    departureTime: legs[0]?.departureTime || null,
    arrivalTime: legs[legs.length - 1]?.arrivalTime || null,
    warnings,
    segments,
    path: segments.flatMap((segment, segmentIndex) => segment.stations.map((station, stationIndex) => ({
      station,
      line: segmentIndex === 0 && stationIndex === 0 ? null : segment.line
    })))
  };
}

async function fetchJourneyPlan(origin, destination) {
  const cacheKey = `${origin.trim().toLowerCase()}|${destination.trim().toLowerCase()}`;
  const now = Date.now();

  if (cachedJourneyPlans[cacheKey] && (now - lastJourneyPlanUpdate[cacheKey]) < JOURNEY_PLAN_CACHE_DURATION) {
    return cachedJourneyPlans[cacheKey];
  }

  try {
    const [fromStop, toStop] = await Promise.all([
      resolveStopPoint(origin),
      resolveStopPoint(destination)
    ]);

    const normalizedOrigin = normalizeStationName(origin) || fromStop?.name || origin;
    const normalizedDestination = normalizeStationName(destination) || toStop?.name || destination;

    if (!normalizedOrigin || !normalizedDestination) {
      throw new Error('Could not resolve station names');
    }

    const journeyData = await makeApiRequest(`/Journey/JourneyResults/${encodeURIComponent(normalizedOrigin)}/to/${encodeURIComponent(normalizedDestination)}?mode=tube&journeyPreference=LeastTime`);
    const routes = Array.isArray(journeyData?.journeys)
      ? journeyData.journeys.slice(0, 4).map(mapTfLJourney).filter(route => route.segments.length > 0)
      : [];

    if (!routes.length) {
      throw new Error('TfL journey API returned no routes');
    }

    const payload = {
      routes,
      source: 'tfl',
      origin: normalizedOrigin,
      destination: normalizedDestination,
      warnings: []
    };

    cachedJourneyPlans[cacheKey] = payload;
    lastJourneyPlanUpdate[cacheKey] = now;
    return payload;
  } catch (error) {
    console.warn('Falling back to local journey planning:', error.message);
    const fallback = buildLocalJourneyResponse(origin, destination);
    cachedJourneyPlans[cacheKey] = fallback;
    lastJourneyPlanUpdate[cacheKey] = now;
    return fallback;
  }
}

// Routes
app.get('/api/stations', (req, res) => {
  try {
    res.json(localStations);
  } catch (error) {
    console.error('Error loading stations:', error);
    res.status(500).json({ error: 'Failed to load stations' });
  }
});

app.get('/api/lines', async (req, res) => {
  try {
    // Get tube lines from TfL API
    const now = Date.now();
    if (!cachedLines || (now - lastLinesUpdate) > CACHE_DURATION) {
      console.log('Fetching lines from TfL API...');
      try {
        const lines = await makeApiRequest('/Line/Mode/tube');
        if (!Array.isArray(lines)) {
          throw new Error('Unexpected TfL lines response');
        }
        // Enhance with local line data
        const enhancedLines = lines.map(line => ({
          ...line,
          id: line.id || line.name?.toLowerCase().replace(/\s+/g, '-'),
          displayName: tflLineNameMap[line.id?.toLowerCase()] || line.name,
          color: getLineColor(line.name || line.id)
        }));
        cachedLines = enhancedLines;
        lastLinesUpdate = now;
        console.log(`Successfully fetched ${enhancedLines.length} lines from TfL`);
      } catch (apiError) {
        console.warn('TfL API /Line/Mode/tube unavailable, using local data:', apiError.message);
        // Fallback to local data
        const localLines = [
          { name: 'Bakerloo', id: 'bakerloo', displayName: 'Bakerloo', color: '#B36305' },
          { name: 'Central', id: 'central', displayName: 'Central', color: '#E32017' },
          { name: 'Circle', id: 'circle', displayName: 'Circle', color: '#FFD300' },
          { name: 'District', id: 'district', displayName: 'District', color: '#00782A' },
          { name: 'Hammersmith', id: 'hammersmith-city', displayName: 'Hammersmith', color: '#F3A9BB' },
          { name: 'Jubilee', id: 'jubilee', displayName: 'Jubilee', color: '#A0A5A9' },
          { name: 'Metropolitan', id: 'metropolitan', displayName: 'Metropolitan', color: '#9B0056' },
          { name: 'Northern', id: 'northern', displayName: 'Northern', color: '#000000' },
          { name: 'Piccadilly', id: 'piccadilly', displayName: 'Piccadilly', color: '#003688' },
          { name: 'Victoria', id: 'victoria', displayName: 'Victoria', color: '#0098D4' },
          { name: 'Waterloo', id: 'waterloo-city', displayName: 'Waterloo', color: '#95CDBA' }
        ];
        cachedLines = localLines;
        lastLinesUpdate = now;
      }
    }
    res.json(cachedLines);
  } catch (error) {
    console.error('Error in /api/lines:', error);
    res.status(500).json({ error: 'Failed to load lines' });
  }
});

app.get('/api/line/:lineId/status', async (req, res) => {
  const { lineId } = req.params;
  try {
    const now = Date.now();

    if (cachedLineStatuses[lineId] && (now - lastLineStatusUpdate[lineId]) < LINE_STATUS_CACHE_DURATION) {
      return res.json(cachedLineStatuses[lineId]);
    }

    const allStatuses = await fetchAllLineStatuses();
    if (allStatuses && allStatuses[lineId]) {
      return res.json(allStatuses[lineId]);
    }

    const status = await makeApiRequest(`/Line/${lineId}/Status`, { priority: true });
    cachedLineStatuses[lineId] = status;
    lastLineStatusUpdate[lineId] = now;
    res.json(status);
  } catch (error) {
    console.error('Error fetching line status for', lineId, error.message);

    if (cachedLineStatuses[lineId]) {
      console.log('Returning cached line status due to API error');
      return res.json(cachedLineStatuses[lineId]);
    }

    console.warn(`Returning empty status for ${lineId} due to API error`);
    return res.json([]);
  }
});

function summarizeStatuses(statusMap) {
  const summary = { healthy: 0, minor: 0, disrupted: 0, unknown: 0, total: 0 };

  Object.values(statusMap).forEach(statusData => {
    const statusInfo = Array.isArray(statusData) ? statusData[0] : statusData;
    const description = statusInfo?.lineStatuses?.[0]?.statusSeverityDescription || statusInfo?.statusSeverityDescription || '';
    const normalized = (description || '').toLowerCase();

    if (normalized.includes('good service')) {
      summary.healthy += 1;
    } else if (normalized.includes('minor') || normalized.includes('reduced') || normalized.includes('part suspension') || normalized.includes('planned closure')) {
      summary.minor += 1;
    } else if (normalized.includes('severe') || normalized.includes('major') || normalized.includes('suspended') || normalized.includes('disrupted') || normalized.includes('closure')) {
      summary.disrupted += 1;
    } else {
      summary.unknown += 1;
    }

    summary.total += 1;
  });

  return summary;
}

app.get('/api/status/summary', async (req, res) => {
  try {
    const statuses = await fetchAllLineStatuses();
    const summary = summarizeStatuses(statuses || cachedLineStatuses);
    const lastUpdated = Math.max(...Object.values(lastLineStatusUpdate), Date.now());
    return res.json({ summary, lastUpdated });
  } catch (error) {
    console.error('Error fetching status summary:', error.message);
    const summary = summarizeStatuses(cachedLineStatuses);
    const lastUpdated = Math.max(...Object.values(lastLineStatusUpdate), Date.now());
    res.status(200).json({ summary, lastUpdated, warning: 'Partial summary from cache' });
  }
});

app.get('/api/service-alerts', async (req, res) => {
  try {
    const statuses = await fetchAllLineStatuses();
    const alerts = Object.entries(statuses || cachedLineStatuses).flatMap(([lineId, statusData]) => {
      const statusInfo = Array.isArray(statusData) ? statusData[0] : statusData;
      const line = tflLineNameMap[lineId] || statusInfo?.name || lineId;
      const lineSeverity = statusInfo?.lineStatuses?.[0]?.statusSeverityDescription || statusInfo?.statusSeverityDescription || 'Unknown';
      const reason = statusInfo?.lineStatuses?.[0]?.reason || statusInfo?.reason || 'No details available';
      const normalized = (lineSeverity || '').toLowerCase();
      const isAlert = normalized.includes('minor') || normalized.includes('reduced') || normalized.includes('part suspension') || normalized.includes('planned closure') || normalized.includes('severe') || normalized.includes('major') || normalized.includes('suspended') || normalized.includes('disrupted') || normalized.includes('closure');
      if (!isAlert) return [];
      const severity = normalized.includes('severe') || normalized.includes('major') || normalized.includes('suspended') || normalized.includes('disrupted') || normalized.includes('closure') ? 'disrupted' : 'minor';
      return [{ line, lineId, status: lineSeverity, severity, reason }];
    });

    res.json({ alerts, lastUpdated: Date.now() });
  } catch (error) {
    console.error('Error fetching service alerts:', error.message);
    res.status(500).json({ error: 'Unable to fetch service alerts' });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime(), timestamp: Date.now() });
});

// Batch endpoint - fetch all line statuses efficiently
app.get('/api/lines/all-statuses', async (req, res) => {
  try {
    const statuses = await fetchAllLineStatuses();
    if (statuses && Object.keys(statuses).length > 0) {
      return res.json(statuses);
    }
    if (Object.keys(cachedLineStatuses).length > 0) {
      return res.json(cachedLineStatuses);
    }
    res.status(500).json({ error: 'Failed to fetch line statuses' });
  } catch (error) {
    console.error('Error fetching all line statuses:', error.message);
    if (Object.keys(cachedLineStatuses).length > 0) {
      return res.json(cachedLineStatuses);
    }
    res.status(500).json({ error: 'Failed to fetch line statuses' });
  }
});

app.get('/api/stoppoint/search', async (req, res) => {
  try {
    const { query } = req.query;
    if (!query) {
      return res.status(400).json({ error: 'Query parameter required' });
    }
    
    const cacheKey = query.toLowerCase().trim();
    const now = Date.now();
    
    // Check cache
    if (cachedStopSearches[cacheKey] && (now - lastStopSearchUpdate[cacheKey]) < STOP_SEARCH_CACHE_DURATION) {
      return res.json(cachedStopSearches[cacheKey]);
    }
    
    const queryString = `/StopPoint/Search?query=${encodeURIComponent(query)}&modes=tube`;
    const results = await makeApiRequest(queryString);
    
    // Enhance results with local line information
    if (results && results.matches) {
      results.matches = results.matches.map(match => {
        const localStationInfo = localStationData.stations && localStationData.stations[match.name];
        
        if (localStationInfo) {
          match.lines = localStationInfo.lines || [];
          match.zone = localStationInfo.zone || 1;
        }
        
        return match;
      });
    }
    
    // Cache the result
    cachedStopSearches[cacheKey] = results;
    lastStopSearchUpdate[cacheKey] = now;
    
    res.json(results);
  } catch (error) {
    console.error('Error searching stops:', error.message);
    
    // Return cached data if available
    const cacheKey = req.query.query?.toLowerCase().trim();
    if (cacheKey && cachedStopSearches[cacheKey]) {
      console.log('Returning cached stop search results due to API error');
      return res.json(cachedStopSearches[cacheKey]);
    }
    
    res.json({ matches: [] });
  }
});

app.get('/api/stoppoint/:stopId/arrivals', async (req, res) => {
  try {
    const { stopId } = req.params;
    const arrivals = await makeApiRequest(`/StopPoint/${stopId}/Arrivals`);
    if (!Array.isArray(arrivals)) {
      throw new Error('Unexpected arrivals response');
    }
    const sorted = arrivals
      .sort((a, b) => (a.timeToStation || 0) - (b.timeToStation || 0))
      .slice(0, 10);
    res.json(sorted);
  } catch (error) {
    console.error('Error fetching arrivals:', error.message);
    res.json([]);
  }
});

app.get('/api/journey-plan', async (req, res) => {
  try {
    const { from, to } = req.query;
    if (!from || !to) {
      return res.status(400).json({ error: 'Both from and to query parameters are required' });
    }

    const payload = await fetchJourneyPlan(String(from), String(to));
    res.json(payload);
  } catch (error) {
    console.error('Error planning journey:', error.message);
    res.status(500).json({ error: 'Failed to plan journey' });
  }
});

function normalizeCrowding(crowding) {
  if (!crowding || typeof crowding !== 'object') {
    return 'Moderate';
  }
  // TfL crowding data might be in different format
  // For now, return moderate as default
  return 'Moderate';
}

function mapArrivalToTrain(arrival) {
  const timeToStation = Number(arrival.timeToStation) || 0;
  const progress = Math.min(100, Math.max(0, 100 - Math.round((timeToStation / 120) * 100)));
  
  // Map TfL lineId to our line names - handle both ID and display name
  let lineName = arrival.lineName || 'Unknown';
  
  // Try to map from lineId first
  if (arrival.lineId) {
    const normalized = arrival.lineId.toLowerCase();
    lineName = tflLineNameMap[normalized] || arrival.lineName || 'Unknown';
  }
  
  // Ensure lineName is properly capitalized
  if (lineName && lineName !== 'Unknown') {
    lineName = lineName.charAt(0).toUpperCase() + lineName.slice(1).toLowerCase();
  }
  
  return {
    id: arrival.vehicleId || `${arrival.lineId}-${arrival.platformName || arrival.stationName || arrival.towards}-${arrival.destinationName}`.replace(/\s+/g, '_'),
    line: lineName,
    lineId: arrival.lineId || 'unknown',
    destination: arrival.destinationName || arrival.towards || 'Unknown',
    nextStation: arrival.stationName || arrival.platformName || arrival.towards || 'Next stop',
    currentPosition: 1,
    progress,
    status: timeToStation <= 30 ? 'Due' : 'Running',
    passengers: normalizeCrowding(arrival.crowding),
    timeToStation,
    expectedArrival: arrival.expectedArrival,
    stopPointId: arrival.stopPointId,
    platformName: arrival.platformName || 'TBC'
  };
}

async function fetchLiveTfLTrains(forceRefresh = false) {
  try {
    const allArrivals = await fetchAllArrivals(forceRefresh);

    if (!Array.isArray(allArrivals) || allArrivals.length === 0) {
      console.log('No arrivals from TfL, using fallback data');
      const fallback = require('./public/trains-tracking.json');
      return fallback.trains;
    }

    const trains = allArrivals
      .sort((a, b) => (a.timeToStation || 0) - (b.timeToStation || 0))
      .slice(0, 150)
      .map(mapArrivalToTrain);

    console.log(`Returning ${trains.length} trains from TfL API`);
    return trains;
  } catch (error) {
    console.error('Error fetching live trains from TfL:', error.message);
    return localTrainsTracking.trains;
  }
}

app.get('/api/live-trains', async (req, res) => {
  try {
    const trains = await fetchLiveTfLTrains();
    res.json({ trains });
  } catch (error) {
    console.error('Error fetching live trains from TfL:', error.message);
    res.json(localTrainsTracking);
  }
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Global error handler
app.use((err, req, res, next) => {
  console.error('Unhandled express error:', err);
  if (!res.headersSent) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error);
});

process.on('unhandledRejection', (reason) => {
  console.error('Unhandled promise rejection:', reason);
});

// WebSocket connection for live updates
let liveTrainCache = null;
let trainUpdateInterval = null;

io.on('connection', async (socket) => {
  console.log('New client connected:', socket.id);
  
  if (!liveTrainCache) {
    try {
      const trains = await fetchLiveTfLTrains();
      liveTrainCache = { trains };
    } catch (error) {
      console.error('Error loading TfL live trains:', error.message);
      liveTrainCache = localTrainsTracking;
    }
  }

  socket.emit('initial-trains', liveTrainCache);
  
  if (!trainUpdateInterval) {
    trainUpdateInterval = setInterval(async () => {
      try {
        const trains = await fetchLiveTfLTrains();
        liveTrainCache = { trains };
        io.emit('train-update', liveTrainCache);
      } catch (error) {
        console.error('Error updating trains:', error.message);
      }
    }, 30000); // Increased from 15s to 30s to reduce API call frequency
  }
  
  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

// Start Server
if (require.main === module) {
  server.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
    console.log('Integrating with TfL API for real-time data...');
  });
}

module.exports = { app, server, io };