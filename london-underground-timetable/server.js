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

  inFlightRequests.set(path, requestPromise);
  requestPromise.finally(() => inFlightRequests.delete(path));
  requestPromise.catch(() => {});

  return requestPromise;
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
let lastLinesUpdate = 0;
let lastLineStatusUpdate = {};
let lastAllLineStatusesUpdate = 0;
let lastAllArrivalsUpdate = 0;
let lastStopSearchUpdate = {};
const CACHE_DURATION = 600000; // 10 minutes (increased from 5 to reduce API calls)
const LINE_STATUS_CACHE_DURATION = 120000; // 2 minutes (increased from 1 minute)
const STOP_SEARCH_CACHE_DURATION = 600000; // 10 minutes (unchanged - already 5 minutes)
const ALL_ARRIVALS_CACHE_DURATION = 30000; // 30 seconds (increased from 15s)

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