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
const PORT = 3000;

// TfL API Configuration
const TFL_API_BASE = 'https://api.tfl.gov.uk';
// Using public API without authentication, adding user agent for better compatibility
const TFL_OPTIONS = {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
  }
};

// Strict rate limiting with global request queue
let lastApiCall = 0;
const API_RATE_LIMIT = 3000; // 3 seconds between ALL requests
let requestQueue = [];
let isProcessingQueue = false;

function queueApiRequest(path) {
  return new Promise((resolve, reject) => {
    requestQueue.push({ path, resolve, reject, timestamp: Date.now() });
    processQueue();
  });
}

async function processQueue() {
  if (isProcessingQueue || requestQueue.length === 0) return;
  
  isProcessingQueue = true;
  
  while (requestQueue.length > 0) {
    const { path, resolve, reject } = requestQueue.shift();
    const now = Date.now();
    const timeSinceLastCall = now - lastApiCall;
    
    if (timeSinceLastCall < API_RATE_LIMIT) {
      await new Promise(r => setTimeout(r, API_RATE_LIMIT - timeSinceLastCall));
    }
    
    try {
      const result = await executeApiRequest(path);
      resolve(result);
    } catch (error) {
      reject(error);
    }
  }
  
  isProcessingQueue = false;
}

function makeApiRequest(path) {
  return queueApiRequest(path);
}

function executeApiRequest(path) {
  return new Promise((resolve, reject) => {
    lastApiCall = Date.now();
    const url = `${TFL_API_BASE}${path}`;
    
    console.log(`[${new Date().toLocaleTimeString()}] API Request: ${path}`);
    
    https.get(url, TFL_OPTIONS, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          if (res.statusCode === 200) {
            const parsed = JSON.parse(data);
            console.log(`[${new Date().toLocaleTimeString()}] ✓ Success: ${path} (${Array.isArray(parsed) ? parsed.length : 'object'} items)`);
            resolve(parsed);
          } else {
            console.log(`[${new Date().toLocaleTimeString()}] ✗ Failed: ${path} (HTTP ${res.statusCode})`);
            reject(new Error(`API returned status ${res.statusCode}`));
          }
        } catch (e) {
          reject(new Error('Invalid JSON response from TfL API'));
        }
      });
    }).on('error', (e) => {
      console.error(`[${new Date().toLocaleTimeString()}] ✗ Error: ${path}`, e.message);
      reject(e);
    });
  });
}

// Cache for API data
let cachedLines = null;
let cachedStations = null;
let cachedLineStatuses = {};
let cachedStopSearches = {};
let lastLinesUpdate = 0;
let lastStationsUpdate = 0;
let lastLineStatusUpdate = {};
let lastStopSearchUpdate = {};
const CACHE_DURATION = 300000; // 5 minutes
const LINE_STATUS_CACHE_DURATION = 60000; // 1 minute for line status
const STOP_SEARCH_CACHE_DURATION = 300000; // 5 minutes for stop searches

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Routes
app.get('/api/stations', async (req, res) => {
  try {
    const stations = require('./public/stations.json');
    res.json(stations);
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
        cachedLines = lines;
        lastLinesUpdate = now;
        console.log(`Successfully fetched ${lines.length} lines from TfL`);
      } catch (apiError) {
        console.log('TfL API unavailable, using local data');
        // Fallback to local data
        const stations = require('./public/stations.json');
        cachedLines = Object.keys(stations).map(line => ({ name: line, id: line.toLowerCase() }));
      }
    }
    res.json(cachedLines);
  } catch (error) {
    console.error('Error in /api/lines:', error);
    res.status(500).json({ error: 'Failed to load lines' });
  }
});

app.get('/api/line/:lineId/status', async (req, res) => {
  try {
    const { lineId } = req.params;
    const now = Date.now();
    
    // Check cache
    if (cachedLineStatuses[lineId] && (now - lastLineStatusUpdate[lineId]) < LINE_STATUS_CACHE_DURATION) {
      return res.json(cachedLineStatuses[lineId]);
    }
    
    const status = await makeApiRequest(`/Line/${lineId}/Status`);
    
    // Cache the result
    cachedLineStatuses[lineId] = status;
    lastLineStatusUpdate[lineId] = now;
    
    res.json(status);
  } catch (error) {
    console.error('Error fetching line status:', error.message);
    
    // Return cached data if available, even if expired
    if (cachedLineStatuses[req.params.lineId]) {
      console.log('Returning cached line status due to API error');
      return res.json(cachedLineStatuses[req.params.lineId]);
    }
    
    res.status(500).json({ error: 'Failed to fetch line status', message: error.message });
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
    const sorted = arrivals
      .sort((a, b) => a.timeToStation - b.timeToStation)
      .slice(0, 10);
    res.json(sorted);
  } catch (error) {
    console.error('Error fetching arrivals:', error.message);
    res.json([]);
  }
});

// TfL line name mapping
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
  'jubilee': 'Jubilee'
};

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
  
  // Map TfL lineId to our line names
  const lineName = tflLineNameMap[arrival.lineId] || arrival.lineName || 'Unknown';
  
  return {
    id: arrival.vehicleId || `${arrival.lineId}-${arrival.platformName || arrival.stationName || arrival.towards}-${arrival.destinationName}`.replace(/\s+/g, '_'),
    line: lineName,
    destination: arrival.destinationName || arrival.towards || 'Unknown',
    nextStation: arrival.stationName || arrival.platformName || arrival.towards || 'Next stop',
    currentPosition: 1,
    progress,
    status: timeToStation <= 30 ? 'Due' : 'Running',
    passengers: normalizeCrowding(arrival.crowding),
    timeToStation,
    expectedArrival: arrival.expectedArrival
  };
}

async function fetchLiveTfLTrains() {
  const lineIds = ['bakerloo', 'central', 'circle', 'district', 'hammersmith-city', 'jubilee', 'metropolitan', 'northern', 'piccadilly', 'victoria', 'waterloo-city'];
  
  try {
    // Fetch arrivals for all lines sequentially to avoid rate limiting
    const allArrivals = [];
    let successCount = 0;
    
    for (const lineId of lineIds) {
      try {
        const arrivals = await makeApiRequest(`/Line/${lineId}/Arrivals`);
        if (Array.isArray(arrivals)) {
          allArrivals.push(...arrivals);
          successCount++;
          console.log(`✓ Fetched ${arrivals.length} arrivals for ${lineId}`);
        }
      } catch (error) {
        console.log(`✗ Error fetching arrivals for ${lineId}: ${error.message}`);
      }
    }
    
    console.log(`Successfully fetched arrivals from ${successCount}/${lineIds.length} lines (${allArrivals.length} total arrivals)`);
    
    if (allArrivals.length === 0) {
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
    // Fallback to local data
    const fallback = require('./public/trains-tracking.json');
    return fallback.trains;
  }
}

app.get('/api/live-trains', async (req, res) => {
  try {
    const trains = await fetchLiveTfLTrains();
    res.json({ trains });
  } catch (error) {
    console.error('Error fetching live trains from TfL:', error.message);
    const fallback = require('./public/trains-tracking.json');
    res.json(fallback);
  }
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
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
      liveTrainCache = require('./public/trains-tracking.json');
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
    }, 15000);
  }
  
  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

// Start Server
server.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
  console.log('Integrating with TfL API for real-time data...');
});