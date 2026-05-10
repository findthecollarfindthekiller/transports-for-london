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

// Rate limiting
let lastApiCall = 0;
const API_RATE_LIMIT = 1000; // 1 second between calls

// Helper function to make HTTPS requests to TfL API with rate limiting
function makeApiRequest(path) {
  return new Promise((resolve, reject) => {
    const now = Date.now();
    const timeSinceLastCall = now - lastApiCall;
    
    if (timeSinceLastCall < API_RATE_LIMIT) {
      // Wait for rate limit
      setTimeout(() => makeApiRequest(path).then(resolve).catch(reject), API_RATE_LIMIT - timeSinceLastCall);
      return;
    }
    
    lastApiCall = now;
    
    const url = `${TFL_API_BASE}${path}`;
    console.log(`Making API request to: ${url}`);
    
    https.get(url, TFL_OPTIONS, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          // Handle both JSON and text responses
          if (res.statusCode === 200) {
            const parsed = JSON.parse(data);
            resolve(parsed);
          } else if (res.statusCode === 429) {
            console.log('Rate limited by TfL API, retrying in 5 seconds...');
            setTimeout(() => makeApiRequest(path).then(resolve).catch(reject), 5000);
            return;
          } else {
            console.log(`TfL API returned status ${res.statusCode} for ${path}`);
            reject(new Error(`API returned status ${res.statusCode}`));
          }
        } catch (e) {
          console.log('Response parsing error:', e.message);
          reject(new Error('Invalid JSON response from TfL API'));
        }
      });
    }).on('error', (e) => {
      console.error('TfL API request error:', e.message);
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
    // Fetch arrivals for all lines
    const arrivalPromises = lineIds.map(lineId => 
      makeApiRequest(`/Line/${lineId}/Arrivals`)
        .then(arrivals => arrivals || [])
        .catch(error => {
          console.log(`Error fetching arrivals for ${lineId}:`, error.message);
          return [];
        })
    );
    
    const allArrivals = await Promise.all(arrivalPromises);
    const combinedArrivals = allArrivals.flat();
    
    if (combinedArrivals.length === 0) {
      throw new Error('No arrivals data available from any line');
    }
    
    return combinedArrivals
      .sort((a, b) => (a.timeToStation || 0) - (b.timeToStation || 0))
      .slice(0, 150)
      .map(mapArrivalToTrain);
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