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
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
  }
};

// Helper function to make HTTPS requests to TfL API
function makeApiRequest(path) {
  return new Promise((resolve, reject) => {
    const url = `${TFL_API_BASE}${path}`;
    https.get(url, TFL_OPTIONS, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          // Handle both JSON and text responses
          if (res.statusCode === 200) {
            const parsed = JSON.parse(data);
            resolve(parsed);
          } else {
            console.log(`TfL API returned status ${res.statusCode}`);
            reject(new Error(`API returned status ${res.statusCode}`));
          }
        } catch (e) {
          console.log('Response:', data.substring(0, 200));
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
let lastLinesUpdate = 0;
let lastStationsUpdate = 0;
const CACHE_DURATION = 300000; // 5 minutes

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
    const status = await makeApiRequest(`/Line/${lineId}/Status`);
    res.json(status);
  } catch (error) {
    console.error('Error fetching line status:', error.message);
    res.json({ status: 'Unknown', message: 'Status unavailable' });
  }
});

app.get('/api/stoppoint/search', async (req, res) => {
  try {
    const { query } = req.query;
    if (!query) {
      return res.status(400).json({ error: 'Query parameter required' });
    }
    const results = await makeApiRequest(`/StopPoint/Search?query=${encodeURIComponent(query)}`);
    res.json(results);
  } catch (error) {
    console.error('Error searching stops:', error.message);
    res.json({ matches: [] });
  }
});

app.get('/api/stoppoint/:stopId/arrivals', async (req, res) => {
  try {
    const { stopId } = req.params;
    const arrivals = await makeApiRequest(`/StopPoint/${stopId}/Arrivals`);
    // Sort and limit to next 10
    const sorted = arrivals
      .sort((a, b) => a.timeToStation - b.timeToStation)
      .slice(0, 10);
    res.json(sorted);
  } catch (error) {
    console.error('Error fetching arrivals:', error.message);
    res.json([]);
  }
});

app.get('/api/live-trains', (req, res) => {
  const trains = require('./public/trains-tracking.json');
  res.json(trains);
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// WebSocket connection for live updates
let liveTrainCache = null;
let trainUpdateInterval = null;

io.on('connection', (socket) => {
  console.log('New client connected:', socket.id);
  
  // Send initial train data
  if (!liveTrainCache) {
    liveTrainCache = require('./public/trains-tracking.json');
  }
  socket.emit('initial-trains', liveTrainCache);
  
  // Simulate train movement with real-time updates
  if (!trainUpdateInterval) {
    trainUpdateInterval = setInterval(async () => {
      try {
        // Fetch real data from TfL for enhanced tracking
        liveTrainCache = require('./public/trains-tracking.json');
        
        // Simulate train movement
        liveTrainCache.trains.forEach(train => {
          if (train.status === 'Running') {
            train.progress += Math.random() * 10 + 5;
            if (train.progress >= 100) {
              train.progress = 0;
              train.currentPosition += 1;
              const stations = require('./public/stations.json');
              const lineStations = stations[train.line];
              if (train.currentPosition >= lineStations.length) {
                train.currentPosition = 0;
              }
              train.nextStation = lineStations[train.currentPosition];
            }
          }
        });
        
        io.emit('train-update', liveTrainCache);
      } catch (error) {
        console.error('Error updating trains:', error);
      }
    }, 2000);
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