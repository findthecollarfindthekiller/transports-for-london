# TfL API Integration Guide

## Overview
This application integrates with the Transport for London (TfL) API to provide real-time data about London Underground lines, stations, and train arrivals.

## TfL API Features Implemented

### 1. **Line Information Endpoint**
- **Endpoint**: `/api/lines`
- **Description**: Fetches all London Underground tube lines
- **TfL API**: `GET /Line/Mode/tube`
- **Response**: Array of line objects with name and ID
- **Caching**: 5 minutes to reduce API calls

### 2. **Line Status Endpoint**
- **Endpoint**: `/api/line/:lineId/status`
- **Description**: Gets current status of a specific line
- **TfL API**: `GET /Line/{lineId}/Status`
- **Response**: Line status including disruptions and messages
- **Use Case**: Display service alerts and disruptions

### 3. **Stop Point Search Endpoint**
- **Endpoint**: `/api/stoppoint/search?query={stationName}`
- **Description**: Search for stations by name
- **TfL API**: `GET /StopPoint/Search`
- **Response**: List of matching stops with details
- **Use Case**: Smart station search functionality

### 4. **Arrivals Prediction Endpoint**
- **Endpoint**: `/api/stoppoint/:stopId/arrivals`
- **Description**: Get next train arrivals at a specific station
- **TfL API**: `GET /StopPoint/{stopId}/Arrivals`
- **Response**: Next 10 arrivals sorted by time
- **Use Case**: Real-time next arrival predictions

## API Configuration

### Authentication
The TfL API allows public access without authentication for most endpoints. Rate limiting is in place (~1000 requests per minute).

### Base URL
```
https://api.tfl.gov.uk
```

### Error Handling
All API endpoints include graceful error handling:
- Falls back to local cached data on API failures
- Logs errors for debugging
- Returns appropriate error responses
- Automatically uses local data if TfL API is unavailable

## Implementation Details

### Server-Side Integration (server.js)
```javascript
// TfL API Configuration
const TFL_API_BASE = 'https://api.tfl.gov.uk';

// Helper function to make HTTPS requests
function makeApiRequest(path) {
  // Handles JSON parsing and error cases
  // Returns Promise for async/await usage
}

// Caching mechanism
const cachedLines = null;
const CACHE_DURATION = 300000; // 5 minutes
```

### Client-Side Integration (index.html)
```javascript
// Fetch real TfL data
async function loadData() {
  // Loads local data first
  // Tries to augment with TfL data
  // Falls back gracefully on errors
}

// Helper functions
async function fetchRealArrivals(stopPointId)
async function searchStationWithTfL(query)
```

## Data Flow

### Live Train Tracking
1. Server maintains WebSocket connection with connected clients
2. Every 2 seconds:
   - Load latest train data
   - Simulate realistic train movements
   - Broadcast updates to all connected clients
3. Frontend renders real-time train cards with:
   - Current position
   - Progress to next station
   - Passenger load
   - Status (Running/Delayed)

### Station Search
1. User searches for a station
2. Frontend attempts TfL API search first
3. Falls back to local station database
4. Displays results with:
   - Station name and zone
   - Serving lines
   - Local timetable data

## Sample API Responses

### Line Information
```json
[
  {
    "id": "central",
    "name": "Central",
    "modeName": "Tube",
    "disruptions": [],
    "created": "2026-05-01T00:00:00Z"
  },
  {
    "id": "bakerloo",
    "name": "Bakerloo",
    "modeName": "Tube",
    "disruptions": [],
    "created": "2026-05-01T00:00:00Z"
  }
]
```

### Arrivals Data
```json
[
  {
    "id": "WCS",
    "operationType": 1,
    "lineId": "central",
    "lineName": "Central",
    "platformName": "Eastbound",
    "towards": "Stratford",
    "expectedArrival": "2026-05-01T04:03:00Z",
    "timeToStation": 120
  }
]
```

## Testing the Integration

### Test TfL Endpoints
```bash
# Get lines
curl https://api.tfl.gov.uk/Line/Mode/tube

# Get line status
curl https://api.tfl.gov.uk/Line/central/Status

# Search stations
curl "https://api.tfl.gov.uk/StopPoint/Search?query=oxford%20circus"

# Get arrivals
curl https://api.tfl.gov.uk/StopPoint/1000049/Arrivals
```

### Local Server Endpoints
```bash
# Fetch lines via our server
curl http://localhost:3000/api/lines

# Fetch line status
curl http://localhost:3000/api/line/central/status

# Search stations
curl "http://localhost:3000/api/stoppoint/search?query=baker%20street"

# Get arrivals at a station
curl http://localhost:3000/api/stoppoint/1000049/arrivals
```

## Performance Considerations

1. **Caching**: Lines data is cached for 5 minutes to reduce API calls
2. **Error Recovery**: Failed API calls gracefully fall back to local data
3. **WebSocket Efficiency**: Uses Socket.io for efficient real-time communication
4. **Rate Limiting**: Respects TfL API rate limits
5. **Bandwidth**: Only essential data is transmitted over WebSocket

## Future Enhancements

1. **Real Disruption Data**: Display actual TfL service disruptions
2. **Journey Planning**: Use TfL Journey API for route suggestions
3. **Real Train Tracking**: Integrate actual train location data (when available)
4. **Accessibility**: Add accessibility features
5. **Offline Support**: Service worker for offline functionality
6. **Advanced Analytics**: Track popular routes and times

## Troubleshooting

### API Connection Errors
- Check internet connection
- Verify TfL API is accessible
- Check server logs for detailed error messages

### Missing Station Data
- Ensure `stations-data.json` is present
- Verify station IDs match TfL API format

### Real-time Updates Not Working
- Check WebSocket connection (Socket.io)
- Verify server is running
- Check browser console for errors

## References

- [TfL API Documentation](https://api.tfl.gov.uk/)
- [Socket.io Documentation](https://socket.io/docs/)
- [Express.js Documentation](https://expressjs.com/)

## License
This integration follows TfL's terms of service. See TfL API documentation for usage requirements.
