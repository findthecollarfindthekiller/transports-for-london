# London Underground Timetable Application

This application provides a timetable for the London Underground with the following features:

- **Color-coded lines**: Each Underground line is visually distinct with its official color.
- **Station arrival information**: Displays arrival times for each station on a selected line.
- **Live train tracking**: Real-time tracking of trains across the network with WebSocket updates.

## Features
- Interactive user interface for selecting lines and stations.
- Real-time updates for train schedules.
- **Live Train Tracking**: See trains in motion with:
  - Real-time train positions and destinations
  - Distance progress to next station
  - Passenger load indicators
  - Train status (Running/Delayed)
  - Updates every 2 seconds via WebSocket
- Clear and intuitive design.
- Responsive layout for various screen sizes.

## Getting Started
1. Clone the repository.
2. Install dependencies using `npm install`.
3. Run the application using `npm start`.
4. Open your browser to `http://localhost:3000`.

## Usage
- **Lines & Timetables Tab**: Select a line to view stations and timetables
- **Station Information Tab**: Search for specific stations and see all serving lines
- **Live Tracking Tab**: Monitor active trains in real-time with live position updates

## Technologies Used
- **Frontend**: HTML5, CSS3, JavaScript, Socket.io Client
- **Backend**: Node.js with Express, Socket.io Server
- **Real-time Communication**: WebSocket (Socket.io)
- **External API**: TfL (Transport for London) API for real data integration
- **Data**: JSON-based local data (extensible to MongoDB)

## TfL API Integration
The application now integrates with the official Transport for London API to provide:
- **Real Line Information**: Fetch actual tube lines and status
- **Station Search**: Search stations using TfL database
- **Arrival Predictions**: Get next train predictions for stations
- **Service Disruptions**: Display current service information

For detailed integration information, see [TFL_INTEGRATION.md](TFL_INTEGRATION.md)

### API Endpoints
- `GET /api/lines` - Fetch all tube lines
- `GET /api/line/:lineId/status` - Get line status
- `GET /api/stoppoint/search?query=...` - Search stations
- `GET /api/stoppoint/:stopId/arrivals` - Get next arrivals

## Live Train Tracking Features
The Live Tracking feature provides:
- **Train Position**: Current station index along the line
- **Progress Bar**: Visual indicator of distance to next station
- **Passenger Info**: Live passenger load levels (Quiet, Moderate, Busy, Very Busy)
- **Train Status**: Running or Delayed status indicators
- **Destination Info**: Shows which station the train is heading to
- **Real-time Updates**: WebSocket connection ensures live data streaming

## Project Structure
```
london-underground-timetable/
├── server.js                    # Express server with Socket.io
├── package.json                 # Dependencies
├── README.md                    # This file
└── public/
    ├── index.html              # Main application UI
    ├── stations.json           # Station data
    ├── timetables.json        # Timetable schedules
    ├── stations-data.json     # Station metadata
    └── trains-tracking.json   # Live train data
```

## Future Enhancements
- Integrate with TfL API for real-time data
- Add accessibility features for visually impaired users
- Mobile app version
- Route planning between stations
- Service alerts and disruptions
- User accounts and saved preferences

## Running the Development Server
For development with auto-reload:
```bash
npm run dev
```

---

This project is under active development. Contributions are welcome!