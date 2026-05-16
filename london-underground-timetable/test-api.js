const https = require('https');
const options = {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
  }
};

// Test different endpoints
const endpoints = [
  '/Line/bakerloo/Arrivals',
  '/Mode/tube/Arrivals',
  '/StopPoint/Search?query=Bank&modes=tube'
];

async function testEndpoint(endpoint) {
  return new Promise((resolve) => {
    const url = 'https://api.tfl.gov.uk' + endpoint;
    console.log('Testing: ' + endpoint);
    https.get(url, options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        console.log('Status: ' + res.statusCode);
        if (res.statusCode === 200) {
          try {
            const parsed = JSON.parse(data);
            console.log('Data type: ' + (Array.isArray(parsed) ? 'Array (' + parsed.length + ' items)' : 'Object'));
          } catch (e) {
            console.log('Parse error');
          }
        }
        console.log('---');
        resolve();
      });
    }).on('error', (e) => {
      console.log('Error: ' + e.message);
      console.log('---');
      resolve();
    });
  });
}

(async () => {
  for (const ep of endpoints) {
    await testEndpoint(ep);
    await new Promise(r => setTimeout(r, 2000));
  }
})();
