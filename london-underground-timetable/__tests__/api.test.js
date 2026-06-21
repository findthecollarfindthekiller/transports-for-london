const request = require('supertest');
const { app } = require('../server');

jest.setTimeout(30000);

describe('London Underground Timetable API', () => {
  test('GET /api/health returns ok status', async () => {
    const response = await request(app).get('/api/health').expect(200);
    expect(response.body).toHaveProperty('status', 'ok');
    expect(response.body).toHaveProperty('uptime');
    expect(typeof response.body.uptime).toBe('number');
  });

  test('GET /api/lines returns an array of lines', async () => {
    const response = await request(app).get('/api/lines').expect(200);
    expect(Array.isArray(response.body)).toBe(true);
    expect(response.body.length).toBeGreaterThan(0);
    expect(response.body[0]).toHaveProperty('id');
    expect(response.body[0]).toHaveProperty('displayName');
  });

  test('GET /api/status/summary returns a valid summary object', async () => {
    const response = await request(app).get('/api/status/summary').expect(200);
    expect(response.body).toHaveProperty('summary');
    expect(response.body.summary).toHaveProperty('healthy');
    expect(response.body.summary).toHaveProperty('minor');
    expect(response.body.summary).toHaveProperty('disrupted');
    expect(typeof response.body.summary.healthy).toBe('number');
  }, 30000);

  test('GET /api/service-alerts returns alerts array', async () => {
    const response = await request(app).get('/api/service-alerts').expect(200);
    expect(response.body).toHaveProperty('alerts');
    expect(Array.isArray(response.body.alerts)).toBe(true);
  }, 30000);

  test('GET /api/live-trains returns trains array', async () => {
    const response = await request(app)
      .get('/api/live-trains')
      .timeout({ deadline: 30000, response: 25000 })
      .expect(200);
    expect(response.body).toHaveProperty('trains');
    expect(Array.isArray(response.body.trains)).toBe(true);
  }, 30000);

  test('GET /api/journey-plan returns route options for known stations', async () => {
    const response = await request(app)
      .get('/api/journey-plan')
      .query({ from: 'Kings Cross', to: 'Victoria' })
      .timeout({ deadline: 30000, response: 25000 })
      .expect(200);

    expect(response.body).toHaveProperty('routes');
    expect(Array.isArray(response.body.routes)).toBe(true);
    expect(response.body.routes.length).toBeGreaterThan(0);
    expect(response.body.routes[0]).toHaveProperty('segments');
    expect(Array.isArray(response.body.routes[0].segments)).toBe(true);
  }, 30000);
});
