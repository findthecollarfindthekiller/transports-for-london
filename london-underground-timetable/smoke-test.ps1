$urls = @(
  'http://localhost:3000/api/lines',
  'http://localhost:3000/api/line/bakerloo/status',
  'http://localhost:3000/api/stoppoint/search?query=Bank',
  'http://localhost:3000/api/live-trains'
)

foreach ($u in $urls) {
  Write-Host "Testing $u"
  try {
    $code = curl.exe -sS -o NUL -w "%{http_code}" $u
    Write-Host "HTTP $code"
  } catch {
    Write-Host "ERROR $($_.Exception.Message)"
  }
  Write-Host '---'
}
