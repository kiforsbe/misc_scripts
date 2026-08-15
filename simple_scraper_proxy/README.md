# simple_scraper_proxy

A standalone scraping proxy that fetches an upstream HTML page, extracts feed data using a local YAML selector template, and returns an RSS 2.0 feed.

## Features
- Keeps the original `simple_http_proxy.py` untouched while reusing its simple local server model
- Fetches a remote HTML page and parses it with BeautifulSoup
- Loads scraping rules from local YAML templates in `simple_scraper_proxy_templates`
- Renders RSS 2.0 output with an `atom:self` link and optional `nyaa:*` namespaced fields
- Includes an example Nyaa template that maps torrent table rows into RSS items

## Usage Examples
```bash
# Start the scraper proxy
python simple_scraper_proxy/simple_scraper_proxy.py --port 8081

# Request RSS using the bundled Nyaa template
http://localhost:8081/?url=<url>&template=nyaa_rss
```

## Requires
- beautifulsoup4
- PyYAML
