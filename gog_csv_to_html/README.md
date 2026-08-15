# gog_csv_to_html

A Python script that converts GOG Galaxy CSV export files into a modern, interactive HTML game library viewer. It creates a responsive web application with game browsing, filtering, search functionality, rich media integration, and advanced AI-powered game analysis including clustering visualization and similarity recommendations.

The script automatically fetches additional media content from online sources and caches it locally for improved performance. It provides a professional game library interface similar to modern gaming platforms, with detailed game information, ratings, playtime tracking, visual elements, and intelligent game analysis features.

## Features
- **Modern Interactive Interface**: Responsive React-based web application with dual-pane layout
- **Rich Media Integration**:
  - Automatic YouTube trailer and gameplay video embedding
  - Game screenshot galleries with carousel navigation and modal view
  - Cover art and background images from game metadata
- **Advanced Filtering & Search**:
  - Real-time search across titles, descriptions, genres, and tags
  - Filter by played/unplayed games and recently played titles
  - Platform-based filtering and sorting options
- **Game Information Display**:
  - Comprehensive game details including playtime, ratings, release dates
  - Developer/publisher information and genre classifications
  - Platform badges and compatibility information
  - Purchase and last played date tracking
- **AI-Powered Game Analysis** (Enhanced):
  - **14-axis game scoring system** using Ollama and deepseek-r1 model for analyzing:
    - Core mechanics complexity and count
    - Player agency and world impact
    - Narrative density and integration
    - Scope, pacing, and replayability
    - Technical execution and aesthetics
  - **Interactive cluster visualization** using t-SNE and K-means clustering
  - **Game similarity recommendations** based on comprehensive vector analysis
  - **Visual axis comparison** in compact grid format for quick game assessment
- **Machine Learning Features**:
  - **MiniLM text embeddings** for semantic game similarity analysis
  - **Hybrid vector space** combining structured axis data with semantic embeddings
  - **Real-time clustering** with meaningful cluster naming and analysis
  - **Intelligent game recommendations** using euclidean distance in high-dimensional space
- **Performance Optimizations**:
  - Local SQLite database for media content and axis scoring caching
  - React virtualization for smooth scrolling of large game libraries
  - Lazy loading of images and content
  - Standardized vector preprocessing for improved clustering results
- **Professional Presentation**:
  - Modern gradient backgrounds and card-based layouts
  - Star rating displays and playtime formatting
  - Responsive design for desktop and mobile viewing
  - Bootstrap-based styling with custom enhancements
- **Game Analysis Integration** (Optional):
  - AI-powered game axis scoring using Ollama and deepseek-r1 model
  - 14-axis game comparison system for analyzing game mechanics, narrative, and design
  - Cached scoring results for improved performance on repeated runs

## Usage (Examples)
```bash
# Convert CSV to HTML with full AI analysis (recommended)
python gog_csv_to_html/gog_csv_to_html.py gog_export.csv

# Convert with custom output filename
python gog_csv_to_html/gog_csv_to_html.py gog_export.csv -o my_game_library.html

# Skip media fetching for faster processing (disables AI features)
python gog_csv_to_html/gog_csv_to_html.py gog_export.csv --no-media

# Disable media caching
python gog_csv_to_html/gog_csv_to_html.py gog_export.csv --no-cache

# Open result in browser automatically
python gog_csv_to_html/gog_csv_to_html.py gog_export.csv --open

# Show cache statistics including AI analysis data
python gog_csv_to_html/gog_csv_to_html.py --cache-stats

# Use custom Ollama host for AI analysis
python gog_csv_to_html/gog_csv_to_html.py gog_export.csv --ollama-host http://192.168.1.100:11434
```

## AI Analysis Features
The script includes sophisticated AI-powered game analysis:

- **Axis Scoring**: Each game is analyzed across 14 dimensions using the deepseek-r1 model
- **Cluster Analysis**: Games are automatically grouped using machine learning clustering algorithms
- **Similarity Engine**: Find games similar to your favorites using hybrid semantic + structured analysis
- **Visual Analytics**: Interactive t-SNE plots show your game library's structure and patterns

## Template Dependency
- **gog_csv_to_html_template.html**: Jinja2 template file containing the HTML structure, CSS styling, and React-based JavaScript application with ML clustering features. Must be present in the `webapp/` subfolder next to the Python script.
- **gog_csv_to_html_template.css**: CSS styling file with responsive design and clustering modal styles.

## Requires
- requests
- beautifulsoup4
- jinja2
- **ollama** (for AI-powered game axis scoring - requires deepseek-r1 model)
- **pydantic** (for structured output validation with Ollama)

### Optional Dependencies
- **Image Processing**: gzip, zlib, brotli (for handling compressed web responses)
- **Web Browser Integration**: webbrowser (standard library, for --open flag)
- **AI Game Analysis**: Ollama server with deepseek-r1 model (for axis scoring and clustering features)

## Setup for AI Features
To enable the full AI analysis capabilities:

1. **Install Ollama**: Download from [ollama.ai](https://ollama.ai)
2. **Install deepseek-r1 model**: `ollama pull deepseek-r1`
3. **Start Ollama server**: `ollama serve`
4. **Run with AI features**: `python gog_csv_to_html/gog_csv_to_html.py your_games.csv`

The script will automatically detect Ollama availability and enable advanced features when the server and model are accessible.
