import argparse
import os
import sys
import time
import xml.etree.ElementTree as ET
import urllib.request
import urllib.parse
from typing import List, Dict, Tuple, Optional
import curses
import logging  # Added for logging
import base64  # Added for encoding credentials
import html  # Added for decoding HTML entities
import json  # Added for saving the mapping file

# Configure logging
LOG_FILE = "rss_feed_downloader.log"
logging.basicConfig(
    level=logging.WARNING,  # Default log level for file
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stderr)
    ]
)
console_handler = logging.StreamHandler(sys.stderr)
console_handler.setLevel(logging.ERROR)  # Log only errors to console
logging.getLogger().addHandler(console_handler)

def sanitize_filename(filename: str) -> str:
    """
    Sanitize a string to create a valid Windows filename.
    
    Args:
        filename: The original filename
    
    Returns:
        A sanitized filename
    """
    invalid_chars = '<>:"/\\|?*'
    sanitized = ''.join(c if c not in invalid_chars else '_' for c in filename)
    return sanitized.strip()

def get_file_extension(mime_type: str) -> str:
    """
    Map MIME types to common file extensions.
    
    Args:
        mime_type: The MIME type of the file
    
    Returns:
        A file extension (without the dot)
    """
    mime_to_extension = {
        "audio/mpeg": "mp3",
        "audio/mp4": "m4a",
        "audio/x-wav": "wav",
        "audio/ogg": "ogg",
        "video/mp4": "mp4",
        "video/x-matroska": "mkv",
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "application/pdf": "pdf",
        "text/plain": "txt",
        # Add more mappings as needed
    }
    return mime_to_extension.get(mime_type, "bin")  # Default to "bin" if unknown

def parse_rss_feed(feed_path: str) -> List[Dict]:
    """
    Parse an RSS feed from file or URL and extract items with enclosures.
    
    Args:
        feed_path: Path to local RSS file or URL of RSS feed
    
    Returns:
        List of dictionaries containing item information
    """
    try:
        logging.info(f"Parsing RSS feed from {feed_path}")
        # Check if feed_path is a URL or local file
        if feed_path.startswith(('http://', 'https://')):
            with urllib.request.urlopen(feed_path) as response:
                rss_content = response.read()
        else:
            with open(feed_path, 'rb') as f:
                rss_content = f.read()
        
        # Parse the XML content
        root = ET.fromstring(rss_content)
        
        # Find the channel element
        channel = root.find('channel')
        if channel is None:
            raise ValueError("Invalid RSS format: no channel element found")
        
        # Extract items with enclosures
        items = []
        for item in channel.findall('item'):
            title_elem = item.find('title')
            title = title_elem.text if title_elem is not None else "Untitled"
            
            enclosure = item.find('enclosure')
            if enclosure is not None and 'url' in enclosure.attrib:
                url = enclosure.attrib['url']
                # Decode HTML entities in the URL (e.g., &amp; -> &)
                url = html.unescape(url)
                length = int(enclosure.attrib.get('length', 0))
                file_type = enclosure.attrib.get('type', 'unknown')
                
                # Generate a valid filename based on the title and file type
                extension = get_file_extension(file_type)
                sanitized_title = sanitize_filename(title)
                filename = f"{sanitized_title}.{extension}"
                
                items.append({
                    'title': title,
                    'url': url,  # Use the decoded URL
                    'filename': filename,
                    'size': length,
                    'type': file_type,
                    'selected': True  # Default selected
                })
        
        return items
    except Exception as e:
        logging.error(f"Error parsing RSS feed: {e}")
        sys.exit(1)

def get_file_size_str(size_bytes: int) -> str:
    """Convert file size in bytes to human-readable format"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes/(1024*1024):.1f} MB"
    else:
        return f"{size_bytes/(1024*1024*1024):.1f} GB"

def download_file(url: str, dest_path: str, progress_callback=None, auth: Optional[Tuple[str, str]] = None) -> bool:
    """
    Download a file from URL to destination path with progress reporting and optional authentication.
    
    Args:
        url: URL to download
        dest_path: Destination file path
        progress_callback: Function to call with progress updates
        auth: Tuple containing (username, password) for HTTP Basic Authentication
        
    Returns:
        True if download successful, False otherwise
    """
    try:
        logging.info(f"Starting download: {url} -> {dest_path}")
        
        # Add headers to the request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://www.patreon.com/"
        }
        
        # Add authentication header if credentials are provided
        if auth:
            username, password = auth
            credentials = f"{username}:{password}"
            encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
            headers["Authorization"] = f"Basic {encoded_credentials}"
            logging.info("Using HTTP Basic Authentication")

        request = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(request) as response:
            file_size = int(response.info().get('Content-Length', 0))
            downloaded = 0
            chunk_size = 8192
            
            with open(dest_path, 'wb') as f:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    if progress_callback:
                        progress = (downloaded / file_size) if file_size > 0 else 0
                        progress_callback(progress)
                    
        logging.info(f"Download completed: {dest_path}")
        return True
    except Exception as e:
        logging.error(f"Error downloading {url}: {e}")
        return False

def console_gui(stdscr, items: List[Dict]) -> List[Dict]:
    """
    Console-based GUI for selecting items to download.
    
    Args:
        stdscr: curses window object
        items: List of items with enclosures
        
    Returns:
        List of selected items
    """
    try:
        logging.info("Starting console GUI for item selection")
        curses.curs_set(0)  # Hide cursor
        stdscr.clear()
        
        # Colors setup
        curses.start_color()
        curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLACK)  # Normal
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_WHITE)  # Selected
        curses.init_pair(3, curses.COLOR_GREEN, curses.COLOR_BLACK)  # Selected item
        curses.init_pair(4, curses.COLOR_YELLOW, curses.COLOR_BLACK)  # Instructions
        
        current_pos = 0
        offset = 0
        
        while True:
            stdscr.clear()
            height, width = stdscr.getmaxyx()
            
            # Calculate visible range
            max_items = height - 6  # Reserve 6 lines for header and footer
            start_idx = offset
            end_idx = min(start_idx + max_items, len(items))
            
            # Header
            header = f" RSS Feed Downloader - {sum(1 for item in items if item['selected'])}/{len(items)} items selected"
            stdscr.addstr(0, 0, header, curses.color_pair(1) | curses.A_BOLD)
            stdscr.addstr(1, 0, "=" * (width - 1), curses.color_pair(1))
            
            # Print items
            for i in range(start_idx, end_idx):
                item = items[i]
                y_pos = i - start_idx + 2
                
                # Highlight current position
                attr = curses.color_pair(2) if i == current_pos else curses.color_pair(1)
                
                # Selection indicator
                select_char = "[×]" if item['selected'] else "[ ]"
                select_attr = curses.color_pair(3) if item['selected'] else curses.color_pair(1)
                
                # Truncate title if needed
                title = item['title']
                max_title_len = width - 30
                if len(title) > max_title_len:
                    title = title[:max_title_len-3] + "..."
                
                # Format display string
                size_str = get_file_size_str(item['size'])
                display = f" {select_char} {title} ({size_str})"
                
                # Print item
                stdscr.addstr(y_pos, 0, select_char, select_attr | curses.A_BOLD)
                stdscr.addstr(y_pos, 4, f"{title} ({size_str})", attr)
            
            # Footer with instructions
            footer_y = height - 3
            stdscr.addstr(footer_y - 1, 0, "=" * (width - 1), curses.color_pair(1))
            stdscr.addstr(footer_y, 0, " Space: Toggle | A: Select All | D: Deselect All | Enter: Download | Q: Quit", 
                        curses.color_pair(4))
            
            # Handle user input
            stdscr.refresh()
            key = stdscr.getch()
            
            if key == ord('q') or key == ord('Q'):
                return []  # Return empty list to cancel
            
            elif key == ord(' '):  # Toggle selection
                items[current_pos]['selected'] = not items[current_pos]['selected']
            
            elif key == ord('a') or key == ord('A'):  # Select all
                for item in items:
                    item['selected'] = True
            
            elif key == ord('d') or key == ord('D'):  # Deselect all
                for item in items:
                    item['selected'] = False
            
            elif key == curses.KEY_UP:  # Move up
                if current_pos > 0:
                    current_pos -= 1
                    if current_pos < offset:
                        offset = current_pos
            
            elif key == curses.KEY_DOWN:  # Move down
                if current_pos < len(items) - 1:
                    current_pos += 1
                    if current_pos >= offset + max_items:
                        offset = current_pos - max_items + 1
            
            elif key == curses.KEY_ENTER or key == 10 or key == 13:  # Enter key
                return [item for item in items if item['selected']]
    except Exception as e:
        logging.error(f"Error in console GUI: {e}")
        raise

def download_gui(stdscr, items: List[Dict], output_dir: str) -> None:
    """
    Console-based GUI for downloading files with progress display.
    
    Args:
        stdscr: curses window object
        items: List of items to download
        output_dir: Directory to save downloaded files
    """
    try:
        logging.info(f"Starting download GUI for {len(items)} items")
        curses.curs_set(0)  # Hide cursor
        stdscr.clear()
        
        # Colors setup
        curses.start_color()
        curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLACK)  # Normal
        curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)  # Success
        curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK)  # In progress
        curses.init_pair(4, curses.COLOR_RED, curses.COLOR_BLACK)    # Error
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Prepare mapping file
        mapping_file_path = os.path.join(output_dir, "downloaded_files.json")
        downloaded_files = []

        height, width = stdscr.getmaxyx()
        
        # Header
        header = f" RSS Feed Downloader - Downloading {len(items)} items"
        stdscr.addstr(0, 0, header, curses.color_pair(1) | curses.A_BOLD)
        stdscr.addstr(1, 0, "=" * (width - 1), curses.color_pair(1))
        
        # Main progress bar
        main_progress_y = 2
        stdscr.addstr(main_progress_y, 0, f" Total progress: 0/{len(items)} (0%)", curses.color_pair(1))
        
        # Download each item
        for idx, item in enumerate(items):
            progress_y = 4
            
            # Update overall progress
            progress_pct = int((idx / len(items)) * 100)
            stdscr.addstr(main_progress_y, 0, f" Total progress: {idx}/{len(items)} ({progress_pct}%)", curses.color_pair(1))
            
            # Draw progress bar
            bar_width = width - 20
            filled = int(bar_width * idx / len(items))
            stdscr.addstr(main_progress_y + 1, 0, " [" + "=" * filled + " " * (bar_width - filled) + "]", curses.color_pair(1))
            
            # Current file info
            file_info = f" Downloading: {item['title']} ({get_file_size_str(item['size'])})"
            stdscr.addstr(progress_y, 0, file_info, curses.color_pair(3))
            
            # Item list with status
            list_start_y = 7
            visible_items = min(len(items), height - list_start_y - 1)
            
            for i in range(visible_items):
                item_idx = i
                if item_idx < len(items):
                    status = ""
                    attr = curses.color_pair(1)
                    
                    if item_idx < idx:
                        status = "[DONE]"
                        attr = curses.color_pair(2)
                    elif item_idx == idx:
                        status = "[DOWNLOADING]"
                        attr = curses.color_pair(3)
                    else:
                        status = "[PENDING]"
                    
                    # Truncate title if needed
                    title = items[item_idx]['title']
                    max_title_len = width - len(status) - 5
                    if len(title) > max_title_len:
                        title = title[:max_title_len-3] + "..."
                    
                    # Format display string
                    display = f" {title}"
                    
                    # Print item with status
                    stdscr.addstr(list_start_y + i, 0, display, curses.color_pair(1))
                    stdscr.addstr(list_start_y + i, width - len(status) - 1, status, attr)
            
            stdscr.refresh()
            
            # Download the file with progress updates
            dest_path = os.path.join(output_dir, item['filename'])
            
            def update_progress(progress):
                bar_width = width - 20
                filled = int(bar_width * progress)
                progress_text = f" Progress: {int(progress * 100)}%"
                stdscr.addstr(progress_y + 1, 0, progress_text, curses.color_pair(3))
                stdscr.addstr(progress_y + 2, 0, " [" + "=" * filled + " " * (bar_width - filled) + "]", curses.color_pair(3))
                stdscr.refresh()
            
            success = download_file(item['url'], dest_path, update_progress)
            
            # Update status and save mapping
            if success:
                stdscr.addstr(progress_y, 0, f" Downloaded: {item['title']}", curses.color_pair(2))
                downloaded_files.append({
                    "title": item['title'],
                    "url": item['url'],
                    "filename": item['filename'],
                    "size": item['size'],
                    "type": item['type'],
                    "path": os.path.abspath(dest_path)
                })
            else:
                stdscr.addstr(progress_y, 0, f" Failed: {item['title']}", curses.color_pair(4))
            
            stdscr.refresh()
            time.sleep(0.5)  # Short pause to show completion status

        # Save mapping file
        try:
            with open(mapping_file_path, 'w', encoding='utf-8') as f:
                json.dump(downloaded_files, f, ensure_ascii=False, indent=4)
            logging.info(f"Mapping file saved: {mapping_file_path}")
        except Exception as e:
            logging.error(f"Error saving mapping file: {e}")
        
        # Final status
        stdscr.addstr(main_progress_y, 0, f" Total progress: {len(items)}/{len(items)} (100%)", curses.color_pair(2))
        filled = width - 20
        stdscr.addstr(main_progress_y + 1, 0, " [" + "=" * filled + "]", curses.color_pair(2))
        
        # Complete message
        stdscr.addstr(height - 2, 0, " Download complete! Press any key to exit.", curses.color_pair(2) | curses.A_BOLD)
        stdscr.refresh()
        stdscr.getch()
        logging.info("Download GUI completed")
    except Exception as e:
        logging.error(f"Error in download GUI: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="RSS Feed Downloader with Console GUI")
    parser.add_argument("feed", help="Path to local RSS file or URL of RSS feed")
    parser.add_argument("-o", "--output-dir", default="downloads", help="Directory to save downloaded files")
    parser.add_argument("-l", "--log-level", default="WARNING", help="Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    parser.add_argument("-u", "--username", help="Username for HTTP Basic Authentication")
    parser.add_argument("-p", "--password", help="Password for HTTP Basic Authentication")
    args = parser.parse_args()

    # Set log level based on user input
    log_level = getattr(logging, args.log_level.upper(), logging.WARNING)
    logging.getLogger().setLevel(log_level)
    logging.info(f"Log level set to {args.log_level.upper()}")

    # Prepare authentication tuple if credentials are provided
    auth = None
    if args.username and args.password:
        auth = (args.username, args.password)
        logging.info("Authentication credentials provided")

    try:
        logging.info(f"Parsing RSS feed from {args.feed}")
        # Parse the RSS feed
        print(f"Parsing RSS feed from {args.feed}...")
        items = parse_rss_feed(args.feed)
        
        if not items:
            print("No enclosures found in the RSS feed.")
            return
        
        print(f"Found {len(items)} items with enclosures.")
        print("Starting selection interface...")
        
        # Start the curses application for selection
        try:
            selected_items = curses.wrapper(console_gui, items)
            
            if not selected_items:
                print("No items selected, exiting.")
                return
            
            print(f"Selected {len(selected_items)} items for download.")
            
            # Start download interface
            curses.wrapper(download_gui, selected_items, args.output_dir)
            
            print(f"Downloads complete! Files saved to {os.path.abspath(args.output_dir)}")
        
        except KeyboardInterrupt:
            logging.warning("Operation cancelled by user")
            print("\nOperation cancelled by user.")
        
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            print(f"An error occurred: {e}")

        logging.info("Program completed successfully")
    except KeyboardInterrupt:
        logging.warning("Operation cancelled by user")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Exiting program")

if __name__ == "__main__":
    main()
