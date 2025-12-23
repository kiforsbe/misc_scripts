"""Browser utilities for opening URLs in different ways across platforms."""

import os
import sys
import shutil
import subprocess
import webbrowser
import logging
from typing import List, Optional, Tuple, Dict, Any


class BrowserLauncher:
    """Cross-platform browser launcher with support for new windows."""
    
    def __init__(self):
        """Initialize the browser launcher."""
        self._cached_browser_info: Optional[Tuple[str, List[str]]] = None
        self._screen_dimensions: Optional[Tuple[int, int]] = None
    
    def get_screen_dimensions(self) -> Tuple[int, int]:
        """Get the primary screen dimensions.
        
        Returns:
            Tuple of (width, height) in pixels. Defaults to (1920, 1080) if detection fails.
        """
        if self._screen_dimensions:
            return self._screen_dimensions
        
        try:
            if sys.platform == 'win32':
                import ctypes
                user32 = ctypes.windll.user32
                width = user32.GetSystemMetrics(0)  # SM_CXSCREEN
                height = user32.GetSystemMetrics(1)  # SM_CYSCREEN
                self._screen_dimensions = (width, height)
                logging.debug(f"Screen dimensions: {width}x{height}")
                return (width, height)
            
            elif sys.platform == 'darwin':
                # macOS: Use system_profiler or defaults
                result = subprocess.run(
                    ['system_profiler', 'SPDisplaysDataType'],
                    capture_output=True, text=True, timeout=2
                )
                # Parse output for resolution (this is simplified)
                for line in result.stdout.split('\n'):
                    if 'Resolution' in line:
                        # Format: "Resolution: 1920 x 1080"
                        parts = line.split(':')
                        if len(parts) > 1:
                            dims = parts[1].strip().split('x')
                            if len(dims) >= 2:
                                width = int(dims[0].strip())
                                height = int(dims[1].strip().split()[0])  # Remove any trailing text
                                self._screen_dimensions = (width, height)
                                logging.debug(f"Screen dimensions: {width}x{height}")
                                return (width, height)
            
            else:
                # Linux: Try xrandr
                result = subprocess.run(
                    ['xrandr'], capture_output=True, text=True, timeout=2
                )
                for line in result.stdout.split('\n'):
                    if ' connected primary' in line or ' connected' in line:
                        # Format: "1920x1080+0+0"
                        parts = line.split()
                        for part in parts:
                            if 'x' in part and '+' in part:
                                dims = part.split('+')[0].split('x')
                                width = int(dims[0])
                                height = int(dims[1])
                                self._screen_dimensions = (width, height)
                                logging.debug(f"Screen dimensions: {width}x{height}")
                                return (width, height)
        
        except Exception as e:
            logging.debug(f"Failed to get screen dimensions: {e}")
        
        # Default fallback
        self._screen_dimensions = (1920, 1080)
        logging.debug("Using default screen dimensions: 1920x1080")
        return (1920, 1080)
    
    def get_browser_command(self) -> Optional[Tuple[str, List[str]]]:
        """Get the default browser executable and base arguments.
        
        Returns:
            Tuple of (browser_path, base_args) or None if not found
        """
        if self._cached_browser_info:
            return self._cached_browser_info
        
        try:
            # Get the default browser controller from webbrowser module
            browser = webbrowser.get()
            
            # Try to extract browser path from the controller
            if hasattr(browser, 'name'):
                browser_name = browser.name.lower()
                logging.debug(f"Default browser name: {browser.name}")
            else:
                browser_name = ''
            
            # Platform-specific browser detection
            if sys.platform == 'win32':
                result = self._get_windows_browser()
                if result:
                    self._cached_browser_info = result
                    return result
            
            elif sys.platform == 'darwin':
                result = self._get_macos_browser()
                if result:
                    self._cached_browser_info = result
                    return result
            
            else:
                result = self._get_linux_browser()
                if result:
                    self._cached_browser_info = result
                    return result
        
        except Exception as e:
            logging.debug(f"Browser detection failed: {e}")
        
        return None
    
    def _get_windows_browser(self) -> Optional[Tuple[str, List[str]]]:
        """Get default browser on Windows using registry.
        
        Returns:
            Tuple of (browser_path, base_args) or None
        """
        try:
            import winreg
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r'Software\Microsoft\Windows\Shell\Associations\UrlAssociations\http\UserChoice') as key:
                prog_id = winreg.QueryValueEx(key, 'ProgId')[0]
            
            with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, f'{prog_id}\\shell\\open\\command') as key:
                command = winreg.QueryValueEx(key, '')[0]
                # Extract executable path (usually in quotes)
                if '"' in command:
                    browser_path = command.split('"')[1]
                else:
                    browser_path = command.split()[0]
                
                if os.path.exists(browser_path):
                    logging.debug(f"Found browser via registry: {browser_path}")
                    return (browser_path, [])
        except Exception as e:
            logging.debug(f"Registry lookup failed: {e}")
        
        return None
    
    def _get_macos_browser(self) -> Optional[Tuple[str, List[str]]]:
        """Get default browser on macOS.
        
        Returns:
            Tuple of (browser_path, base_args) or None
        """
        # Check common browser paths on macOS
        common_browsers = [
            '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
            '/Applications/Firefox.app/Contents/MacOS/firefox',
            '/Applications/Safari.app/Contents/MacOS/Safari',
            '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
        ]
        for browser_path in common_browsers:
            if os.path.exists(browser_path):
                logging.debug(f"Found browser on macOS: {browser_path}")
                return (browser_path, [])
        
        return None
    
    def _get_linux_browser(self) -> Optional[Tuple[str, List[str]]]:
        """Get default browser on Linux.
        
        Returns:
            Tuple of (browser_path, base_args) or None
        """
        # Try xdg-settings first
        try:
            result = subprocess.run(['xdg-settings', 'get', 'default-web-browser'], 
                                  capture_output=True, text=True, timeout=2)
            if result.returncode == 0:
                desktop_file = result.stdout.strip()
                # Try to find the executable from .desktop file
                desktop_paths = [
                    f'/usr/share/applications/{desktop_file}',
                    f'~/.local/share/applications/{desktop_file}',
                ]
                for path in desktop_paths:
                    path = os.path.expanduser(path)
                    if os.path.exists(path):
                        with open(path, 'r') as f:
                            for line in f:
                                if line.startswith('Exec='):
                                    exec_line = line[5:].strip()
                                    browser_path = shutil.which(exec_line.split()[0])
                                    if browser_path:
                                        logging.debug(f"Found browser on Linux: {browser_path}")
                                        return (browser_path, [])
        except Exception as e:
            logging.debug(f"xdg-settings lookup failed: {e}")
        
        # Fallback: check common browser commands
        common_browsers = ['google-chrome', 'chromium', 'firefox', 'microsoft-edge']
        for browser_cmd in common_browsers:
            browser_path = shutil.which(browser_cmd)
            if browser_path:
                logging.debug(f"Found browser via which: {browser_path}")
                return (browser_path, [])
        
        return None
    
    def open_urls(self, urls: List[str], new_window: bool = False, 
                  popup: bool = False, maximized: bool = False,
                  window_size: Optional[Tuple[int, int]] = None,
                  window_position: Optional[Tuple[int, int]] = None) -> None:
        """Open URLs in the default browser.
        
        Args:
            urls: List of URLs to open
            new_window: If True, open each URL in a new browser window (ignored if popup=True or maximized=True)
            popup: If True, open in popup mode (chromeless, half screen width, centered)
            maximized: If True, open in maximized window
            window_size: Optional tuple of (width, height) for window size
            window_position: Optional tuple of (x, y) for window position
        """
        if not urls:
            logging.warning("No URLs found to open")
            return
        
        # Determine window mode (popup and maximized override new_window)
        if popup:
            new_window = False
            maximized = False
            screen_width, screen_height = self.get_screen_dimensions()
            window_width = screen_width // 2
            window_height = screen_height
            window_x = (screen_width - window_width) // 2
            window_y = 0
            window_size = (window_width, window_height)
            window_position = (window_x, window_y)
            logging.debug(f"Popup mode: size={window_size}, position={window_position}")
        elif maximized:
            new_window = False
            popup = False
            # Maximized mode - let browser handle it
            logging.debug("Maximized mode enabled")
        
        window_mode = "popup window" if popup else ("maximized window" if maximized else ("new window" if new_window else "browser"))
        logging.info(f"Opening {len(urls)} URL(s) in {window_mode}")
        print(f"Opening {len(urls)} URL(s) in {window_mode}:")
        
        # Get browser command if we need special handling
        browser_info = None
        if new_window or popup or maximized or window_size or window_position:
            browser_info = self.get_browser_command()
        
        for url in urls:
            print(f"  • {url}")
            try:
                if browser_info and (new_window or popup or maximized or window_size or window_position):
                    self._open_in_new_window(url, browser_info, window_size, window_position, popup, maximized)
                else:
                    # Default behavior: use webbrowser module
                    # new=1 opens in new window, new=0 opens in same window/new tab
                    webbrowser.open(url, new=1 if new_window else 0)
                    logging.debug(f"Opened URL: {url}")
            except Exception as e:
                logging.error(f"Error opening {url}: {e}")
        
        print(f"\n✓ Opened {len(urls)} URL(s) in default browser")
    
    def _open_in_new_window(self, url: str, browser_info: Tuple[str, List[str]],
                           window_size: Optional[Tuple[int, int]] = None,
                           window_position: Optional[Tuple[int, int]] = None,
                           is_popup: bool = False,
                           is_maximized: bool = False) -> None:
        """Open a URL in a new browser window.
        
        Args:
            url: URL to open
            browser_info: Tuple of (browser_path, base_args)
            window_size: Optional tuple of (width, height) for window size
            window_position: Optional tuple of (x, y) for window position
            is_popup: If True, optimize for popup window (chromeless, better sizing)
            is_maximized: If True, open in maximized window
        """
        browser_path, base_args = browser_info
        browser_name = os.path.basename(browser_path).lower()
        
        # Build browser-specific arguments
        args = [browser_path] + base_args
        
        # Determine the new-window flag and size/position based on browser
        if 'chrome' in browser_name or 'chromium' in browser_name or 'edge' in browser_name:
            if is_popup:
                # Use --app mode for popup - creates chromeless window that respects size/position
                args.append(f'--app={url}')
                if window_size:
                    args.append(f'--window-size={window_size[0]},{window_size[1]}')
                if window_position:
                    args.append(f'--window-position={window_position[0]},{window_position[1]}')
            elif is_maximized:
                # Open in maximized window
                args.extend(['--new-window', '--start-maximized', url])
            elif window_size or window_position:
                # Regular new window with custom size/position
                args.append('--new-window')
                if window_size:
                    args.append(f'--window-size={window_size[0]},{window_size[1]}')
                if window_position:
                    args.append(f'--window-position={window_position[0]},{window_position[1]}')
                args.append(url)
            else:
                # Just new window, no size control
                args.extend(['--new-window', url])
                
        elif 'firefox' in browser_name:
            args.append('--new-window')
            if window_size and not is_maximized:
                args.append(f'--width={window_size[0]}')
                args.append(f'--height={window_size[1]}')
            # Firefox doesn't have a maximized flag via command line
            args.append(url)
        elif 'safari' in browser_name:
            # Safari on macOS - size/position would need AppleScript
            args = ['open', '-n', '-a', browser_path, url]
        else:
            # Generic fallback
            args.append(url)
        
        subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        mode = "popup" if is_popup else ("maximized" if is_maximized else "new window")
        logging.debug(f"Opened URL in {mode}: {url} (size={window_size}, pos={window_position})")


def open_urls_in_browser(urls: List[str], new_window: bool = False,
                        popup: bool = False, maximized: bool = False,
                        window_size: Optional[Tuple[int, int]] = None,
                        window_position: Optional[Tuple[int, int]] = None) -> None:
    """Convenience function to open URLs in the default browser.
    
    Args:
        urls: List of URLs to open
        new_window: If True, open each URL in a new browser window
        popup: If True, open in popup mode (chromeless, half screen width, centered)
        maximized: If True, open in maximized window
        window_size: Optional tuple of (width, height) for window size
        window_position: Optional tuple of (x, y) for window position
    """
    launcher = BrowserLauncher()
    launcher.open_urls(urls, new_window, popup, maximized, window_size, window_position)
