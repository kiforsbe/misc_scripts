import win32clipboard
import winsound
from pathlib import Path
import logging
import traceback
import asyncio
import sys

# Set up logging
logging.basicConfig(filename='clipboard_monitor.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_clipboard_data():
    try:
        win32clipboard.OpenClipboard()
        try:
            data = win32clipboard.GetClipboardData()
        except win32clipboard.error:
            data = ""
            log_error("Failed to get clipboard data")
        finally:
            win32clipboard.CloseClipboard()
    except Exception as e:
        data = ""
        log_error(f"Error accessing clipboard: {str(e)}")
    return data

def log_error(message):
    print(f"ERROR: {message}", file=sys.stderr)
    logging.error(f"{message}\n{traceback.format_exc()}")

def play_sound():
    try:
        winsound.PlaySound("SystemAsterisk", winsound.SND_ALIAS | winsound.SND_ASYNC)
    except Exception as e:
        log_error(f"Error playing sound: {str(e)}")

async def main():
    log_file = Path(__file__).parent / "clipboard.csv"
    previous_data = get_clipboard_data()  # Initialize with current clipboard content

    print("Clipboard monitor started. Press Ctrl+C to stop.")

    while True:
        try:
            current_data = get_clipboard_data()
            if current_data and current_data != previous_data:
                try:
                    with log_file.open("a", encoding="utf-8") as f:
                        f.write(f"{current_data}\n")
                    play_sound()
                    print(f"New clipboard content logged: {current_data[:50]}...")
                    previous_data = current_data
                except Exception as e:
                    log_error(f"Error writing to log file: {str(e)}")
        except KeyboardInterrupt:
            print("\nClipboard monitor stopped.")
            break
        except Exception as e:
            log_error(f"Unexpected error: {str(e)}")
        
        await asyncio.sleep(0.1)  # 0.1 second polling interval

if __name__ == "__main__":
    asyncio.run(main())
