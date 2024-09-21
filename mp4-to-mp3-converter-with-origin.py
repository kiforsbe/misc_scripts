import os
import sys
import platform
from moviepy.editor import VideoFileClip
import eyed3

def print_python_version():
    print(f"Python version: {platform.python_version()}")
    print(f"Running on: {platform.system()} {platform.release()}")
    print()

def get_file_origin_url(file_path):
    try:
        with open(file_path + ":Zone.Identifier", "r") as f:
            content = f.read()
        
        referrer_url = None
        host_url = None
        for line in content.split('\n'):
            if line.startswith("ReferrerUrl="):
                referrer_url = line.split('=', 1)[1]
            elif line.startswith("HostUrl="):
                host_url = line.split('=', 1)[1]
        
        return referrer_url, host_url
    
    except FileNotFoundError:
        return None, None
    except Exception as e:
        print(f"Error getting origin URL: {str(e)}")
        return None, None

def set_mp3_metadata(audio_file, filename, referrer_url, host_url):
    audiofile = eyed3.load(audio_file)
    if audiofile.tag is None:
        audiofile.initTag()

    # Check if filename contains artist information
    if " - " in filename:
        artist, title = filename.split(" - ", 1)
        audiofile.tag.artist = artist.strip()
        audiofile.tag.title = title.strip()
    else:
        audiofile.tag.artist = "Udio"
        audiofile.tag.title = filename.strip()

    # Add ReferrerUrl and HostUrl to comments
    comments = []
    if referrer_url:
        comments.append(f"ReferrerUrl: {referrer_url}")
    if host_url:
        comments.append(f"HostUrl: {host_url}")
    
    if comments:
        audiofile.tag.comments.set("\n".join(comments))

    audiofile.tag.save()

def convert_mp4_to_mp3_with_cover(input_file):
    try:
        # Get the directory and file name without extension
        file_dir = os.path.dirname(input_file)
        file_name = os.path.splitext(os.path.basename(input_file))[0]

        # Get origin URLs
        referrer_url, host_url = get_file_origin_url(input_file)

        # Extract audio from video
        video = VideoFileClip(input_file)
        audio = video.audio
        audio_file = os.path.join(file_dir, f"{file_name}.mp3")
        audio.write_audiofile(audio_file, verbose=False, logger=None)

        # Extract first frame as cover image at full resolution
        cover_image = os.path.join(file_dir, f"{file_name}_cover.jpg")
        video.save_frame(cover_image, t=0)

        # Add cover image and set metadata
        audiofile = eyed3.load(audio_file)
        if audiofile.tag is None:
            audiofile.initTag()
        audiofile.tag.images.set(3, open(cover_image, "rb").read(), "image/jpeg")
        audiofile.tag.save()

        # Set metadata (title, artist, and origin URLs)
        set_mp3_metadata(audio_file, file_name, referrer_url, host_url)

        # Clean up temporary files
        os.remove(cover_image)

        # Close video to release resources
        video.close()

        return True, None

    except Exception as e:
        return False, str(e)

def main():
    print_python_version()

    if len(sys.argv) < 2:
        print("Usage: Drag and drop MP4 files onto this script.")
        return

    print("Processing files...")
    print("-----------------")

    errors = []

    for input_file in sys.argv[1:]:
        if input_file.lower().endswith('.mp4'):
            print(f"Converting: {os.path.basename(input_file)} ... ", end='', flush=True)
            success, error = convert_mp4_to_mp3_with_cover(input_file)
            status = "Success" if success else "Fail"
            print(f"[{status}].")
            if not success:
                errors.append((input_file, error))
        else:
            print(f"Skipping: {os.path.basename(input_file)} (Not an MP4 file).")

    if errors:
        print("\nErrors encountered:")
        print("--------------------")
        for file, error in errors:
            print(f"File: {file}")
            print(f"Error: {error}")
            print()

    print("\nAll operations completed.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nAn unexpected error occurred: {str(e)}")
    finally:
        print("\nPress Enter to exit...")
        input()
