import runpy

# Defaults to the CLI. Run the GUI or Flask webservice explicitly:
#   python -m youtube-video-downloader.youtube-video-downloader-gui
#   python -m youtube-video-downloader.youtube-video-downloader-flask-ws
if __name__ == "__main__":
    runpy.run_module(f"{__package__}.youtube-video-downloader-cli", run_name="__main__")
