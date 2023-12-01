# misc_scripts
Miscellaneous scripts to automate common tasks.

## srt_to_transcript.py
Saves contents of the specified `.srt` files to a plain text trasncripts.

### Requires
- srt

## transcribe_to_srt.py
Transcribes the specified media files such as `.mkv` to `.srt` subtitles.
Defaults to model `m-bain/whisperX` and language `English` (`"en"`) for transcription.
The model should automatically download and isntall when the script is run.

### Requires
- SubsAI (https://github.com/abdeladim-s/subsai)
  - Model: m-bain/whisperX

### Recommended
Torch with CUDA support is highly recommended if you have a CUDA capable machine. For SubsAI with `torch-2.0.1` requirement, install `torch-2.0.1+cu118` per instruction https://pytorch.org/get-started/previous-versions/#v201 instead of default one in SubsAI `requirements.txt` file.
