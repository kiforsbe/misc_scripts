import sys
import os
from pydub import AudioSegment

def convert_and_append(input_paths):
    if not input_paths:
        print("No input files provided.")
        return

    # Get the directory of the first input file
    output_dir = os.path.dirname(input_paths[0])
    output_path = os.path.join(output_dir, "combined_output.mp3")

    combined = AudioSegment.empty()
    
    for input_path in input_paths:
        if not input_path.lower().endswith('.ogg'):
            print(f"Skipping {input_path}: Not an OGG file")
            continue
        
        try:
            audio = AudioSegment.from_ogg(input_path)
            combined += audio
            print(f"Processed: {input_path}")
        except Exception as e:
            print(f"Error processing {input_path}: {str(e)}")
    
    combined.export(output_path, format="mp3")
    print(f"All files have been converted and appended to: {output_path}")

def main():
    if len(sys.argv) < 2:
        print("Usage: Drag and drop OGG files onto this script")
        return

    convert_and_append(sys.argv[1:])

if __name__ == "__main__":
    main()
