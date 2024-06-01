import srt
import os
import sys
import getopt
import glob

def getOpts(argv):
    # Set some output defaults
    inputPath = "*.srt"
    outputPath = "out"
    verbose=False

    # Check command line args, and if available, override the defaults
    try:
        opts, args = getopt.getopt(argv, "hi:o:v", ["input=", "output="])
    except getopt.GetoptError:
        print(r"srt_to_transcript.py [-i <inputPath>] [-o <outputPath>] [-v]")
        sys.exit(2)

    # Walk through each option and arg
    for opt, arg in opts:
        if opt == '-h':
            printHelp()
            sys.exit()
        elif opt in ("-i", "--input"):
            inputPath = arg
        elif opt in ("-o", "--output"):
            outputPath = arg
        elif opt in ("-v"):
            verbose = True
    
    # Return final output, either defaults or overridden ones
    return inputPath, outputPath, verbose

def printHelp():
    print(r"srt_to_transcript.py [-i <inputPath>] [-o <outputPath>] [-f <ffmpegPath>] [-v]")
    print(r"")
    print(r"Batch convert all specified .srt files to .txt.")
    print(r"")
    print(r"Command-line options:")
    print(r"-i path")
    print(r"--input=path")
    print(r"    Path to files to operate on. Wildcards can be applied. [default=*.ogg].")
    print(r"")
    print(r"-o path")
    print(r"--output=path")
    print(r"    Path to output of files [default='out/'].")
    print(r"")
    print(r"-v")
    print(r"    Enable verbose output.")

def main(argv):
    # Get options
    #inputPath, outputPath, verbose = getOpts(argv)
    inputPath = r"J:\Recordings\subs4\2024-05-22 15-23 Deep dive HMI.srt"
    outputPath = "out"
    verbose = True

    # Normalize paths
    inputPath = os.path.normpath(inputPath)
    outputPath = os.path.normpath(outputPath)

    # If output path does not exist, create it
    if not os.path.exists(outputPath):
        os.mkdir(outputPath)

    # Get list of all files mathing inputPath (can use wild cards, e.g. "inputpath\*.srt")
    files = glob.glob(inputPath)

    # Execute operation on each file
    for file in files:
        outputFile = os.path.join(outputPath, f"{os.path.splitext(os.path.basename(file))[0]}.txt")
        subtitles = []

        # Strip the srt and combine sentences
        if(verbose): print(f"Transcribing: '{file}'... ", end="", flush=True)

        # Read subtitles and save stipped version
        with open(file) as fp:
            subtitle_generator = srt.parse(fp)
            subtitles = list(subtitle_generator)

        # Initialize an empty list for the unique subtitles
        unique_subtitles = []

        # Iterate over the subtitles
        for i in range(len(subtitles)):
            # If it's the last subtitle or the current subtitle's content doesn't match the next one's
            if i == len(subtitles) - 1 or subtitles[i].content != subtitles[i+1].content:
                # Add the subtitle to the unique_subtitles list
                unique_subtitles.append(subtitles[i])

        # Now unique_subtitles contains the subtitles without consecutive duplicates
        subtitles = unique_subtitles
            
        # Write subtitles to plain text file
        with open(outputFile, 'w') as fp:
            for item in subtitles:
                # write each item on a new line
                fp.write("%s\n" % item.content)
        
        if(verbose): print(f"Done")

if __name__ == "__main__":
    main(sys.argv[1:])

