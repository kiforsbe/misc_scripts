from subsai import SubsAI
import os
import sys
import getopt
import glob

def getOpts(argv):
    # Set some output defaults
    inputPath = ""
    outputPath = "out"
    hfToken = ""
    verbose=False

    # Check command line args, and if available, override the defaults
    try:
        opts, args = getopt.getopt(argv, "hi:o:t:v", ["input=", "output=", "token="])
    except getopt.GetoptError:
        print(r"transcribe.py [-i <inputPath>] [-o <outputPath>] [-v]")
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
        elif opt in ("-t", "--token"):
            hfToken = arg
        elif opt in ("-v"):
            verbose = True
    
    # Return final output, either defaults or overridden ones
    return inputPath, outputPath, hfToken, verbose

def printHelp():
    print(r"transcribe.py [-i <inputPath>] [-o <outputPath>] [-t <hf_token>] [-v]")
    print(r"")
    print(r"Transcribe all specified media files to .srt.")
    print(r"")
    print(r"Command-line options:")
    print(r"-i path")
    print(r"--input=path")
    print(r"    Path to files to operate on. Wildcards can be applied. [default=''].")
    print(r"")
    print(r"-o path")
    print(r"--output=path")
    print(r"    Path to output of files [default='out/'].")
    print(r"")
    print(r"-t token")
    print(r"--token=hf_token")
    print(r"    Token for speaker diarization [default=''].")
    print(r"")
    print(r"-v")
    print(r"    Enable verbose output.")

def main(argv):
    # Get options
    inputPath, outputPath, hfToken, verbose = getOpts(argv)

    # Normalize paths
    inputPath = os.path.normpath(inputPath)
    outputPath = os.path.normpath(outputPath)

    # If output path does not exist, create it
    if not os.path.exists(outputPath):
        os.mkdir(outputPath)

    # Get list of all files mathing inputPath (can use wild cards, e.g. "inputpath\*.mp4")
    files = glob.glob(inputPath)

    # Create instance of the model
    subs_ai = SubsAI()
    if len(hfToken) > 0:
        model = subs_ai.create_model('m-bain/whisperX', {'model_type': 'base',
                                                        'device': 'cuda',
                                                        'language': 'en',
                                                        'speaker_labels': True,
                                                        'min_speakers': 1,
                                                        'HF_TOKEN': hfToken})
    else:
        model = subs_ai.create_model('m-bain/whisperX', {'model_type': 'base',
                                                        'device': 'cuda',
                                                        'language': 'en'})

    # Execute operation on each file
    for file in files:
        outputFile = os.path.join(outputPath, f"{os.path.splitext(os.path.basename(file))[0]}.srt")

        # Transcribe the file
        if(verbose): print(f"Transcribing: '{file}'... ", end="", flush=True)

        # Make the transcription
        subs = subs_ai.transcribe(file, model)

        # Save the transcription
        subs.save(outputFile)
        
        if(verbose): print(f"Done")

if __name__ == "__main__":
    main(sys.argv[1:])

