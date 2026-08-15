import runpy

if __name__ == "__main__":
    runpy.run_module(f"{__package__}.file_metadata_scanner", run_name="__main__")
