import runpy

if __name__ == "__main__":
    runpy.run_module(f"{__package__}.mini-dlna-server", run_name="__main__")
