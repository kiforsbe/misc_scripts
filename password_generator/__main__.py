import runpy

if __name__ == "__main__":
    runpy.run_module(f"{__package__}.password_generator", run_name="__main__")
