import runpy

if __name__ == "__main__":
    runpy.run_module(f"{__package__}.md_to_docx", run_name="__main__")
