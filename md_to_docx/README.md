# md_to_docx

A script that converts Markdown files to Microsoft Word DOCX format. It processes Markdown content by first converting it to HTML using mistletoe, then parsing the HTML with BeautifulSoup to create properly formatted Word documents. The converter handles various Markdown elements including headings, paragraphs, lists (ordered and unordered with nesting), tables, bold/italic text, code blocks, links, and blockquotes. Tables are automatically formatted with proper styling and column widths.

The script can be used from the command line by specifying an input Markdown file and optionally an output DOCX file. If no output filename is provided, it will generate one based on the input filename and avoid overwriting existing files.

## Usage (Examples)
```bash
# Convert README.md to README.docx
python md_to_docx/md_to_docx.py README.md

# Convert with specific output filename
python md_to_docx/md_to_docx.py input.md output.docx
```

## Features
- Converts Markdown to properly formatted Word documents
- Handles headings (H1-H9), paragraphs, lists, and tables
- Supports inline formatting (bold, italic, code)
- Processes nested lists with appropriate indentation
- Formats tables with automatic column sizing and styling
- Generates debug HTML file for troubleshooting
- Automatic output filename generation to avoid overwriting

## Requires
- mistletoe
- beautifulsoup4
- python-docx
