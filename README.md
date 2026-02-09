# PDF Reader

Simple PDF reader and metadata extractor for thesis projects.

## Features

- **Read PDF content** - Extract full text from academic papers
- **Extract metadata** - Get title, author, and abstract from PDFs
- **Excel export** - Batch process all PDFs to CSV file

## Usage

```bash
# Show available PDF files and options
uv run python pdf_reader.py

# Read PDF content (original functionality)
uv run python pdf_reader.py "filename.pdf" [--first5|--full]

# Extract metadata from single PDF
uv run python pdf_reader.py --info "filename.pdf"

# Export all PDFs to CSV for Excel
uv run python pdf_reader.py --export
```

## What Gets Extracted

- **Title** - From first pages of PDF
- **Author** - From metadata or text patterns  
- **Abstract** - From dedicated abstract pages
- **Output** - CSV file ready for Excel import

## Dependencies

- `pypdf` - PDF text extraction
