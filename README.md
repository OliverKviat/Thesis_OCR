# PDF Reader

PDF reader and metadata extractor for academic theses.

## Features

- **Read PDF content** - Extract full text from PDFs
- **Smart title extraction** - English title from filename + verification in PDF
- **Extract metadata** - Author and abstract from PDFs
- **Excel export** - Batch process all PDFs to CSV

## Usage

```bash
# Show available PDF files and options
uv run python pdf_reader.py

# Read PDF content
uv run python pdf_reader.py "filename.pdf" [--first5|--full]

# Extract metadata from single PDF
uv run python pdf_reader.py --info "filename.pdf"

# Export all PDFs to CSV for Excel
uv run python pdf_reader.py --export
```

## Title Extraction

Title is extracted from filename (removes ID and translation):
- Searches first 10 PDF pages for filename title
- If found: Uses filename title
- If not found: Extracts title from PDF content
- CSV shows both with match status

## CSV Output Columns

- Filename
- Title_From_Filename
- Title_From_PDF (if different from filename title)
- Title_Match (Yes/No)
- Author
- Abstract
- File_Path

## Dependencies

- `pypdf` - PDF text extraction
