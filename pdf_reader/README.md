# PDF Reader

Simple PDF reader for thesis OCR project.

## Task 1: Basic PDF Reading ✓

Opens and reads PDF files from `Data/RAW_test` folder using `pypdf` library.

## Usage

```bash
# Show available PDF files
uv run python pdf_reader.py

# Read a specific PDF file
uv run python pdf_reader.py "filename.pdf"
```

## Current Status

- ✅ UV package management setup
- ✅ Basic PDF text extraction with pypdf
- ✅ File listing and selection
- ✅ Page-by-page processing
- ✅ Terminal output

## Dependencies

- `pypdf`: PDF text extraction

## Next Steps

- Add text processing and metadata extraction
- Implement output formatting options
- Add error handling improvements
