### ==== IMPORT ====
import pypdf
import logging
from pathlib import Path
import pandas as pd

# Suppress all pypdf log messages below the ERROR level
logging.getLogger("pypdf").setLevel(logging.ERROR)

### ==== FUNCTIONS ====
# Count the number of TOTAL pages in the PDF file
def count_tot_pages(reader) -> int:
    """Counts the number of pages in the PDF file.

    RETURNS: 
    The total number of pages in the PDF file"""
    num_tot_pages = len(reader.pages)
    
    return num_tot_pages

# Count the number of CONTENT pages in the PDF file
def count_cont_pages(reader, audit=False, page_prints=False) -> int:
    """Counts the number of pages in the PDF file that contain main content."""
    num_cont_pages = 0
    num_tot_pages = len(reader.pages)
    min_end_page = max(1, int(num_tot_pages * 0.30))
    
    end_boundary_exact = {
        "references",
        "bibliography",
        "works cited",
        "list of references",
        "reference list",
        "appendix",
        "appendices",
        "referencer",
        "bibliografi",
        "litteratur",
        "litteraturliste",
        "litteraturfortegnelse",
        "kildeliste",
        "bilag",
        "appendiks",
        "list of figures",
        "list of tables",
    }
    end_boundary_prefix = (
        "references",
        "bibliography",
        "works cited",
        "appendix",
        "appendices",
        "referencer",
        "bibliografi",
        "litteratur",
        "kildeliste",
        "bilag",
        "appendiks",
    )

    boundary_page_text = None
    boundary_page_number = None

    for page_number, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        lines = [line.strip().lower() for line in text.splitlines() if line.strip()]

        matched_line = None
        match_trigger = None
        reject_reason = None

        for line in lines:
            tokens = line.split()
            prefix_token = None
            core_line = line

            # Allow heading labels like "6 References" or "F List of tables".
            if tokens:
                first_token = tokens[0].rstrip(").:-")
                if first_token.isdigit() or (len(first_token) == 1 and first_token.isalpha()):
                    prefix_token = first_token
                    core_line = " ".join(tokens[1:]).strip()

            # Determine which trigger matched (and record it).
            if core_line and core_line in end_boundary_exact:
                if prefix_token and prefix_token.isdigit():
                    match_trigger = f"numeric-prefix exact  ('{prefix_token} {core_line}')"
                elif prefix_token:
                    match_trigger = f"letter-prefix exact  ('{prefix_token} {core_line}')"
                else:
                    match_trigger = f"exact  ('{core_line}')"
            elif core_line and any(core_line.startswith(p) for p in end_boundary_prefix):
                matched_prefix = next(p for p in end_boundary_prefix if core_line.startswith(p))
                if prefix_token and prefix_token.isdigit():
                    match_trigger = f"numeric-prefix prefix-match  ('{prefix_token} {core_line}', prefix='{matched_prefix}')"
                elif prefix_token:
                    match_trigger = f"letter-prefix prefix-match  ('{prefix_token} {core_line}', prefix='{matched_prefix}')"
                else:
                    match_trigger = f"prefix-match  ('{core_line}', prefix='{matched_prefix}')"
            else:
                match_trigger = None

            if match_trigger is None:
                continue

            words = line.split()
            has_short_length = len(line) <= 60 and len(words) <= 8

            # Reject lines that look like running prose rather than headings.
            ends_with_comma_semicolon = line.endswith(",") or line.endswith(";") or line.endswith(":") or line.endswith(".") or line.endswith(")")

            core_words = core_line.split()
            first_core_token = core_words[0] if core_words else ""
            if first_core_token in end_boundary_exact:
                trailing_words = core_words[1:]
            else:
                trailing_words = core_words
            lowercase_trailing_count = sum(
                1 for w in trailing_words if w.isalpha() and w.islower()
            )
            has_many_lowercase_trailing = lowercase_trailing_count >= 4

            if not has_short_length:
                reject_reason = "failed length rule (>60 chars or >8 words)"
                if audit:
                    print(f"[AUDIT] Rejected candidate on page {page_number}: '{line}' ({reject_reason})")
                continue

            if ends_with_comma_semicolon:
                reject_reason = "ends with comma/semicolon/colon/period/parenthesis (sentence-like)"
                if audit:
                    print(f"[AUDIT] Rejected candidate on page {page_number}: '{line}' ({reject_reason})")
                continue

            if has_many_lowercase_trailing:
                reject_reason = "many lowercase trailing words (sentence-like)"
                if audit:
                    print(f"[AUDIT] Rejected candidate on page {page_number}: '{line}' ({reject_reason})")
                continue

            matched_line = line
            break

        if matched_line is not None:
            if audit:
                print(f"[AUDIT] Candidate end boundary on page {page_number} via {match_trigger}")

            # Validate that end boundary is found after first 30% of pages.
            if page_number > min_end_page:
                boundary_page_text = text
                boundary_page_number = page_number
                if audit:
                    print(f"[AUDIT] Accepted boundary on page {page_number} (>{min_end_page} pages threshold).")
                break
            elif audit:
                print(f"[AUDIT] Ignored candidate on page {page_number} (must be > {min_end_page}).")

        num_cont_pages += 1

    # __________________
    # DELETE THIS LATER:
    if page_prints:
        choice = input("Reached the end of the main content. Press 1 to print the next page, 2 to stop: ").strip()

        if choice == "1" and boundary_page_text is not None:
            # Print the first detected non-main-content page.
            print(f"========== START OF PAGE {boundary_page_number} ==========")
            print(f"{boundary_page_text}\n{'-' * 40}")
            print("========== END OF PAGE ==========")
        elif choice == "1":
            print("No validated end boundary page was found.")
    # __________________

    return num_cont_pages, match_trigger

# Count the number of words in the PDF file
def word_count(reader, last_page) -> int:
    """Count the number of words in the PDF file."""
    tot_words = 0

    for page in reader.pages[:last_page]:
        text = page.extract_text()
        tot_words += len(text.split())
    return tot_words

def print_pdf_text(reader) -> None:
    """Prints the text content of each page in the PDF file."""
    for i, page in enumerate(reader.pages):
        text = page.extract_text()
        print(f"Page {i + 1}:\n{text}\n{'-' * 40}")

### ==== MAIN FUNCTION ====
# Opem and read in a LOCAL PDF file
def main(pdf_path, supres_prints=True):
    with open(pdf_path, 'rb') as file:
        try:
            reader = pypdf.PdfReader(file)
        except Exception as e:
            print(f"Error reading PDF file: {e}")

        # Number of pages in the PDF file
        num_tot_pages = count_tot_pages(reader)

        # Number of pages with main content in the PDF file
        num_cont_pages, match_trigger, = count_cont_pages(reader, audit=False, page_prints=False) # audit=True/False to get audit prints, page_prints=True/False to get option to print end page/first excluded page

        # Number of words in the PDF file
        num_words_full = word_count(reader, num_tot_pages)

        # Number of words in the main content of the PDF file
        num_words_cont = word_count(reader, num_cont_pages)

        # All print statements in the main function

        if not supres_prints:    
            print(f"Total number of pages in PDF: {num_tot_pages}")
            print(f"Number of pages with main content in PDF: {num_cont_pages}")
            print(f"Accepted candidate reason: {match_trigger}")
            print(f"Total number of words in the full PDF: {num_words_full}")
            print(f"Number of words in the main content of the PDF: {num_words_cont}")

    return num_tot_pages, num_cont_pages, match_trigger, num_words_full, num_words_cont


### ==== FILE PATHS ====
# Resolve the folder from the repository root so discovery works regardless of current working directory.
repo_root = Path(__file__).resolve().parents[1]
folder_path = repo_root / "Data" / "RAW_test"

pdf_files = sorted(folder_path.glob("*.pdf"))

if not pdf_files:
    print(f"No PDF files found in: {folder_path}")
else:
    total_files = len(pdf_files)
    print(f"Found {total_files} PDF file(s) in: {folder_path}")

    user_input = input(f"How many files do you want to process? (1-{total_files}): ").strip()

    try:
        num_to_process = int(user_input)
        if num_to_process <= 0:
            raise ValueError
    except ValueError:
        raise SystemExit("Error: Invalid input. Please enter a positive integer. Execution terminated.")

    if num_to_process > total_files:
        choice = input(
            f"Error: Requested {num_to_process} files, but only {total_files} available.\n"
            "Type 'all' to process all files, or 'q' to terminate: "
        ).strip().lower()

        if choice == "all":
            num_to_process = total_files
        elif choice in {"q", "quit"}:
            raise SystemExit("Execution terminated by user. No files were processed.")
        else:
            raise SystemExit("Error: Invalid choice. Execution terminated without processing files.")

# Building dataframe to store the metrics for each PDF file
metrics_df = pd.DataFrame(columns=["pdf_file", "num_tot_pages", "num_cont_pages", "match_trigger", "num_words_full", "num_words_cont"])

# Process the specified number of PDF files and collect metrics.
for current_file_number, pdf_path in enumerate(pdf_files[:num_to_process], start=1):
    print(f"\n=== Processing file {current_file_number} of {num_to_process} ===")
    print(f"Current file is: {pdf_path.name}")
    num_tot_pages, num_cont_pages, match_trigger, num_words_full, num_words_cont = main(pdf_path, supres_prints=True)
    metrics_df.loc[len(metrics_df)] = {
        "pdf_file": pdf_path.name,
        "num_tot_pages": num_tot_pages,
        "num_cont_pages": num_cont_pages,
        "match_trigger": match_trigger,
        "num_words_full": num_words_full,
        "num_words_cont": num_words_cont
    }

# Save the metrics to a CSV file in the same folder as the script.
output_csv_path = repo_root / "Data" / "extracted_metrics.csv"
metrics_df.to_csv(output_csv_path, index=False)
print(f"\nMetrics for {num_to_process} PDF file(s) have been saved to: {output_csv_path}")