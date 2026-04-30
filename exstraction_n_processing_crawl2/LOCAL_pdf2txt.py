### IMPORTS ###
import os
import pymupdf

### PATHS ###
#pdf_path = "Data/RAW_test"
#txt_path = "Data/TXT_test"
pdf_path = "Data/handin_test"
txt_path = "Data/TXT_handin_test"

### CHOOSE PDF FILES TO CONVERT ###
list_pdf_files = [f for f in os.listdir(pdf_path) if f.endswith('.pdf')]
print(f"Found {len(list_pdf_files)} PDF files to convert.")

### FUNCTIONS ###
def pdf_to_txt(pdf_path, pdf_file, txt_path):
    os.makedirs(txt_path, exist_ok=True)
    pdf_document = pymupdf.open(os.path.join(pdf_path, pdf_file))
    extracted_text = ""
    for page_num in range(pdf_document.page_count):
        page = pdf_document.load_page(page_num)
        extracted_text += f"==PAGE:{page_num + 1}==\n"
        extracted_text += page.get_text()
    out_path = os.path.join(txt_path, os.path.splitext(pdf_file)[0] + '.txt')
    with open(out_path, 'w', encoding='utf-8') as txt_file:
        txt_file.write(extracted_text)
    print(f"Wrote: {out_path}")
    return out_path

### MAIN ###
if __name__ == "__main__":
    for pdf_file in list_pdf_files:
        pdf_to_txt(pdf_path, pdf_file, txt_path)