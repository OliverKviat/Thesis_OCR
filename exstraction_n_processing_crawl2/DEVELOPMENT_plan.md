# DEVELOPMENT PLAN FOR ON SCRIPT TO RULE THEM ALL

## Objective
The objective of the produced a script that convert the pdf's to .txt files and exstracts the following metrics into a .csv file.

**Basic Metrics** Legacy = *gcp_metrics_from_pdfs.py*
- [] num_tot_pages: Number of total pages in the pdf file - using PyMuPDF to get this
- [] num_cont_pages: Number of pages for the main content of the pdf file (that is excluding references, appendix etc.) Using the legacy method used previosly to determine content pages
- [] num_words_full: The number of words in full text of the pdf.
- [] num_words_cont: The number of words in main content part of the text
**Aditional Elements** Legacy = *gcp_num_fig-tab-ref_exstractor.py*
- [] num_figures: the number of figures in the .pdf - using the legacy method used previously to dertermin this.
- [] num_tables: the number of tables in the .pdf - using the legacy method used previously to determin this.
- [] num_references: the number of references in the .pdf reference list - using the legacy method used previously to determin this.
**Handin Month** Legacy = *gcp_seasonality_from_pdfs.py*
- [] handin_month: The month for handin of the thesis. Building on the legacy method aleready applied, but shoul NOT keep the year. What is needed is the month ONLY.
- [] handin_month_num: Convert the handin_month string to the correspending month number (1:12).
**Lingustic Elements** Legacy = *gcp_linguistics_exstractor.py*
- [] total_sentences: The number of total sentences in the main content of the text 
- [] total_words: The number of total words in the main content of the text
- [] unique_words: The number of unique words in the main content of the text
- [] avg_sentence_length: The average sentence length of the sentences in the main content of the text
- [] avg_word_length: The average length of words in the main content text
- [] lexical_diversity: The lexical diversity of the text in the main content of the text
- [] flesch_kincaid_grade: The flesh kincaid grade for the text in the main content of the text

### Context for legacy methods
The legacy scripts that produce the metrics mentioned above are:

* extraction_more_from_pdf/gcp_linguistics_exstractor.py:
total_sentences, total_words, unique_words, avg_sentence_length, avg_word_length, lexical_diversity, flesch_kincaid_grade

* extraction_more_from_pdf/gcp_metrics_from_pdfs.py:
num_tot_pages, num_cont_pages, match_trigger, num_words_full, num_words_cont

* extraction_more_from_pdf/gcp_num_fig-tab-ref_exstractor.py:
num_figures, num_tables, num_references

* extraction_more_from_pdf/gcp_seasonality_from_pdfs.py:
handin_month; corrupt_cid (semicolon-delimited CSV)

# Runing the script(s)

* ``pdf2txt_with_sidecar.py``

````
uv run exstraction_n_processing_crawl2/pdf2txt_with_sidecar.py \
  --input-dir Data/RAW_test/ \           
  --out-dir Data/TXT_test \       
  --workers 8
````

* ``gcp_txt_unified_extractor.py``

`````
uv run exstraction_n_processing_crawl2/batch_pdf2txt_converter.py \
  --input-dir Data/RAW_test/ \           
  --out-dir Data/ \               
  --workers 8
````