# Columns gathered and ready for grade appending:

- [x] **`Publication Year`**: The year of publication
- [x] **`MASTER THESIS TITLE`**: The english title of the thesis
- [x] **`BY`**: The author(s) in the format "lastname, name" (if multiple auhtors, they're separated wiht ";") 
- [x] **`SUPERVISED BY`**: The supervisor(s) in the format "lastname, name" (if multiple supervisors, they're separated with ";")
- [x] **`num_tot_pages`**: Number of total pages in .pdf file
- [x] **`num_cont_pages`**: Number of content pages in the .pdf file (excluding appendix, references etc.)
- [x] **`handin_month`**: The month of handin exstracted from the .pdf file. *OBS(!):* disregard the year in the stirng, and use the metric `Publication Year` for true year.
- [x] **`num_figures`**: Number of figures in the .pdf file
- [x] **`num_tables`**: Number of tables in the .pdf file
- [x] **`num_references`** Number of references listed in the section regarding bibliography in the .pdf file
- [x] **`total_sentences`**: Number of sentences in main content of .pdf file
- [x] **`total_words`**: Number of words in main content of .pdf file
- [x] **`unique_words`**: Number of unique words in main content of .pdf file
- [x] **`avg_sentence_length`**: Average sentence lenght of main content of .pdf file
- [x] **`avg_word_length`**: Average word lenght of main conent of .pdf file
- [x] **`lexical_diversity`**: Measure of the lexical diversity in the main content of .pdf file (unique_words/total_words)
- [x] **`flesch_kincaid_grade`**: ...
- [x] **`Department_new`**: The department of DTU from which the thesis is published
- [x] **`num_authors`**: Number of authors for MSc Thesis, count of semicolons inn column `BY`. 
If the value is missing (NaN), fillna(0) treats it as 0 semicolons, resulting in 1 author.


**WILL BE APPENDED LATER:**
- [] **`handin_month_num`** getting only the month from column `handin_month` and mapping to a number (1-12) using the calendar module for robustness.
- [] **`grading_scientific_contribution`**: Sub grading score, (x-y)
- [] **`grading_methodological_rigor`**: Sub grading score, (x-y)
- [] **`grading_technical_implementation`**: Sub grading score, (x-y)
- [] **`grading_literature_review`**: Sub grading score, (x-y)
- [] **`grading_process_professionalism`**: Sub grading score, (x-y)
- [] **`grading_impact_applicability`**: Sub grading score, (x-y)
- [] **`grading_research_question_alignment`**: Sub grading score, (x-y)
- [] **`grading_total_score`**: Total assigned grading score (1-100) for the thesis by local LLM. Consistes of the scores; scientific contribution, methodological rigor, technical implementation, literature review, process professionalism, impact applicability.

**Excluded from analysis and not relevant anymore:**
- [] **`equation_count`**: Number of equations in the .pdf file