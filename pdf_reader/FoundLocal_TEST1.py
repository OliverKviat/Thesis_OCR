"""Run this model in Python

> pip install openai
"""
from openai import OpenAI
import os
import json


def model_call(user_prompt: str, counter: int = 0):
    client = OpenAI(
        base_url = "http://localhost:5272/v1/",
        api_key = "unused", # required for the API but not used
    )

    response = client.chat.completions.create(
        messages = [
            {
                "role": "system",
                "content": "# ROLE: Academic Reverse-Engineer.\nTASK: Convert MSc Abstract -> \"Month Zero\" (Pre-study) Research Proposal.\nCRITICAL: DO NOT write an abstract. Use ONLY Future Tense.\n \n# CORE RULES\n1. TENSE: Use ONLY \"will\", \"aims to\", \"is expected to\".\n   - BANNED: \"investigates\", \"focuses\", \"compared\", \"showed\", \"found\", \"presents\", \"deals with\", \"addresses\".\n2. PERSPECTIVE: Shift from \"This paper\" to \"This project\".\n3. LEAKAGE: Results/Conclusions MUST be transformed into \"Target Hypotheses\" or \"Aims\".\n \n# STRUCTURE & TEMPLATE (FOLLOW EXACTLY)\n1. Title: [Input Title EXACTLY with NO changes]\n2. Author: [Placeholder]\n3. Supervisor: [Placeholder]\n4. Department: [Placeholder]\n \n5. Problem Statement: Narrative section that motives and define the knowledge gap or technical limitation existing BEFORE the study.\n   - [Field Name] is essential for [Global Stake].\n   - However, current systems are constrained by [Technical Bottleneck].\n   - This project will address the gap by [Proposed Action].\n   - VETO: No \"The problem is\" or \"In the field of\".\n \n6. Research Questions: Numbered list of 3-4 open-ended questions.\n7. Data:\n   - Input (Available at Month Zero): [List tools/sets]\n   - Target (To be produced): [List intended metrics/data]\n \n8. Method:\n   - Architecture Design: Will develop [Technical details]...\n   - Simulation Setup: Will utilize [Environment details]...\n   - Comparative Testing: Will conduct [Testing details]...\n \n9. Expected Outcomes: List of intended deliverables (using \"will\" or \"is expected to\").\n \n10. References: List of search terms related to the subject.\n \n# EXECUTION\n1. Shift to \"Month Zero\" perspective: Imagine the research has NOT started.\n2. VETO: If a sentence does not contain \"will\", \"aims to\", or \"is planned\", rewrite it.\n3. TERMINATE: Stop immediately after the last Reference. Do NOT provide closing remarks.",
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": user_prompt,
                    },
                ],
            },
        ],
        model = "qwen2.5-1.5b-instruct-generic-cpu:4",
        max_tokens = 1000,
        temperature = 0.5,
        top_p = 0.8,
    )

    #print(f"[Model Response] {response.choices[0].message.content}")

    counter += 1
    return response.choices[0].message.content, counter

def retreave_prompt(i: int = 0):
    """Retreave the user prompt from the specefied file."""
    path = os.path.join(os.path.dirname(__file__), '..', 'Data', 'Extracted_data', 'extracted_metadata.json')
    print(f"Reading JSON file from: {path}")
    with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

    json_title = data[i]['title']
    json_abstract = data[i]['abstract']

    prompt = f"Title: {json_title}\nAbstract: {json_abstract}"
    return prompt

def main():
    """CLI entry point."""
    
    counter = input("Enter the index of the paper to process (0-9): ")
    try:
        counter = int(counter)
        if counter < 0 or counter > 9:
            raise ValueError("Index must be between 0 and 9.")
    except ValueError as e:
        print(f"Invalid input: {e}")
        return

    user_prompt = retreave_prompt(counter)
       
    response, counter = model_call(user_prompt)
    print(f"Response: {response}")
    print(f"Counter: {counter}")

if __name__ == "__main__":
    main()