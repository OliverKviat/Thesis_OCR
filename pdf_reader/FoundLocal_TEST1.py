"""Run this model in Python

> pip install openai
"""
from openai import OpenAI
import os
import json
from SI_options import PROMPT_REGISTRY, select_prompt


def model_call(system_instruction: str, user_prompt: str, counter: int = 0):
    client = OpenAI(
        base_url = "http://localhost:5272/v1/",
        api_key = "unused", # required for the API but not used
    )

    response = client.chat.completions.create(
        messages = [
            {
                "role": "system",
                "content": system_instruction,
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

    return response.choices[0].message.content

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
    
    print("Welcome to the Academic Reverse-Engineer CLI!")
    print("Please choose the System Instruction to be used for the model:")
    for prompt_id, spec in sorted(PROMPT_REGISTRY.items()):
        print(f"{prompt_id}. System Instruction – {spec.version}")

    SI_choise = input("Enter the number corresponding to your choice: ")
    try:
        selected_prompt = select_prompt(int(SI_choise), persist_selection=True)
    except ValueError as error:
        print(f"Invalid prompt choice: {error}")
        return

    system_instruction = selected_prompt.system_prompt
    print(f"System Instruction – {selected_prompt.version} selected.")

    object_choise = input("Enter the index of the paper to process (0-9): ")
    try:
        counter = int(object_choise)
        if counter < 0 or counter > 9:
            raise ValueError("Index must be between 0 and 9.")
    except ValueError as e:
        print(f"Invalid input: {e}")
        return


    user_prompt = retreave_prompt(counter)
       
    response = model_call(system_instruction, user_prompt)
    print(f"Response: {response}")

if __name__ == "__main__":
    main()