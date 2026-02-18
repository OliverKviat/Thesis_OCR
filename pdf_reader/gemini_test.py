#!/usr/bin/env python3
"""
To set the API key to use, write in terminal:
export GEMINI_API_KEY="YOUR_API_KEY"

To check if the API key is set, write in terminal:
echo $GEMINI_API_KEY
"""

# To run this code you need to install the following dependencies:
# pip install google-genai

import os
from google import genai
from google.genai import types

API_KEY_ENV_VAR = "GEMINI_API_KEY"

def generate():
    api_key = os.environ.get(API_KEY_ENV_VAR)
    if not api_key:
        raise RuntimeError(
            f"Missing API key. Set {API_KEY_ENV_VAR} in your shell before running."
        )

    client = genai.Client(
        api_key=api_key,
    )

    model = "gemini-2.0-flash"
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text="""title: Synthetic inertia support from wind turbines with energy storage systems. abstract: The foreseen high penetration of wind power into the power systems imposes ad- vanced requirements to sustain the frequency stability in the network. One of this requirements for wind turbines is to provide synthetic inertia, which emulates the behaviour of a synchronous generator during frequency disturbances. For the provi- sion of this synthetic inertia, wind turbines can also be supported by energy storage systems. This thesis investigates the effect of introducing energy storage systems in the power system in order to support the synthetic inertia or fast frequency response from wind turbines. Results are analysed utilising the genetic algorithm and fmincon as opti- mization methods for the fast frequency response control. In addition to this, the impact of two control system approaches as close loop and open loop is studied at wind speeds below and close to the rated speed of a wind turbine. Results have shown that the introduction of energy storage systems in the power system is beneﬁcial for the support of provision of FFR from wind turbines. Fur- thermore, the genetic algorithm and close loop approach returned a more adequate frequency response in the power system than fmincon and the open loop approach. This is mainly noticeable at wind speeds close to the rated speed of the wind tur- bine."""),
            ],
        ),
    ]
    generate_content_config = types.GenerateContentConfig(
        system_instruction=[
            types.Part.from_text(text="""System Instruction: You are a specialist in academic reverse-engineering and research proposal synthesis. Your task is to take an abstract from a MSc thesis and \"back-calculate\" the original one-page project description that was likely written before the study began.
Perspective Shift – CRITICAL: Do NOT write a summary of the thesis findings. You must adopt a prospective, planning-focused tone. Use future tense (e.g., \"The project will evaluate...\" rather than \"The project evaluated...\") to describe the intended research path.
Fidelity Guardrails (Scope & Intent): To ensure the reconstruction reflects the true original intent rather than hindsight:
•	Leakage Prevention: Avoid \"insight leakage.\" Do not include conclusions, specific results, or \"aha!\" moments presented in the abstract unless they were explicitly framed as a priori hypotheses. Transform every \"finding\" in the abstract into a \"research hypothesis.\" If the abstract says \"Algorithm A reduced latency by 20%,\" the reconstruction must state: \"It is hypothesized that the application of Algorithm A will result in measurable latency reductions compared to current benchmarks.\"
•	Lexical Locking: Use the specific terminology, naming conventions, and technical acronyms established in the abstract to maintain consistency with the author's academic voice.
•	Internal Knowledge Retrieval: Since you do not have internet access, identify the top 3 most likely foundational papers from your internal training data that establish the scientific basis for the technologies or methodologies mentioned in the abstract.
One-Pager Structure (Required):
1.	Title: Transferred directly from the prompt/input.
2.	Author(s): Full name of student(s) and student number(s) (format: 'sxxxxxx') directly from prompt/input (if given, otherwise leave placeholder).
3.	Supervisor(s): Transferred directly from prompt/input (if given, otherwise leave placeholder).
4.	Department: Transferred directly from prompt/input (if given, otherwise leave placeholder).
5.	Problem Statement: Frame the gap in knowledge or technical limitation that existed prior to the research.
6.	Research Questions (Numbered): Define the specific, high-level questions the project aims to answer.
7.	Data Partitioning:
1.	Input Data: Describe only the datasets/materials available at \"Month Zero.\"
2.	Target Data: Describe the data the project intends to produce or collect (e.g., \"The project will generate a labeled dataset of...\").
8.	Method: Describe the planned technical approach. You must identify and incorporate the top 3 most relevant foundational papers  from internal training data that fits the primary scientific basis for this thesis. Do not list these papers, but use them as citations for the intended methodology to be applied in the project.
9.	Expected Outcomes: Define the intended deliverables (e.g., \"A proposed framework for...\").
10.	References: List the 3 foundational papers identified in Section 8 (Method) using APA reference style.
Instructions
1.	Analyze the Source: Carefully parse the provided thesis abstract, focusing on understanding the aim and academical angle of the thesis to ensure prospective accuracy.
2.	Identify Foundations: Identify the 3 most seminal references related to the core methodology using your internal knowledge. Ensure they are valid, real publications.
3.	Strict Formatting: The output must follow the One-Pager Structure exactly as defined in the System Instruction.
4.	Tone Check: Ensure no mention of \"the results showed\" or \"it was concluded.\" Use \"will,\" \"aims to,\" and \"is expected to.\""""),
        ],
    )

    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        print(chunk.text, end="")

if __name__ == "__main__":
    generate()


