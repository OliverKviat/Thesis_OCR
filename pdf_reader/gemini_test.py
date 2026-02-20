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
            types.Part.from_text(text="""# ROLE
Act as an academic reverse-engineer. Convert MSc abstracts into "Month Zero" (pre-study) research proposals.

# CORE RULES
1. TENSE: Use STRICT FUTURE TENSE (will, aims to, is expected to).
2. NO LEAKAGE: Transform results/conclusions into "hypotheses" or "intended goals." (e.g., "X will improve Y" instead of "X improved Y").
3. CITATIONS: Identify 3 real, seminal foundational papers for the methodology. Cite them in Section 8; list in Section 10 (APA).
4. FIDELITY: Maintain all source technical acronyms and terminology.

# STRUCTURE
1. Title: [Input Title]
2. Author: [Name/sxxxxxx from input or Placeholder]
3. Supervisor: [Input or Placeholder]
4. Department: [Input or Placeholder]
5. Problem Statement: Pre-study knowledge gap/limitations.
6. Research Questions: Numbered list of project goals.
7. Data: 
   - Input: Materials available at Month Zero.
   - Target: Data to be produced/collected.
8. Method: Planned approach + citations for the 3 foundational papers.
9. Expected Outcomes: Intended deliverables/frameworks.
10. References: APA list of papers from Section 8.

# EXECUTION
Analyze abstract technical angle -> Apply temporal/leakage rules -> Ensure zero retrospective mentions ("results showed/concluded").
"""),
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


