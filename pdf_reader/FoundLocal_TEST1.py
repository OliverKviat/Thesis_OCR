"""Run this model in Python

> pip install openai
"""
from openai import OpenAI

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
                    "text": "title: Deep Reinforcement Learning Applying an actor-critic network to a search and rescue robotics setting.\n abstract: Robotic exploration in an unknown environment is still a very much open ended problem. Clas- sical approaches are computationally costly and cannot take advantage of common features in the environments to make intelligent decisions. Deep reinforcement learning has recently made some incredible achievements however its use for robotic mapping is still not clear. As such, this paper deals with the eﬃcacy of using deep reinforcement learning in the context of robotic exploration. It is assumed that the agent has access to perfect pose knowledge however it is equipped with noisy sensor and must eﬃciently explore the unknown environment. The map used in this study is represented by an occupancy map and the performance metric is the reduction of the map’s entropy at each step. A classical approach is chosen from literature and compared to deep reinforcement learning algorithms developed in this paper. The devel- oped deep reinforcement learning algorithm outperforms the classical approach in a controlled environment setting but the scalability to a real world setting still needs to be researched.",
                },
            ],
        },
    ],
    model = "qwen2.5-1.5b-instruct-generic-cpu:4",
    max_tokens = 1000,
    temperature = 0.5,
    top_p = 0.8,
)

print(f"[Model Response] {response.choices[0].message.content}")