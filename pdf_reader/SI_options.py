import json
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Dict
from pathlib import Path

@dataclass(frozen=True)
class PromptSpec:
    system_prompt: str
    version: str

def _load_prompt_registry() -> Dict[int, PromptSpec]:
    config_path = Path(__file__).resolve().parent / "SI_versions.json"
    raw_entries = json.loads(config_path.read_text(encoding="utf-8"))

    registry: Dict[int, PromptSpec] = {}
    for index, entry in enumerate(raw_entries, start=1):
        prompt_id = entry.get("id", index)
        registry[int(prompt_id)] = PromptSpec(
            system_prompt=entry["SI"],
            version=entry["version"],
        )

    if not registry:
        raise ValueError("No prompts were loaded from SI_versions.json")

    return registry


PROMPT_REGISTRY: Dict[int, PromptSpec] = _load_prompt_registry()

def get_prompt_by_id(prompt_id: int) -> PromptSpec:
    try:
        return PROMPT_REGISTRY[prompt_id]
    except KeyError:
        raise ValueError(f"Unknown prompt ID: {prompt_id}")


def save_prompt_selection(
    prompt_id: int,
    output_path: Path | None = None,
) -> Path:
    spec = get_prompt_by_id(prompt_id)

    if output_path is None:
        project_root = Path(__file__).resolve().parents[1]
        output_path = project_root / "pdf_reader" /"selected_prompt.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "selected_at_utc": datetime.now(timezone.utc).isoformat(),
        "prompt_id": prompt_id,
        "version": spec.version,
        "system_prompt": spec.system_prompt,
    }
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return output_path


def select_prompt(prompt_id: int, persist_selection: bool = True) -> PromptSpec:
    spec = get_prompt_by_id(prompt_id)
    if persist_selection:
        save_prompt_selection(prompt_id)
    return spec


if __name__ == "__main__":
    selected_id = int(input("Enter the prompt ID: "))
    selected = select_prompt(selected_id, persist_selection=True)
    print(f"Selected prompt version: {selected.version}")