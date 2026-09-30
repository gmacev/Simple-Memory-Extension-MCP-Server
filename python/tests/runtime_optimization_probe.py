from __future__ import annotations

import runpy
from importlib.util import find_spec
from pathlib import Path

from simple_memory_models.tokenization import count_token_lengths


class FakeTokenizer:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], dict[str, object]]] = []

    def __call__(self, texts: list[str], **options: object) -> dict[str, list[int]]:
        self.calls.append((texts, options))
        return {"length": [len(text) + 2 for text in texts]}


tokenizer = FakeTokenizer()
assert count_token_lengths(tokenizer, []) == []
assert tokenizer.calls == []
assert count_token_lengths(tokenizer, ["a", "abcd", ""]) == [3, 6, 2]
assert len(tokenizer.calls) == 1
texts, options = tokenizer.calls[0]
assert texts == ["a", "abcd", ""]
assert options == {
    "add_special_tokens": True,
    "truncation": False,
    "padding": False,
    "return_length": True,
}


class InvalidTokenizer:
    def __call__(self, _texts: list[str], **_options: object) -> dict[str, list[int]]:
        return {"length": [1]}


try:
    count_token_lengths(InvalidTokenizer(), ["one", "two"])
except RuntimeError as error:
    assert "invalid length batch" in str(error)
else:
    raise AssertionError("Invalid tokenizer batch response should fail")

if all(find_spec(name) is not None for name in ("torch", "sentence_transformers", "transformers")):
    runpy.run_path(str(Path(__file__).with_name("prefix_reuse_probe.py")))
else:
    print("Prefix reuse tensor probe skipped: optional model dependencies are not installed.")

print("Python runtime optimization probe passed.")
