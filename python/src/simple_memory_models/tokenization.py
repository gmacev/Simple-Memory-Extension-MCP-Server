from __future__ import annotations

from typing import Any, Protocol


class BatchTokenizer(Protocol):
    def __call__(self, texts: list[str], **options: object) -> Any: ...


def count_token_lengths(tokenizer: BatchTokenizer, texts: list[str]) -> list[int]:
    if not texts:
        return []
    encoded = tokenizer(
        texts,
        add_special_tokens=True,
        truncation=False,
        padding=False,
        return_length=True,
    )
    lengths = encoded.get("length")
    if not isinstance(lengths, list) or len(lengths) != len(texts):
        raise RuntimeError("Tokenizer returned an invalid length batch")
    return [int(length) for length in lengths]
