from __future__ import annotations

from unittest.mock import patch

from simple_memory_models.runtime import (
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_EMBEDDING_REVISION,
    DEFAULT_QUERY_INSTRUCTION,
    DEFAULT_RERANKER_MODEL,
    DEFAULT_RERANKER_REVISION,
    DEFAULT_RERANK_INSTRUCTION,
    ModelRuntime,
    RuntimeConfig,
)


def config(device: str) -> RuntimeConfig:
    return RuntimeConfig(
        embedding_model=DEFAULT_EMBEDDING_MODEL,
        embedding_revision=DEFAULT_EMBEDDING_REVISION,
        reranker_model=DEFAULT_RERANKER_MODEL,
        reranker_revision=DEFAULT_RERANKER_REVISION,
        query_instruction=DEFAULT_QUERY_INSTRUCTION,
        rerank_instruction=DEFAULT_RERANK_INSTRUCTION,
        device=device,
        embedding_batch_size=8,
        rerank_batch_size=4,
        local_files_only=True,
    )


class FakeTokenizer:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], dict[str, object]]] = []

    def __call__(self, texts: list[str], **options: object) -> dict[str, list[int]]:
        self.calls.append((texts, options))
        return {"length": [len(text) + 2 for text in texts]}


runtime = ModelRuntime(config("cpu"))
tokenizer = FakeTokenizer()
with patch.object(runtime, "_get_tokenizer", return_value=tokenizer):
    assert runtime.count_tokens([]) == []
    assert tokenizer.calls == []
    assert runtime.count_tokens(["a", "abcd", ""]) == [3, 6, 2]
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


with patch.object(runtime, "_get_tokenizer", return_value=InvalidTokenizer()):
    try:
        runtime.count_tokens(["one", "two"])
    except RuntimeError as error:
        assert "invalid length batch" in str(error)
    else:
        raise AssertionError("Invalid tokenizer batch response should fail")

print("Python runtime optimization probe passed.")
