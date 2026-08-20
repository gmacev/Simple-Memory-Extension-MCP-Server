from __future__ import annotations

from typing import Any

from simple_memory_models.worker import _handle


class FakeRuntime:
    def embed_queries(self, texts: list[str]) -> list[list[float]]:
        return [[float(index), float(len(text))] for index, text in enumerate(texts)]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [[float(len(text))] for text in texts]

    def count_tokens(self, texts: list[str]) -> list[int]:
        return [len(text) for text in texts]

    def rerank_pairs(self, pairs: list[tuple[str, str]]) -> list[float]:
        return [float(index) for index, _pair in enumerate(pairs)]


def expect_error(payload: dict[str, Any]) -> None:
    try:
        _handle(FakeRuntime(), payload)  # type: ignore[arg-type]
    except ValueError:
        return
    raise AssertionError(f"Expected malformed payload to fail: {payload!r}")


runtime = FakeRuntime()
queries = _handle(runtime, {"operation": "embed_queries", "texts": ["a", "bbb"]})
assert queries == {"vectors": [[0.0, 1.0], [1.0, 3.0]]}
assert _handle(runtime, {"operation": "embed_queries", "texts": []}) == {"vectors": []}

pairs = [
    {"query": "q1", "document": "d1"},
    {"query": "q2", "document": "d2"},
]
assert _handle(runtime, {"operation": "rerank_pairs", "pairs": pairs}) == {
    "scores": [0.0, 1.0]
}
assert _handle(runtime, {"operation": "rerank_pairs", "pairs": []}) == {"scores": []}

expect_error({"operation": "embed_queries", "texts": ["valid", 2]})
expect_error({"operation": "rerank_pairs", "pairs": [{"query": "q"}]})
expect_error(
    {"operation": "rerank_pairs", "pairs": [{"query": "", "document": "d"}]}
)
expect_error(
    {
        "operation": "rerank_pairs",
        "pairs": [{"query": "q", "document": "d", "extra": True}],
    }
)

print("Python worker protocol probe passed.")
