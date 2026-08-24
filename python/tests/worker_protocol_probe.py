from __future__ import annotations

import base64
import struct
from typing import Any

from simple_memory_models.worker import _handle


class FakeRuntime:
    def embed_queries(self, texts: list[str]) -> list[list[float]]:
        return [[float(index), float(len(text))] for index, text in enumerate(texts)]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [[float(len(text))] for text in texts]

    def embed_query(self, text: str) -> list[float]:
        return [float(len(text)), 7.0]

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


def unpack_matrix(payload: object) -> tuple[int, int, tuple[float, ...]]:
    assert isinstance(payload, dict)
    assert payload["encoding"] == "base64-f32le"
    rows = payload["rows"]
    dimensions = payload["dimensions"]
    data = payload["data"]
    assert isinstance(rows, int) and isinstance(dimensions, int) and isinstance(data, str)
    decoded = base64.b64decode(data, validate=True)
    assert len(decoded) == rows * dimensions * 4
    values = struct.unpack(f"<{rows * dimensions}f", decoded) if decoded else ()
    return rows, dimensions, values


def unpack_vector(payload: object) -> tuple[int, tuple[float, ...]]:
    assert isinstance(payload, dict)
    assert payload["encoding"] == "base64-f32le"
    dimensions = payload["dimensions"]
    data = payload["data"]
    assert isinstance(dimensions, int) and isinstance(data, str)
    decoded = base64.b64decode(data, validate=True)
    assert len(decoded) == dimensions * 4
    return dimensions, struct.unpack(f"<{dimensions}f", decoded) if decoded else ()


runtime = FakeRuntime()
queries = _handle(runtime, {"operation": "embed_queries", "texts": ["a", "bbb"]})
assert isinstance(queries, dict)
assert unpack_matrix(queries["vectors"]) == (2, 2, (0.0, 1.0, 1.0, 3.0))
empty_queries = _handle(runtime, {"operation": "embed_queries", "texts": []})
assert isinstance(empty_queries, dict)
assert unpack_matrix(empty_queries["vectors"]) == (0, 0, ())
query = _handle(runtime, {"operation": "embed_query", "text": "abcd"})
assert isinstance(query, dict)
assert unpack_vector(query["vector"]) == (2, (4.0, 7.0))

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
