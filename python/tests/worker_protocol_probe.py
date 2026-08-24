from __future__ import annotations

import base64
import struct
from typing import Any

from simple_memory_models.worker_protocol import handle_request


class FakeFloat32Array:
    def __init__(self, shape: tuple[int, ...], values: list[float]) -> None:
        self.shape = shape
        self.values = values
        self.ndim = len(shape)
        self.size = len(values)

    def astype(self, dtype: str, *, copy: bool) -> FakeFloat32Array:
        assert dtype == "<f4"
        assert copy is False
        return self

    def tobytes(self, order: str = "C") -> bytes:
        assert order == "C"
        return struct.pack(f"<{len(self.values)}f", *self.values)


class FakeRuntime:
    def embed_queries(self, texts: list[str]) -> FakeFloat32Array:
        if not texts:
            return FakeFloat32Array((0, 0), [])
        values = [value for index, text in enumerate(texts) for value in (index, len(text))]
        return FakeFloat32Array((len(texts), 2), [float(value) for value in values])

    def embed_documents(self, texts: list[str]) -> FakeFloat32Array:
        if not texts:
            return FakeFloat32Array((0, 0), [])
        return FakeFloat32Array((len(texts), 1), [float(len(text)) for text in texts])

    def embed_query(self, text: str) -> FakeFloat32Array:
        return FakeFloat32Array((2,), [float(len(text)), 7.0])

    def count_tokens(self, texts: list[str]) -> list[int]:
        return [len(text) for text in texts]

    def rerank_pairs(self, pairs: list[tuple[str, str]]) -> list[float]:
        return [float(index) for index, _pair in enumerate(pairs)]


def expect_error(payload: dict[str, Any]) -> None:
    try:
        handle_request(FakeRuntime(), payload)
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
queries = handle_request(runtime, {"operation": "embed_queries", "texts": ["a", "bbb"]})
assert isinstance(queries, dict)
assert unpack_matrix(queries["vectors"]) == (2, 2, (0.0, 1.0, 1.0, 3.0))
empty_queries = handle_request(runtime, {"operation": "embed_queries", "texts": []})
assert isinstance(empty_queries, dict)
assert unpack_matrix(empty_queries["vectors"]) == (0, 0, ())
query = handle_request(runtime, {"operation": "embed_query", "text": "abcd"})
assert isinstance(query, dict)
assert unpack_vector(query["vector"]) == (2, (4.0, 7.0))

pairs = [
    {"query": "q1", "document": "d1"},
    {"query": "q2", "document": "d2"},
]
assert handle_request(runtime, {"operation": "rerank_pairs", "pairs": pairs}) == {
    "scores": [0.0, 1.0]
}
assert handle_request(runtime, {"operation": "rerank_pairs", "pairs": []}) == {"scores": []}

expect_error({"operation": "embed_queries", "texts": ["valid", 2]})
expect_error({"operation": "rerank_pairs", "pairs": [{"query": "q"}]})
expect_error({"operation": "rerank_pairs", "pairs": [{"query": "", "document": "d"}]})
expect_error(
    {
        "operation": "rerank_pairs",
        "pairs": [{"query": "q", "document": "d", "extra": True}],
    }
)

print("Python worker protocol probe passed.")
