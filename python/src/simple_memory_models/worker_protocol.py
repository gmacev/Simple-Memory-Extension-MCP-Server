from __future__ import annotations

import base64
import os
from typing import Any, Protocol, Self


class Float32Array(Protocol):
    size: int
    ndim: int
    shape: tuple[int, ...]

    def astype(self, dtype: str, *, copy: bool) -> Self: ...

    def tobytes(self, order: str = "C") -> bytes: ...


class WorkerRuntime(Protocol):
    def model_info(self) -> dict[str, object]: ...

    def embed_documents(self, texts: list[str]) -> Float32Array: ...

    def embed_queries(self, texts: list[str]) -> Float32Array: ...

    def count_tokens(self, texts: list[str]) -> list[int]: ...

    def embed_query(self, text: str) -> Float32Array: ...

    def rerank(self, query: str, documents: list[str]) -> list[float]: ...

    def rerank_pairs(self, pairs: list[tuple[str, str]]) -> list[float]: ...


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _require_strings(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key} must be an array of strings")
    return value


def _require_pairs(payload: dict[str, Any], key: str) -> list[tuple[str, str]]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ValueError(f"{key} must be an array of query/document objects")
    pairs: list[tuple[str, str]] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"query", "document"}:
            raise ValueError(f"{key} must contain only query/document objects")
        query = item.get("query")
        document = item.get("document")
        if not isinstance(query, str) or not query or not isinstance(document, str):
            raise ValueError(f"{key} entries require a non-empty query and string document")
        pairs.append((query, document))
    return pairs


def _float32le_bytes(values: Float32Array, expected_values: int) -> bytes:
    converted = values.astype("<f4", copy=False)
    data = converted.tobytes(order="C")
    expected_bytes = expected_values * 4
    if len(data) != expected_bytes:
        raise ValueError(
            f"Embedding byte length mismatch: expected {expected_bytes}, received {len(data)}"
        )
    return data


def _packed_float32_matrix(values: Float32Array) -> dict[str, object]:
    if values.ndim != 2:
        raise ValueError("Embedding result must be a two-dimensional matrix")
    rows, dimensions = (int(value) for value in values.shape)
    if (rows == 0) != (dimensions == 0):
        raise ValueError("Embedding result has an invalid empty matrix shape")
    data = _float32le_bytes(values, rows * dimensions)
    return {
        "encoding": "base64-f32le",
        "rows": rows,
        "dimensions": dimensions,
        "data": base64.b64encode(data).decode("ascii"),
    }


def _packed_float32_vector(values: Float32Array) -> dict[str, object]:
    if values.ndim != 1:
        raise ValueError("Embedding result must be a one-dimensional vector")
    dimensions = int(values.shape[0])
    data = _float32le_bytes(values, dimensions)
    return {
        "encoding": "base64-f32le",
        "dimensions": dimensions,
        "data": base64.b64encode(data).decode("ascii"),
    }


def handle_request(runtime: WorkerRuntime, payload: dict[str, Any]) -> object:
    operation = _require_string(payload, "operation")
    if operation == "health":
        return {"status": "ok", "pid": os.getpid(), **runtime.model_info()}
    if operation == "model_info":
        return runtime.model_info()
    if operation == "embed_documents":
        return {
            "vectors": _packed_float32_matrix(
                runtime.embed_documents(_require_strings(payload, "texts"))
            )
        }
    if operation == "embed_queries":
        return {
            "vectors": _packed_float32_matrix(
                runtime.embed_queries(_require_strings(payload, "texts"))
            )
        }
    if operation == "count_tokens":
        return {"counts": runtime.count_tokens(_require_strings(payload, "texts"))}
    if operation == "embed_query":
        return {
            "vector": _packed_float32_vector(runtime.embed_query(_require_string(payload, "text")))
        }
    if operation == "rerank":
        return {
            "scores": runtime.rerank(
                _require_string(payload, "query"), _require_strings(payload, "documents")
            )
        }
    if operation == "rerank_pairs":
        return {"scores": runtime.rerank_pairs(_require_pairs(payload, "pairs"))}
    if operation == "shutdown":
        return {"status": "shutting_down"}
    raise ValueError(f"Unknown operation: {operation}")
