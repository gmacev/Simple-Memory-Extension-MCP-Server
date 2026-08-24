from __future__ import annotations

import base64
import json
import os
import sys
import traceback
from typing import Any

import numpy as np

from .runtime import ModelRuntime, RuntimeConfig


def _write(payload: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


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


def _packed_float32_matrix(values: object) -> dict[str, object]:
    matrix = np.asarray(values, dtype="<f4")
    if matrix.size == 0:
        matrix = np.empty((0, 0), dtype="<f4")
    if matrix.ndim != 2:
        raise ValueError("Embedding result must be a two-dimensional matrix")
    contiguous = np.ascontiguousarray(matrix, dtype="<f4")
    return {
        "encoding": "base64-f32le",
        "rows": int(contiguous.shape[0]),
        "dimensions": int(contiguous.shape[1]),
        "data": base64.b64encode(contiguous.tobytes()).decode("ascii"),
    }


def _packed_float32_vector(values: object) -> dict[str, object]:
    vector = np.asarray(values, dtype="<f4")
    if vector.ndim != 1:
        raise ValueError("Embedding result must be a one-dimensional vector")
    contiguous = np.ascontiguousarray(vector, dtype="<f4")
    return {
        "encoding": "base64-f32le",
        "dimensions": int(contiguous.shape[0]),
        "data": base64.b64encode(contiguous.tobytes()).decode("ascii"),
    }


def _handle(runtime: ModelRuntime, payload: dict[str, Any]) -> object:
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
            "vector": _packed_float32_vector(
                runtime.embed_query(_require_string(payload, "text"))
            )
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


def main() -> None:
    runtime = ModelRuntime(RuntimeConfig.from_environment())
    print(
        f"simple-memory model worker ready pid={os.getpid()} device={runtime.config.device}",
        file=sys.stderr,
        flush=True,
    )
    for line in sys.stdin:
        request_id: object = None
        try:
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError("Request must be a JSON object")
            request_id = payload.get("id")
            result = _handle(runtime, payload)
            _write({"id": request_id, "ok": True, "result": result})
            if payload.get("operation") == "shutdown":
                return
        except Exception as error:
            print(traceback.format_exc(), file=sys.stderr, flush=True)
            _write(
                {
                    "id": request_id,
                    "ok": False,
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )


if __name__ == "__main__":
    main()
