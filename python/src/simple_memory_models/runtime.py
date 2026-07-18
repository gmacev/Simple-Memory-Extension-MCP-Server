from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

import torch
from sentence_transformers import CrossEncoder, SentenceTransformer
from transformers import AutoTokenizer, PreTrainedTokenizerBase

DEFAULT_EMBEDDING_MODEL = "Qwen/Qwen3-Embedding-0.6B"
DEFAULT_RERANKER_MODEL = "Qwen/Qwen3-Reranker-0.6B"
DEFAULT_EMBEDDING_REVISION = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3"
DEFAULT_RERANKER_REVISION = "e61197ed45024b0ed8a2d74b80b4d909f1255473"
DEFAULT_QUERY_INSTRUCTION = (
    "Given a memory query, retrieve stored information useful for answering the query "
    "or guiding an action."
)
DEFAULT_RERANK_INSTRUCTION = (
    "Given a memory query, determine whether the candidate memory contains information "
    "useful for answering or acting on it."
)


def accelerator_available(name: str) -> bool:
    device_type, _, raw_index = name.partition(":")
    index = int(raw_index) if raw_index else 0
    if device_type == "cuda":
        return torch.cuda.is_available() and index < torch.cuda.device_count()
    if device_type == "xpu":
        xpu = getattr(torch, "xpu", None)
        return bool(xpu and xpu.is_available() and index < xpu.device_count())
    if device_type == "mps":
        mps = getattr(torch.backends, "mps", None)
        return bool(mps and mps.is_available())
    return device_type == "cpu"


def preferred_device() -> str:
    for device in ("cuda", "xpu", "mps"):
        if accelerator_available(device):
            return device
    return "cpu"


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    embedding_model: str
    embedding_revision: str
    reranker_model: str
    reranker_revision: str
    query_instruction: str
    rerank_instruction: str
    device: str
    embedding_batch_size: int
    rerank_batch_size: int
    local_files_only: bool

    @classmethod
    def from_environment(cls) -> RuntimeConfig:
        requested_device = os.getenv("SIMPLE_MEMORY_DEVICE", "auto").strip().lower()
        device = preferred_device() if requested_device == "auto" else requested_device
        if not accelerator_available(device):
            raise RuntimeError(
                f"Requested device {device!r} is unavailable in the installed PyTorch backend"
            )
        return cls(
            embedding_model=os.getenv("SIMPLE_MEMORY_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
            embedding_revision=os.getenv(
                "SIMPLE_MEMORY_EMBEDDING_REVISION", DEFAULT_EMBEDDING_REVISION
            ),
            reranker_model=os.getenv("SIMPLE_MEMORY_RERANKER_MODEL", DEFAULT_RERANKER_MODEL),
            reranker_revision=os.getenv(
                "SIMPLE_MEMORY_RERANKER_REVISION", DEFAULT_RERANKER_REVISION
            ),
            query_instruction=os.getenv(
                "SIMPLE_MEMORY_QUERY_INSTRUCTION", DEFAULT_QUERY_INSTRUCTION
            ),
            rerank_instruction=os.getenv(
                "SIMPLE_MEMORY_RERANK_INSTRUCTION", DEFAULT_RERANK_INSTRUCTION
            ),
            device=device,
            embedding_batch_size=int(os.getenv("SIMPLE_MEMORY_EMBED_BATCH_SIZE", "8")),
            rerank_batch_size=int(os.getenv("SIMPLE_MEMORY_RERANK_BATCH_SIZE", "4")),
            local_files_only=os.getenv("SIMPLE_MEMORY_LOCAL_FILES_ONLY", "false").lower() == "true",
        )


class ModelRuntime:
    def __init__(self, config: RuntimeConfig) -> None:
        self.config = config
        self._embedding: SentenceTransformer | None = None
        self._reranker: CrossEncoder | None = None
        self._tokenizer: PreTrainedTokenizerBase | None = None
        self._lock = threading.RLock()

    @property
    def embedding_loaded(self) -> bool:
        return self._embedding is not None

    @property
    def reranker_loaded(self) -> bool:
        return self._reranker is not None

    def _model_kwargs(self) -> dict[str, Any]:
        device_type = self.config.device.partition(":")[0]
        if device_type == "cuda" and torch.cuda.is_bf16_supported():
            return {"torch_dtype": torch.bfloat16}
        if device_type == "xpu":
            xpu = getattr(torch, "xpu", None)
            supports_bf16 = getattr(xpu, "is_bf16_supported", None)
            if supports_bf16 and supports_bf16():
                return {"torch_dtype": torch.bfloat16}
        if device_type in {"cuda", "xpu", "mps"}:
            return {"torch_dtype": torch.float16}
        return {"torch_dtype": torch.float32}

    def _device_name(self) -> str:
        device_type, _, raw_index = self.config.device.partition(":")
        index = int(raw_index) if raw_index else 0
        if device_type == "cuda":
            return torch.cuda.get_device_name(index)
        if device_type == "xpu":
            xpu = getattr(torch, "xpu", None)
            return str(xpu.get_device_name(index)) if xpu else "Intel XPU"
        if device_type == "mps":
            return "Apple Metal"
        return "CPU"

    def _get_embedding(self) -> SentenceTransformer:
        with self._lock:
            if self._embedding is None:
                self._embedding = SentenceTransformer(
                    self.config.embedding_model,
                    revision=self.config.embedding_revision,
                    device=self.config.device,
                    model_kwargs=self._model_kwargs(),
                    processor_kwargs={"padding_side": "left"},
                    trust_remote_code=True,
                    local_files_only=self.config.local_files_only,
                )
            return self._embedding

    def _get_reranker(self) -> CrossEncoder:
        with self._lock:
            if self._reranker is None:
                self._reranker = CrossEncoder(
                    self.config.reranker_model,
                    revision=self.config.reranker_revision,
                    device=self.config.device,
                    model_kwargs=self._model_kwargs(),
                    processor_kwargs={"padding_side": "left"},
                    prompts={"memory": self.config.rerank_instruction},
                    default_prompt_name="memory",
                    trust_remote_code=True,
                    local_files_only=self.config.local_files_only,
                )
            return self._reranker

    def _get_tokenizer(self) -> PreTrainedTokenizerBase:
        with self._lock:
            if self._tokenizer is None:
                self._tokenizer = AutoTokenizer.from_pretrained(
                    self.config.embedding_model,
                    revision=self.config.embedding_revision,
                    trust_remote_code=True,
                    local_files_only=self.config.local_files_only,
                )
            return self._tokenizer

    def count_tokens(self, texts: list[str]) -> list[int]:
        tokenizer = self._get_tokenizer()
        return [
            len(tokenizer.encode(text, add_special_tokens=True, truncation=False)) for text in texts
        ]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        model = self._get_embedding()
        vectors = model.encode(
            texts,
            batch_size=self.config.embedding_batch_size,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return vectors.astype("float32", copy=False).tolist()

    def embed_query(self, text: str) -> list[float]:
        model = self._get_embedding()
        prompt = f"Instruct: {self.config.query_instruction}\nQuery: "
        vector = model.encode(
            [text],
            prompt=prompt,
            batch_size=1,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )[0]
        return vector.astype("float32", copy=False).tolist()

    def rerank(self, query: str, documents: list[str]) -> list[float]:
        if not documents:
            return []
        model = self._get_reranker()
        pairs = [(query, document) for document in documents]
        scores = model.predict(
            pairs,
            batch_size=self.config.rerank_batch_size,
            show_progress_bar=False,
            activation_fn=torch.nn.Sigmoid(),
        )
        raw_scores = scores.tolist() if hasattr(scores, "tolist") else list(scores)
        return [float(score[0] if isinstance(score, list) else score) for score in raw_scores]

    def model_info(self) -> dict[str, object]:
        embedding_dimension: int | None = None
        if self._embedding is not None:
            embedding_dimension = self._embedding.get_sentence_embedding_dimension()
        return {
            "embedding_model": self.config.embedding_model,
            "embedding_revision": self.config.embedding_revision,
            "reranker_model": self.config.reranker_model,
            "reranker_revision": self.config.reranker_revision,
            "query_instruction_hash": sha256(
                self.config.query_instruction.encode("utf-8")
            ).hexdigest(),
            "rerank_instruction_hash": sha256(
                self.config.rerank_instruction.encode("utf-8")
            ).hexdigest(),
            "device": self.config.device,
            "device_name": self._device_name(),
            "torch_version": torch.__version__,
            "torch_cuda_version": torch.version.cuda,
            "embedding_dimension": embedding_dimension,
            "embedding_loaded": self.embedding_loaded,
            "reranker_loaded": self.reranker_loaded,
        }
