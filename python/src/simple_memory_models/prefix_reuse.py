from __future__ import annotations

import inspect
from dataclasses import dataclass, field

import numpy as np
import torch
from sentence_transformers import CrossEncoder
from sentence_transformers.cross_encoder.modules.logit_score import LogitScore
from transformers import DynamicCache

# Avoid creating a new cache for a small request; existing prefixes can serve any size.
MIN_PREFILL_PAIRS = 4
MIN_PREFIX_TOKENS = 32
MAX_PREFIX_TOKENS = 1024


@dataclass(slots=True)
class PrefixReuseState:
    tokens: list[int] = field(default_factory=list)
    cache: DynamicCache | None = None
    identity: tuple[int, torch.dtype, torch.device] | None = None
    query: str | None = None


def common_prefix_length(rows: list[list[int]], limit: int = MAX_PREFIX_TOKENS) -> int:
    if not rows:
        return 0
    # Leave a continuation token even when all inputs are identical.
    stop = min(limit, min(map(len, rows)) - 1)
    for index in range(max(0, stop)):
        if any(row[index] != rows[0][index] for row in rows[1:]):
            return index
    return max(0, stop)


@torch.inference_mode()
def predict_with_prefix_reuse(
    encoder: CrossEncoder,
    pairs: list[tuple[str, str]],
    prompt: str,
    batch_size: int,
    state: PrefixReuseState | None = None,
) -> list[float] | None:
    """Reuse exact causal prefix tokens within one request, or request normal prediction.

    No text is reformatted or retokenized separately at the prefix boundary. Each branch
    owns its cache. One bounded exact query prefix can survive worker calls for the
    same query; document continuations are never retained. Candidate selection runs normally.
    Equivalent causal computation can still differ numerically with reduced precision.
    """
    if not pairs or batch_size <= 0:
        return None
    if state is None:
        state = PrefixReuseState()
    if len(pairs) < MIN_PREFILL_PAIRS and state.cache is None:
        return None
    # CrossEncoder supports other model families and older Transformers backends.
    # Only bypass its forward path for the validated Qwen causal/LogitScore layout.
    if len(encoder) != 2 or not isinstance(encoder[1], LogitScore):
        return None
    module = encoder[0]
    if (
        encoder.backend != "torch"
        or module.transformer_task != "text-generation"
        or module.config.model_type != "qwen3"
        or module.tokenizer is None
        or module.tokenizer.padding_side != "left"
        or "ddp_cache_data" not in inspect.signature(DynamicCache).parameters
        or "config" not in inspect.signature(DynamicCache).parameters
        or not callable(getattr(encoder, "_input_length", None))
    ):
        return None

    encoder.eval()
    transformer = module.model
    device = transformer.device
    identity = (id(transformer), transformer.dtype, device)
    query = pairs[0][0] if all(pair[0] == pairs[0][0] for pair in pairs) else None
    if state.identity != identity or query is None or state.query != query:
        state.tokens, state.cache, state.identity = [], None, identity
        state.query = query
    if len(pairs) < MIN_PREFILL_PAIRS and state.cache is None:
        return None
    order = np.argsort([-encoder._input_length(pair) for pair in pairs])
    batches: list[list[list[int]]] = []
    rows: list[list[int]] = []
    for start in range(0, len(order), batch_size):
        features = encoder.preprocess(
            [pairs[index] for index in order[start : start + batch_size]], prompt=prompt
        )
        if not set(features).issubset(
            {"input_ids", "attention_mask", "modality", "logits_to_keep"}
        ):
            return None
        ids, mask = features.get("input_ids"), features.get("attention_mask")
        if not isinstance(ids, torch.Tensor) or not isinstance(mask, torch.Tensor):
            return None
        if ids.ndim != 2 or ids.shape != mask.shape:
            return None
        token_rows = [
            tokens[valid.bool()].tolist() for tokens, valid in zip(ids, mask, strict=True)
        ]
        batches.append(token_rows)
        rows.extend(token_rows)

    # An empty-document probe bounds caching to the query/template prefix. It is not
    # scored and does not replace or reformat any actual candidate's token input.
    probe = encoder.preprocess([(pairs[0][0], "")], prompt=prompt)
    probe_ids, probe_mask = probe.get("input_ids"), probe.get("attention_mask")
    if not isinstance(probe_ids, torch.Tensor) or not isinstance(probe_mask, torch.Tensor):
        return None
    probe_tokens = probe_ids[0][probe_mask[0].bool()].tolist()
    prefix_length = common_prefix_length([*rows, probe_tokens])
    if prefix_length < MIN_PREFIX_TOKENS:
        return None
    matching = 0
    for index in range(min(prefix_length, len(state.tokens))):
        if state.tokens[index] != rows[0][index]:
            break
        matching += 1
    if state.cache is not None and matching >= MIN_PREFIX_TOKENS:
        prefix_length = matching
        source_cache = state.cache
    else:
        if len(pairs) < MIN_PREFILL_PAIRS:
            return None
        prefix_ids = torch.tensor([rows[0][:prefix_length]], dtype=torch.long, device=device)
        prefill = transformer(
            input_ids=prefix_ids,
            attention_mask=torch.ones_like(prefix_ids),
            use_cache=True,
            logits_to_keep=1,
        )
        source_cache = prefill.past_key_values
        if not isinstance(source_cache, DynamicCache) or not hasattr(source_cache, "layers"):
            return None
        if source_cache.get_seq_length() != prefix_length:
            raise RuntimeError("Reranker prefix cache has an unexpected length")
        state.tokens, state.cache = rows[0][:prefix_length], source_cache
    source_length = source_cache.get_seq_length()

    scores: list[float] = []
    for token_rows in batches:
        continuations = [row[prefix_length:] for row in token_rows]
        features = module.tokenizer.pad(
            {"input_ids": continuations}, padding=True, return_tensors="pt"
        ).to(device)
        ids, mask = features["input_ids"], features["attention_mask"]
        count = len(continuations)
        full_mask = torch.cat(
            [torch.ones((count, prefix_length), dtype=mask.dtype, device=device), mask], dim=1
        )
        # Padding between the cached prefix and the continuation has no real position.
        positions = (mask.cumsum(dim=1) - 1 + prefix_length).masked_fill(mask == 0, 0)
        cache = DynamicCache(
            ddp_cache_data=[
                (
                    layer.keys[:, :, :prefix_length, :].expand(count, -1, -1, -1).clone(),
                    layer.values[:, :, :prefix_length, :].expand(count, -1, -1, -1).clone(),
                )
                for layer in source_cache.layers
            ],
            config=transformer.config,
        )
        output = transformer(
            input_ids=ids,
            attention_mask=full_mask,
            position_ids=positions,
            past_key_values=cache,
            use_cache=True,
            logits_to_keep=1,
        )
        logits = encoder[1]({"causal_logits": output.logits})["scores"]
        scores.extend(torch.sigmoid(logits).squeeze(-1).cpu().float().tolist())
        if source_cache.get_seq_length() != source_length:
            raise RuntimeError("Reranker continuation modified the shared prefix cache")

    return [scores[index] for index in np.argsort(order)]
