from __future__ import annotations

from inspect import signature
from types import SimpleNamespace

import torch
from sentence_transformers.cross_encoder.modules.logit_score import LogitScore
from transformers import BatchEncoding, DynamicCache, Qwen3Config

from simple_memory_models.prefix_reuse import (
    PrefixReuseState,
    common_prefix_length,
    predict_with_prefix_reuse,
)

assert common_prefix_length([]) == 0
assert common_prefix_length([[1], [1]]) == 0
assert common_prefix_length([[1, 2, 3], [1, 2, 4]]) == 2
assert common_prefix_length([[1, 2, 3], [1, 2, 3]]) == 2
assert common_prefix_length([[1] * 2000, [1] * 2000]) == 1024


class FakePaddingTokenizer:
    padding_side = "left"

    def pad(self, features, **_options):
        rows = features["input_ids"]
        width = max(map(len, rows))
        return BatchEncoding(
            {
                "input_ids": torch.tensor([[0] * (width - len(row)) + row for row in rows]),
                "attention_mask": torch.tensor(
                    [[0] * (width - len(row)) + [1] * len(row) for row in rows]
                ),
            }
        )


class FakeCausalModel:
    """Deterministic tensor bookkeeping only; no neural model or inference."""

    def __init__(self):
        self.device = "cpu"
        self.dtype = torch.float32
        self.config = Qwen3Config(num_hidden_layers=1)
        self.prefills = []
        self.continuations = []

    def __call__(self, input_ids, attention_mask, past_key_values=None, **_options):
        keys = input_ids.float()[:, None, :, None]
        if past_key_values is None:
            cache = DynamicCache(ddp_cache_data=[(keys, keys.clone())], config=self.config)
            self.prefills.append(cache)
            values = input_ids.sum(dim=1).float()
        else:
            cache = past_key_values
            prefix = cache.layers[0].keys[:, 0, :, 0].long()
            mask = attention_mask[:, prefix.shape[1] :]
            for before, after, valid in zip(prefix, input_ids, mask, strict=True):
                self.continuations.append(before.tolist() + after[valid.bool()].tolist())
            values = prefix.sum(dim=1).float() + (input_ids * mask).sum(dim=1).float()
            cache.update(keys, keys.clone(), 0)
        logits = torch.stack([torch.zeros_like(values), values / 1000], dim=-1)[:, None, :]
        return SimpleNamespace(past_key_values=cache, logits=logits)


class FakeEncoder:
    backend = "torch"

    def __init__(self):
        self.model = FakeCausalModel()
        self.module = SimpleNamespace(
            transformer_task="text-generation",
            config=self.model.config,
            tokenizer=FakePaddingTokenizer(),
            model=self.model,
        )
        self.head = LogitScore(true_token_id=1, false_token_id=0)
        self.preprocessed = []
        self.extra_feature = False

    def __len__(self):
        return 2

    def __getitem__(self, index):
        return [self.module, self.head][index]

    def eval(self):
        return self

    def _input_length(self, pair):
        return sum(map(len, pair))

    @staticmethod
    def tokens(pair):
        return [3] * 40 + [ord(char) % 97 + 4 for char in "\n".join(pair)] + [8]

    def preprocess(self, pairs, **_options):
        rows = [self.tokens(pair) for pair in pairs]
        self.preprocessed.extend(rows)
        features = self.module.tokenizer.pad({"input_ids": rows})
        if self.extra_feature:
            features["unsupported"] = True
        return features


pairs = [
    ("query", "longer evidence"),
    ("query", ""),
    ("different query", "unicode ąčęėįšųūž"),
    ("query", "short"),
    ("query", "short"),
    ("query", "another passage"),
]
if not {"ddp_cache_data", "config"}.issubset(signature(DynamicCache).parameters):
    assert predict_with_prefix_reuse(FakeEncoder(), pairs, "instruction", 4) is None
    print("Python prefix reuse probe passed: unsupported cache API uses normal prediction.")
    raise SystemExit(0)

encoder = FakeEncoder()
scores = predict_with_prefix_reuse(encoder, pairs, "instruction", 4)
expected = torch.sigmoid(torch.tensor([sum(encoder.tokens(pair)) / 1000 for pair in pairs]))
assert scores is not None
torch.testing.assert_close(torch.tensor(scores), expected)
assert encoder.model.continuations == encoder.preprocessed[:-1]
assert len(encoder.model.prefills) == 1
prefix_size = common_prefix_length(encoder.preprocessed)
assert encoder.model.prefills[0].get_seq_length() == prefix_size

# A new request gets a fresh cache; duplicate full inputs leave a continuation token.
identical = [("same", "same")] * 5
again = predict_with_prefix_reuse(encoder, identical, "instruction", 4)
assert again is not None and len(set(again)) == 1
assert len(encoder.model.prefills) == 2
assert encoder.model.prefills[0].get_seq_length() == prefix_size

small = FakeEncoder()
assert predict_with_prefix_reuse(small, pairs[:3], "instruction", 4) is None
assert small.preprocessed == []
assert predict_with_prefix_reuse(small, pairs, "instruction", 0) is None
small.backend = "onnx"
assert predict_with_prefix_reuse(small, pairs, "instruction", 4) is None
assert small.preprocessed == []
unsupported = FakeEncoder()
unsupported.extra_feature = True
assert predict_with_prefix_reuse(unsupported, pairs, "instruction", 4) is None
assert unsupported.model.prefills == []

# Match the real scheduler: four pairs, then a partial batch for the same query.
chunked = FakeEncoder()
state = PrefixReuseState()
chunk_pairs = [("query", document) for _, document in pairs]
chunk_expected = torch.sigmoid(
    torch.tensor([sum(chunked.tokens(pair)) / 1000 for pair in chunk_pairs])
)
first = predict_with_prefix_reuse(chunked, chunk_pairs[:4], "instruction", 4, state)
second = predict_with_prefix_reuse(chunked, chunk_pairs[4:], "instruction", 4, state)
assert first is not None and second is not None
torch.testing.assert_close(torch.tensor(first + second), chunk_expected)
assert len(chunked.model.prefills) == 1
changed = [("new query", "new passage")]
third = predict_with_prefix_reuse(chunked, changed, "instruction", 4, state)
assert third is None and state.cache is None
assert len(chunked.model.prefills) == 1
assert len(state.tokens) <= 1024
new_scores = predict_with_prefix_reuse(chunked, changed * 4, "instruction", 4, state)
assert new_scores is not None
assert state.cache is not None and state.cache.get_seq_length() == len(state.tokens)

# A changed model identity/precision cannot consume another model's cached state.
chunked.model.dtype = torch.float16
assert predict_with_prefix_reuse(chunked, changed, "instruction", 4, state) is None
assert state.cache is None

print("Python prefix reuse probe passed.")
