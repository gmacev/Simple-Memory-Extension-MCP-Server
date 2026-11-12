from __future__ import annotations

import platform
import sys

import torch
from transformers import AutoConfig, AutoModelForSequenceClassification, PreTrainedTokenizerFast
from transformers.utils import cached_file


class Reranker:
    def __init__(
        self,
        model_id: str,
        revision: str,
        device: str,
        batch_size: int,
        local_files_only: bool,
    ) -> None:
        self.device = device
        self.batch_size = batch_size
        pretrained = {"revision": revision, "local_files_only": local_files_only}
        config = AutoConfig.from_pretrained(model_id, **pretrained)
        if config.num_labels != 1:
            raise ValueError("The reranker must produce one relevance logit per pair")

        tokenizer_file = cached_file(model_id, "tokenizer.json", **pretrained)
        if tokenizer_file is None:
            raise RuntimeError(f"Missing tokenizer.json for {model_id}@{revision}")

        # Load the serialized pair template directly. This works with both supported
        # Transformers generations. ModernBERT pools CLS at position zero.
        self.tokenizer = PreTrainedTokenizerFast(
            tokenizer_file=tokenizer_file,
            padding_side="right",
            model_max_length=config.max_position_embeddings,
        )
        self.tokenizer.pad_token = self.tokenizer.convert_ids_to_tokens(config.pad_token_id)

        # The published ONNX export runs on Intel macOS, where PyTorch 2.2 cannot
        # import the ModernBERT classifier under Python 3.12.
        self.onnx = sys.platform == "darwin" and platform.machine() == "x86_64"
        if self.onnx:
            import onnxruntime

            onnx_file = cached_file(model_id, "onnx/model.onnx", **pretrained)
            if onnx_file is None:
                raise RuntimeError(f"Missing ONNX reranker for {model_id}@{revision}")
            self.session = onnxruntime.InferenceSession(
                onnx_file, providers=["CPUExecutionProvider"]
            )
        else:
            device_type = device.partition(":")[0]
            dtype = torch.float16 if device_type in {"cuda", "xpu", "mps"} else torch.float32
            self.model = (
                AutoModelForSequenceClassification.from_pretrained(
                    model_id, config=config, torch_dtype=dtype, use_safetensors=True, **pretrained
                )
                .to(device)
                .eval()
            )

    @torch.inference_mode()
    def predict(self, pairs: list[tuple[str, str]]) -> list[float]:
        order = sorted(
            range(len(pairs)), key=lambda i: len(pairs[i][0]) + len(pairs[i][1]), reverse=True
        )
        scores = [0.0] * len(pairs)
        for start in range(0, len(order), self.batch_size):
            indices = order[start : start + self.batch_size]
            inputs = self.tokenizer(
                [pairs[i][0] for i in indices],
                [pairs[i][1] for i in indices],
                padding=True,
                truncation=True,
                return_token_type_ids=False,
                return_tensors="np" if self.onnx else "pt",
            )
            if self.onnx:
                feeds = {name: value.astype("int64", copy=False) for name, value in inputs.items()}
                (output,) = self.session.run(["logits"], feeds)
                logits = torch.from_numpy(output)
            else:
                logits = self.model(**inputs.to(self.device)).logits
            for index, score in zip(indices, logits.squeeze(-1).sigmoid().tolist(), strict=True):
                scores[index] = score
        return scores
