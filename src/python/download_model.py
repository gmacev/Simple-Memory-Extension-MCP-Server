
import os
from transformers import AutoTokenizer, AutoModel

# Set the model name
MODEL_NAME = "intfloat/multilingual-e5-large-instruct"

print(f"Pre-downloading model: {MODEL_NAME}")
print("This might take a few minutes...")

# Download the model and tokenizer
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModel.from_pretrained(MODEL_NAME)

print(f"Model downloaded and cached at: {os.path.expanduser('~/.cache/huggingface/transformers')}")
print("Setup completed successfully!")
