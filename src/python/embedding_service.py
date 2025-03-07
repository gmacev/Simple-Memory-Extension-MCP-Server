#!/usr/bin/env python3
"""
Embedding service using the E5 model from Hugging Face.
This script provides a bridge between Node.js and the E5 model.
"""

import sys
import json
import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel

# Initialize model
MODEL_NAME = "intfloat/multilingual-e5-large-instruct"
tokenizer = None
model = None

def initialize_model():
    """Initialize the model and tokenizer."""
    global tokenizer, model
    
    if tokenizer is None or model is None:
        print("Initializing E5 model...", file=sys.stderr)
        tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        model = AutoModel.from_pretrained(MODEL_NAME)
        print(f"Model initialized: {MODEL_NAME}", file=sys.stderr)

def average_pool(last_hidden_states, attention_mask):
    """Average pooling function for the model output."""
    last_hidden = last_hidden_states.masked_fill(~attention_mask[..., None].bool(), 0.0)
    return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]

def format_text_for_embedding(text, is_query=False):
    """
    Format text for embedding based on whether it's a query or passage.
    For queries, we add an instruction prefix.
    For passages, we use the text as is.
    """
    if is_query:
        # For queries, add instruction
        task_description = "Given a web search query, retrieve relevant passages that answer the query"
        return f'Instruct: {task_description}\nQuery: {text}'
    else:
        # For passages, use as is
        return text

def generate_embedding(text, is_query=False):
    """Generate embedding for a single text."""
    initialize_model()

    # Format text based on type
    input_text = format_text_for_embedding(text, is_query)

    # Tokenize
    encoded_input = tokenizer(
        input_text, 
        max_length=512, 
        padding=True, 
        truncation=True, 
        return_tensors='pt'
    )
                            
    # Generate embedding
    with torch.no_grad():
        model_output = model(**encoded_input)

    # Pool and normalize
    embedding = average_pool(model_output.last_hidden_state, encoded_input['attention_mask'])
    embedding = F.normalize(embedding, p=2, dim=1)

    # Convert to list
    return embedding[0].tolist()

def generate_embeddings(texts, is_query=False):
    """Generate embeddings for multiple texts."""
    initialize_model()

    if not texts or not isinstance(texts, list):
        raise ValueError("Texts must be a non-empty list of strings")
    
    # Format each text based on type
    input_texts = [format_text_for_embedding(text, is_query) for text in texts]

    # Tokenize
    encoded_input = tokenizer(
        input_texts, 
        max_length=512, 
        padding=True, 
        truncation=True, 
        return_tensors='pt'
    )
                            
    # Generate embeddings
    with torch.no_grad():
        model_output = model(**encoded_input)

    # Pool and normalize
    embeddings = average_pool(model_output.last_hidden_state, encoded_input['attention_mask'])
    embeddings = F.normalize(embeddings, p=2, dim=1)

    # Convert to list
    return embeddings.tolist()

def process_command(command_json):
    """Process a command from Node.js."""
    try:
        command = command_json.get("command")

        if command == "initialize":
            initialize_model()
            return {"status": "initialized"}

        elif command == "generate_embedding":
            text = command_json.get("text")
            is_query = command_json.get("is_query", False)

            if not text:
                return {"error": "No text provided"}

            embedding = generate_embedding(text, is_query)
            return {"embedding": embedding}

        elif command == "generate_embeddings":
            texts = command_json.get("texts")
            is_query = command_json.get("is_query", False)

            if not texts or not isinstance(texts, list):
                return {"error": "No texts provided or invalid format"}

            embeddings = generate_embeddings(texts, is_query)
            return {"embeddings": embeddings}

        else:
            return {"error": f"Unknown command: {command}"}

    except Exception as e:
        return {"error": str(e)}

def main():
    """Main function to process commands from stdin."""
    print("E5 Embedding Service started", file=sys.stderr)
    initialize_model()
    
    for line in sys.stdin:
        try:
            command_json = json.loads(line)
            result = process_command(command_json)
            print(json.dumps(result), flush=True)
        except json.JSONDecodeError:
            print(json.dumps({"error": "Invalid JSON"}), flush=True)
        except Exception as e:
            print(json.dumps({"error": str(e)}), flush=True)

if __name__ == "__main__":
    main() 