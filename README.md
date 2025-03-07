# Simple Memory Extension MCP Server

An MCP server to extend the context of agents. Useful when coding big features or vibe coding and need to store/recall progress, key moments or changes or anything worth remembering. Simply ask the agent to store memories and recall whenever you want.

## Usage

### Starting the Server

```bash
npm install
npm start
```

### Available Tools

#### Context Item Management
- `store_context_item` - Store a value with key in namespace
- `retrieve_context_item_by_key` - Get value by key
- `delete_context_item` - Delete key-value pair

#### Namespace Management
- `create_namespace` - Create new namespace
- `delete_namespace` - Delete namespace and all contents
- `list_namespaces` - List all namespaces
- `list_context_item_keys` - List keys in a namespace

#### Semantic Search
- `retrieve_context_items_by_semantic_search` - Find items by meaning

### Semantic Search Implementation

1. Query converted to vector using E5 model
2. Text automatically split into chunks for better matching
3. Cosine similarity calculated between query and stored chunks
4. Results filtered by threshold and sorted by similarity
5. Top matches returned with full item values

## Development

```bash
# Dev server
npm run dev

# Format code
npm run format
```

## Semantic Search

This project includes semantic search capabilities using the E5 embedding model from Hugging Face. This allows you to find context items based on their meaning rather than just exact key matches.

### Setup

The semantic search feature requires Python dependencies, but these *should be* automatically installed when you run: `npm run start`

### Embedding Model

We use the [intfloat/multilingual-e5-large-instruct](https://huggingface.co/intfloat/multilingual-e5-large-instruct)


### Notes

Developed mostly while vibe coding, so don't expect much :D. But it works, and I found it helpful so w/e. Feel free to contribute or suggest improvements.
