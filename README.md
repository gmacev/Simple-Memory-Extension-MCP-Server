# Simple Memory

Simple Memory is a local, persistent memory layer for AI agents using the Model Context Protocol (MCP).

It gives agents a place to store and recall information across separate chats, tasks, and applications. Memories can contain any JSON data, so the server does not impose a specific workflow or domain.

## What is it for?

Simple Memory can help an agent remember:

- Decisions, facts, risks, and ongoing work across multiple conversations
- Business operations, customers, agreements, and organizational knowledge
- Research findings together with their sources and confidence
- Plans, preferences, notes, and long-running personal projects
- Relationships and dependencies between stored information

Memories support revisions, provenance, time-aware retrieval, semantic search, archiving, and relationships. Ordinary recall uses active, current, presently valid information. An optional review date can warn an agent that information may need confirmation without hiding or changing it. Search is multilingual and combines exact, lexical, and semantic retrieval.

## Models

Simple Memory uses two local models:

- **Qwen3-Embedding-0.6B** converts memories and queries into vectors for multilingual semantic retrieval.
- **Qwen3-Reranker-0.6B** reviews the best candidates and improves their final ordering.

They were selected because they provide strong multilingual retrieval in a relatively small size that remains practical to run locally. Inference automatically prefers a supported GPU and falls back to CPU.

## Where is memory stored?

Memories are stored locally in a SQLite database named `memory.db`.

| Operating system | Default location |
| --- | --- |
| Windows | `%LOCALAPPDATA%\simple-memory\memory.db` |
| macOS | `~/Library/Application Support/simple-memory/memory.db` |
| Linux | `$XDG_DATA_HOME/simple-memory/memory.db`, or `~/.local/share/simple-memory/memory.db` |

The location can be changed with:

- `SIMPLE_MEMORY_DATA_DIR` for a different data directory
- `SIMPLE_MEMORY_DB_PATH` for a specific database file

Model files are stored separately in the standard Hugging Face cache.

## Installation

Requirements:

- Node.js 22.9 or newer
- npm 10 or newer
- Internet access during the first model download

Clone the repository and run the setup command:

```bash
git clone https://github.com/gmacev/Simple-Memory-Extension-MCP-Server.git
cd Simple-Memory-Extension-MCP-Server
npm run setup
```

Or just point your agent to this repo.

Setup installs the Node and Python dependencies, downloads any missing models, selects the best available GPU or CPU backend, builds the server, and verifies the installation. The models are public, so a Hugging Face key is not required.

Python, `uv`, and `curl` do not need to be installed beforehand. Setup downloads the official `uv` installer through Node.js, and `uv` installs the managed Python 3.12 runtime.

Configure your MCP client to launch the server through stdio. Example Codex configuration:

```toml
[mcp_servers.simple-memory]
command = "node"
args = ["dist/index.js"]
cwd = "/absolute/path/to/Simple-Memory-Extension-MCP-Server"
startup_timeout_sec = 120
tool_timeout_sec = 900
```

Replace `cwd` with the absolute path to the cloned repository. The MCP client starts the server automatically; `npm start` does not need to run separately.

## Updating

Completely stop the MCP client that is using Simple Memory, then update the repository and installation. The server must not be running because loaded native dependencies may need to be replaced:

```bash
git pull
npm run update
```

Restart the MCP client afterward.

## Environment variables

All configuration is optional.

### General

| Variable | Purpose | Default |
| --- | --- | --- |
| `SIMPLE_MEMORY_DATA_DIR` | Memory data directory | Platform location listed above |
| `SIMPLE_MEMORY_DB_PATH` | Complete SQLite database path | `<data-dir>/memory.db` |
| `SIMPLE_MEMORY_MODELS` | Set to `disabled` for lexical-only operation | `enabled` |
| `SIMPLE_MEMORY_DEVICE` | Runtime device such as `cuda`, `xpu`, `mps`, or `cpu` | `auto` |
| `SIMPLE_MEMORY_LOCAL_FILES_ONLY` | Prevent model downloads and use the local cache only | `false` |
| `SIMPLE_MEMORY_LOG_LEVEL` | `debug`, `info`, `warn`, or `error` | `info` |
| `SIMPLE_MEMORY_MODEL_TIMEOUT_MS` | Model request timeout | `600000` |

### Transport

| Variable | Purpose | Default |
| --- | --- | --- |
| `SIMPLE_MEMORY_TRANSPORT` | `stdio` or Streamable `http` | `stdio` |
| `SIMPLE_MEMORY_HTTP_HOST` | HTTP bind address | `127.0.0.1` |
| `SIMPLE_MEMORY_HTTP_PORT` | HTTP port | `3000` |
| `SIMPLE_MEMORY_HTTP_TOKEN` | Optional bearer token | Unset |
| `SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS` | Comma-separated browser origins allowed to call HTTP | Local server origins; required for wildcard bind addresses |

### Retrieval and models

| Variable | Purpose | Default |
| --- | --- | --- |
| `SIMPLE_MEMORY_EMBEDDING_MODEL` | Embedding model | `Qwen/Qwen3-Embedding-0.6B` |
| `SIMPLE_MEMORY_EMBEDDING_REVISION` | Embedding model revision | Built-in pinned revision |
| `SIMPLE_MEMORY_RERANKER_MODEL` | Reranking model | `Qwen/Qwen3-Reranker-0.6B` |
| `SIMPLE_MEMORY_RERANKER_REVISION` | Reranking model revision | Built-in pinned revision |
| `SIMPLE_MEMORY_EMBEDDING_DIMENSION` | Stored vector dimensions | `1024` |
| `SIMPLE_MEMORY_QUERY_INSTRUCTION` | Embedding retrieval instruction | Built-in generic instruction |
| `SIMPLE_MEMORY_RERANK_INSTRUCTION` | Reranking instruction | Built-in generic instruction |
| `SIMPLE_MEMORY_EMBED_BATCH_SIZE` | Embedding batch size | `8` |
| `SIMPLE_MEMORY_RERANK_BATCH_SIZE` | Reranking batch size | `4` |
| `SIMPLE_MEMORY_LEXICAL_CANDIDATES` | Lexical candidates considered | `100` |
| `SIMPLE_MEMORY_SEMANTIC_CANDIDATES` | Semantic candidates considered | `100` |
| `SIMPLE_MEMORY_RERANK_CANDIDATES` | Candidates sent to the reranker | `30` |

### Setup and Python

| Variable | Purpose | Default |
| --- | --- | --- |
| `SIMPLE_MEMORY_TORCH_BACKEND` | PyTorch backend selected during setup or update | Automatically detected |
| `SIMPLE_MEMORY_UV` | Path to a specific `uv` executable | Automatically located |
| `SIMPLE_MEMORY_PYTHON` | Path to the Python executable used by the server | Bundled virtual environment |
| `SIMPLE_MEMORY_PYTHON_PROJECT` | Path to the model-runtime project | Repository `python` directory |

Standard Hugging Face variables such as `HF_HOME` can also be used to relocate the shared model cache.

## Available tools

| Tool | Purpose |
| --- | --- |
| `space_create` | Create an isolated memory space. |
| `space_list` | List memory spaces. |
| `memory_create` | Store a new memory. |
| `memory_revise` | Add a new immutable revision. |
| `memory_get` | Read a current or historical memory. |
| `memory_history` | Read revision history. |
| `memory_list` | List active memory summaries by default, with filters and pagination. |
| `memory_search` | Search by exact text, meaning, metadata, provenance, state, or time. |
| `memory_archive` | Reversibly remove a memory from normal recall while preserving it. |
| `memory_restore` | Return an archived memory to normal recall. |
| `memory_delete` | Permanently erase a memory and all related data. |
| `memory_link` | Idempotently create a relationship between memories. |
| `memory_unlink` | Remove a relationship. |
| `memory_traverse` | Explore connected memories with paths, filters, ranking, and pagination. |
| `memory_feedback` | Record usefulness, correctness, verification, or staleness feedback. |
| `memory_status` | Inspect storage, indexing, and model health. |

Agents can also read complete memories and revision histories through MCP resources.

## License

MIT
