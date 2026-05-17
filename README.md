# C3PO & R2D2 — Dual-Agent Enterprise AI Platform

> A production-grade, dual-bot conversational AI platform for pharma enterprise — built with **pure Python and Chainlit** (no LangGraph, no LangChain agents). Custom state management, custom tool registry, custom agent loop.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![Chainlit](https://img.shields.io/badge/Chainlit-UI-orange)
![Claude](https://img.shields.io/badge/LLM-Claude%203.5%20Sonnet-purple)
![OpenSearch](https://img.shields.io/badge/Amazon-OpenSearch-yellow?logo=amazon)
![Databricks](https://img.shields.io/badge/Databricks-SQL%20Warehouse-red)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue?logo=docker)
![AWS](https://img.shields.io/badge/AWS-EKS%20%7C%20ECR-orange?logo=amazon-aws)

---

## What This Is

This is the **MVP (v1) of a dual-agent AI chatbot platform** deployed for pharma enterprise clients. Two AI agents live in one Chainlit app — users switch between them via a profile selector in the UI:

| Bot | What it does |
|-----|-------------|
| 🤖 **C3PO** — Structured Data Agent | Natural language → SQL → Databricks SQL Warehouse → charts + executive insights + downloadable PowerPoint decks |
| 🔍 **R2D2** — Semi-Structured RAG Agent | Hybrid BM25 + k-NN semantic search on Amazon OpenSearch → retrieves HCP opinions, market research, qualitative data |

**The key architectural decision:** All multi-agent orchestration — intent classification, tool routing, multi-turn state management, agentic loop — is implemented in **pure Python without LangGraph or LangChain agents**. Every state transition is explicit and traceable. This was the proof-of-concept that validated the architecture before moving to LangGraph in v2.

---

## Architecture

```
User (Chainlit UI)
        │
        ▼
┌───────────────────────────────────────┐
│         chainlit_bot_main.py          │
│  Password auth · Bot switching        │
│  Session state · Message routing      │
│  Async agent loop (MAX_ITER = 15)     │
└──────────┬───────────────────────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
  C3PO          R2D2
(Structured)  (Semi-Structured)
     │            │
     └─────┬──────┘
           ▼
┌─────────────────────────────────────┐
│           ToolCalls.py              │
│   BaseTool interface + registry     │
│                                     │
│  C3PO tools:                        │
│   • GenerateSQLCode                 │
│   • PythonAfterSQL                  │
│   • CreatePresentationDeck          │
│   • CalculateDateRanges             │
│   • CalculateNSPCaptureRatio        │
│                                     │
│  R2D2 tools:                        │
│   • RunHybridSearch (k-NN)   │
│   • RunSqlOnOpensearch              │
│   • TextProcessor                   │
│                                     │
│  Shared:                            │
│   • InsightsResponse                │
└──────┬──────────────┬───────────────┘
       ▼              ▼
  Databricks     Amazon OpenSearch
  SQL Warehouse  (Hybrid search index)
```

---

## Project Structure

```
C3PO_MVP/
├── c3po/
│   ├── chainlit_bot_main.py        # Entry point: auth, bot switching, agent loop
│   ├── ToolCalls.py                # All tool classes + ToolCallsExecutor registry
│   ├── sql_warehouse_handling.py   # Databricks SQL Warehouse connector
│   ├── CopyFolder.py               # Config/input file management
│   ├── Dockerfile                  # Python 3.11, port 80, production-ready
│   ├── requirements.txt
│   │
│   ├── Structured_Bot/             # C3PO modules
│   │   ├── agent_tools.py          # SalesPerformanceMetrics, DeckCreator
│   │   ├── llm_requests_new.py     # LLM wrapper (Claude 3.5 via Databricks)
│   │   └── helper.py               # FileNameExtractor utilities
│   │
│   ├── Semi_Structured_Bot/        # R2D2 modules
│   │   └── opensearch_execution.py # BM25 + k-NN hybrid search on OpenSearch
│   │
│   ├── Gilead/                     # Client-specific modules
│   │   ├── chatbot_handler.py      # Chat start / settings
│   │   ├── chatbot_manager.py      # Bot switching logic
│   │   ├── sending_req_to_llm.py   # Async LLM execution + insights module
│   │   └── relevant_source_instructions.py
│   │
│   └── Files_Images_Handling/      # Chart/Excel/PPTX delivery to UI
```

---

## How the Agent Loop Works (Pure Python)

No LangGraph. The agent loop is implemented with a `while` loop and explicit state in `cl.user_session`:

```python
MAX_ITER = 15

# State lives in Chainlit's session store
message_history = cl.user_session.get("message_history")
chatbot_name    = cl.user_session.get("chatbot")        # "C3PO" or "R2D2"
tool_params     = cl.user_session.get("tool_params")

# Agent loop
while iter_count < MAX_ITER:
    response = await llm.call(message_history)
    
    if response.has_tool_call:
        tool = executor.get_tool(response.tool_name)  # factory pattern
        result = await tool.run(tool_params)
        message_history.append(tool_result_message)
        iter_count += 1
    else:
        # LLM gave final answer — stream to UI and break
        await stream_to_chainlit(response)
        break
```

---

## Tool Registry (Factory Pattern)

Tools are registered as lambdas — clean, independently testable, zero global state:

```python
class ToolCallsExecutor:
    def __init__(self):
        self.opensearch = OpensearchExecutionManager()
        self.tool_map = {
            "run_hybrid_search":        lambda: RunHybridSearch(self.opensearch),
            "run_sql_on_opensearch":     lambda: RunSqlOnOpensearch(self.opensearch),
            "generate_sql_code":         lambda: GenerateSQLCode(),
            "python_after_sql":          lambda: PythonAfterSQL(),
            "create_presentation_deck":  lambda: CreatePresentationDeck(),
            "calculate_date_ranges":     lambda: CalculateDateRange(),
            "calculate_nsp_capture_ratio": lambda: CalculateNSPCaptureRatio(),
            "text_processor":            lambda: TextProcessor(),
            "insights_response":         lambda: InsightsResponse(),
        }

    def get_tool(self, function_name):
        if function_name in self.tool_map:
            return self.tool_map[function_name]()
        raise ValueError(f"Unknown function: {function_name}")
```

Each tool inherits from `BaseTool` with an async `run()` method — the interface is consistent regardless of which tool is called.

---

## Key Features

### C3PO — Structured Data Agent
- **NL → SQL → Results:** LLM generates Databricks-dialect SQL from plain English; executes via JDBC connector with auto token refresh
- **Python Code Execution:** Runs generated Matplotlib/Plotly code in a managed REPL; sends charts inline to Chainlit UI
- **PowerPoint Deck Auto-Refresh:** `CreatePresentationDeck` runs SQL queries and updates charts in a `.pptx` template using `python-pptx`
- **Date Intelligence:** `CalculateDateRanges` computes R4W, R12M, R13W, QTD ranges — no manual date input needed
- **Multi-turn Memory:** Full conversation history passed to LLM; revised query generated to preserve context across turns

### R2D2 — Semi-Structured RAG Agent
- **Hybrid Search:** BM25 (keyword) + k-NN (semantic vector) on Amazon OpenSearch — catches exact drug name matches AND semantic intent
- **Multi-Index Routing:** Routes to different OpenSearch indices based on chat profile (PMR, Market Map, Early Experience, PBC Market Research)
- **SQL on OpenSearch:** Structured queries against unstructured data via `RunSqlOnOpensearch`
- **TextProcessor:** LLM synthesises retrieved chunks into coherent insight response with source attribution

### Platform Features
- **Password Auth:** `@cl.password_auth_callback` — role-based auth without OAuth complexity
- **Bot Switching:** Profile selector in Chainlit UI cleanly resets state on switch
- **File Delivery:** Charts (PNG), Excel, and PPTX files delivered as inline Chainlit elements then deleted from disk
- **Clickable Follow-ups:** `ClickableQuestionHandler` — LLM-suggested follow-up questions as clickable buttons
- **Fully Async:** All tool execution, LLM calls, and file operations are `async/await`

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| UI | Chainlit (async Python chat) |
| LLM | Claude 3.5 Sonnet via Databricks Mosaic AI endpoint |
| Structured Data | Databricks SQL Warehouse (JDBC) |
| Vector Search | Amazon OpenSearch (BM25 + k-NN) |
| State Management | Pure Python + Chainlit `user_session` |
| Tool Orchestration | Custom `ToolCallsExecutor` (factory pattern) |
| File Generation | Matplotlib, python-pptx, openpyxl, Pandas |
| Container | Docker (Python 3.11-bookworm, port 80) |
| Deployment | AWS EKS via Kubernetes + Helm + CI/CD |
| Auth | Chainlit password auth callback |

---

## Running Locally

```bash
# Clone
git clone https://github.com/ShashiKumarVemula/C3PO_MVP.git
cd C3PO_MVP/c3po

# Virtual env
python -m venv venv && source venv/bin/activate

# Install
pip install -r requirements.txt

# Set env vars
cp .env.example .env   # fill in your credentials

# Run
chainlit run chainlit_bot_main.py --host 0.0.0.0 --port 8000
```

### Docker

```bash
docker build -t c3po-mvp .
docker run -p 80:80 --env-file .env c3po-mvp
```

### Required Environment Variables

```env
DATABRICKS_TOKEN=your_pat_token
DATABRICKS_HOST=dbc-xxxxxxxx.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_id
DATABRICKS_ENDPOINT_URL=https://your-mosaic-endpoint/serving-endpoints/claude/invocations
OPENSEARCH_HOST=your-opensearch-domain
OPENSEARCH_USERNAME=your_user
OPENSEARCH_PASSWORD=your_pass
BOT_TYPE_PMR=PMR
BOT_TYPE_MARKET_MAP=MarketMap
BOT_TYPE_EARLY_EXP=EarlyExperience
```

---

## What Changed in v2 (Production)

| Aspect | This MVP (v1) | Production v2 |
|--------|--------------|---------------|
| Agent orchestration | Pure Python while loop | LangGraph StateGraph |
| Tool routing | Factory + if/else | LangGraph conditional edges |
| State persistence | Chainlit session dict | LangGraph MemorySaver checkpoints |
| Observability | Print statements | Dynatrace APM + structured logging |
| Deployment | Docker + EKS | Helm charts + Bitbucket CI/CD |

The MVP validated: the product concept, the tool interface design (BaseTool pattern carried forward), and the dual-bot architecture. Everything that worked here was built upon in v2.

---

## Author

**Shashi Kumar Vemula** — AI/ML Engineer  
[LinkedIn](https://linkedin.com/in/YOUR_LINKEDIN) · [GitHub](https://github.com/ShashiKumarVemula)

*Built at Setuserv Informatics Pvt. Ltd. for pharma enterprise clients.*
