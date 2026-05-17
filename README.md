What This Is
This is the MVP (v1) of a dual-agent AI chatbot platform deployed for enterprise pharma clients. The platform houses two distinct AI agents in a single Chainlit application — users switch between them via a bot selector in the UI:
BotNameWhat it does🤖 C3POStructured Data AgentConverts natural language questions into SQL → queries Databricks SQL Warehouse → returns charts, insights, and downloadable reports🔍 R2D2Semi-Structured RAG AgentRuns hybrid search (BM25 + k-NN vector) on Amazon OpenSearch to retrieve HCP opinions, market research, and qualitative intelligence
Key architectural decision: This MVP implements the full multi-agent orchestration loop — intent classification, tool routing, state management, multi-turn memory — entirely in pure Python without LangGraph or LangChain agents. Every state transition is explicit, every tool call is traceable.

Architecture
User (Chainlit UI)
        │
        ▼
┌───────────────────────────────────────────┐
│           chainlit_bot_main.py            │
│   - Password auth & session management    │
│   - Bot switching (C3PO ↔ R2D2)          │
│   - Message routing & history tracking   │
│   - Async message loop (MAX_ITER=15)     │
└────────────┬──────────────────────────────┘
             │
    ┌────────┴────────┐
    ▼                 ▼
┌─────────┐    ┌──────────────┐
│  C3PO   │    │    R2D2      │
│Structured│   │Semi-Structured│
│  Bot    │    │    Bot        │
└────┬────┘    └──────┬───────┘
     │                │
     ▼                ▼
┌──────────────────────────────┐
│       ToolCalls.py           │
│  Tool Registry & Executor    │
│  ┌──────────────────────┐   │
│  │ GenerateSQLCode      │   │  ← C3PO path
│  │ PythonAfterSQL       │   │
│  │ CreatePresentationDeck│  │
│  │ CalculateDateRanges  │   │
│  ├──────────────────────┤   │
│  │ RunHybridSearch      │   │  ← R2D2 path
│  │ RunSqlOnOpensearch   │   │
│  │ TextProcessor        │   │
│  └──────────────────────┘   │
└──────────┬───────────────────┘
           │
    ┌──────┴──────┐
    ▼             ▼
┌─────────┐  ┌──────────────┐
│Databricks│  │Amazon        │
│SQL       │  │OpenSearch    │
│Warehouse │  │(Hybrid Search│
│          │  │BM25 + k-NN) │
└─────────┘  └──────────────┘

Key Technical Decisions (Why No LangGraph?)
This MVP was built before adopting LangGraph in the production v2. The architecture demonstrates that you can implement multi-agent state machines in pure Python:
Custom State Management
Instead of LangGraph's StateGraph, we use cl.user_session (Chainlit's session store) as the state container:
python# State is maintained explicitly per session
message_history = cl.user_session.get("message_history")   # full conversation
chatbot_name    = cl.user_session.get("chatbot")           # active bot
tool_params     = cl.user_session.get("tool_params")       # current tool state
Custom Tool Registry (ToolCallsExecutor)
Instead of LangChain's tool decorators, tools are registered in a factory map:
pythonself.tool_map = {
    "run_hybrid_search":        lambda: RunHybridSearch(self.opensearch),
    "run_sql_on_opensearch":    lambda: RunSqlOnOpensearch(self.opensearch),
    "generate_sql_code":        lambda: GenerateSQLCode(),
    "python_after_sql":         lambda: PythonAfterSQL(),
    "create_presentation_deck": lambda: CreatePresentationDeck(),
    "calculate_date_ranges":    lambda: CalculateDateRange(),
    ...
}
Each tool is a class inheriting from BaseTool with an async run() method — clean, testable, and independently deployable.
Multi-Turn Agent Loop
The agentic loop runs for MAX_ITER=15 iterations — the LLM decides when to stop by returning a final answer instead of a tool call:
User Query → LLM → Tool Call? → Execute Tool → Feed result back to LLM → Repeat
                 → Final Answer? → Stream to UI → Done

Project Structure
C3PO_MVP/
├── c3po/
│   ├── chainlit_bot_main.py          # Main entry point — Chainlit app, auth, routing
│   ├── ToolCalls.py                  # All tool implementations + ToolCallsExecutor
│   ├── sql_warehouse_handling.py     # Databricks SQL Warehouse connector
│   ├── CopyFolder.py                 # Config/input file management utility
│   ├── Dockerfile                    # Production Docker image (Python 3.11, port 80)
│   ├── requirements.txt              # All dependencies
│   │
│   ├── Structured_Bot/               # C3PO modules
│   │   ├── agent_tools.py            # SalesPerformanceMetrics, DeckCreator
│   │   ├── llm_requests_new.py       # LLM wrapper (Claude 3.5 via Databricks endpoint)
│   │   ├── helper.py                 # FileNameExtractor, utilities
│   │   └── ...
│   │
│   ├── Semi_Structured_Bot/          # R2D2 modules
│   │   ├── opensearch_execution.py   # Hybrid search (BM25 + k-NN) on OpenSearch
│   │   └── ...
│   │
│   ├── Gilead/                       # Client-specific modules
│   │   ├── chatbot_handler.py        # Chat start / settings handler
│   │   ├── chatbot_manager.py        # Bot switching logic
│   │   ├── llm_requests_new.py       # LLM request builder
│   │   ├── sending_req_to_llm.py     # Async LLM request execution
│   │   └── ...
│   │
│   └── Files_Images_Handling/        # Chart/Excel/PPTX file delivery to UI
│       └── ElementsHandling          # Chainlit file/image element management

Core Features
C3PO — Structured Data Agent

Natural Language → SQL: LLM generates Databricks SQL from plain English queries
Live Data Fetching: Queries Databricks SQL Warehouse with connection pooling and auto token refresh
Chart Generation: Executes Python code to generate Matplotlib/Plotly charts inline in chat
PowerPoint Deck Creation: CreatePresentationDeck tool auto-updates slide charts from SQL results using python-pptx
Date Intelligence: CalculateDateRanges computes R4W, R12M, R13W, QTD ranges from latest weekend date — no manual date entry needed
Multi-turn Memory: Full conversation history maintained per session, revised query sent to LLM to preserve context

R2D2 — Semi-Structured RAG Agent

Hybrid Search: Combines BM25 (keyword) + k-NN (semantic vector) search on Amazon OpenSearch — catches both exact keyword matches and semantic intent
Multi-Index Support: Routes to different OpenSearch indices based on bot profile (PMR, Market Map, Early Experience, PBC Market Research)
SQL on OpenSearch: Executes SQL-style queries against OpenSearch for structured retrieval from unstructured data
Text Processor: LLM synthesises retrieved chunks into a coherent insight response with source attribution

Platform-Level Features

Password Auth: @cl.password_auth_callback — role-based user authentication without OAuth overhead
Bot Switching: Users switch between C3PO and R2D2 via Chainlit's chat profile selector — state resets cleanly on switch
File Delivery: Charts (PNG), Excel files, and PowerPoint decks delivered as inline Chainlit elements then cleaned from disk
Clickable Questions: ClickableQuestionHandler — LLM suggests follow-up questions as clickable buttons in UI
Async Throughout: All tool execution, LLM calls, and file handling are async/await — no blocking operations


Tools Implemented
ToolBotDescriptiongenerate_sql_codeC3POLLM generates Databricks SQL from user querypython_after_sqlC3POExecutes Python on SQL results for charts/analysiscreate_presentation_deckC3POAuto-refreshes PowerPoint charts from live SQL datacalculate_date_rangesC3POComputes R4W/R12M/QTD date ranges automaticallyrun_hybrid_searchR2D2BM25 + k-NN hybrid search on OpenSearchrun_sql_on_opensearchR2D2SQL-style queries on OpenSearch indicestext_processorR2D2LLM synthesis of retrieved chunks into insightsinsights_responseBothPost-response insight generation modulecalculate_nsp_capture_ratioC3POPharma-specific NSP capture rate calculation

Tech Stack
LayerTechnologyUI FrameworkChainlit (async Python chat UI)LLMClaude 3.5 Sonnet via Databricks Mosaic AI endpointStructured DataDatabricks SQL Warehouse (JDBC connector)Vector SearchAmazon OpenSearch (BM25 + k-NN hybrid)State ManagementPure Python + Chainlit user sessionTool OrchestrationCustom ToolCallsExecutor (factory pattern)File GenerationMatplotlib, python-pptx, openpyxl, PandasContainerisationDocker (Python 3.11-bookworm)DeploymentAWS EKS (Kubernetes) via Helm + CI/CDAuthChainlit password auth callback

How to Run Locally
Prerequisites

Python 3.11+
Databricks workspace with SQL Warehouse
Amazon OpenSearch domain
Claude 3.5 Sonnet access via Databricks Mosaic AI Gateway

Setup
bash# 1. Clone the repo
git clone https://github.com/ShashiKumarVemula/C3PO_MVP.git
cd C3PO_MVP/c3po

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables
cp .env.example .env
# Fill in your credentials (see Environment Variables section)

# 5. Run the app
chainlit run chainlit_bot_main.py --host 0.0.0.0 --port 8000
Docker
bash# Build
docker build -t c3po-mvp .

# Run
docker run -p 80:80 --env-file .env c3po-mvp
Environment Variables
Create a .env file with the following (never commit real values):
env# Databricks
DATABRICKS_TOKEN=your_token_here
DATABRICKS_HOST=dbc-xxxxxxxx.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
DATABRICKS_ENDPOINT_URL=https://your-mosaic-endpoint/serving-endpoints/claude/invocations

# Amazon OpenSearch
OPENSEARCH_HOST=your-opensearch-domain
OPENSEARCH_USERNAME=your_username
OPENSEARCH_PASSWORD=your_password

# Bot Configuration
BOT_TYPE_PMR=PMR
BOT_TYPE_MARKET_MAP=MarketMap
BOT_TYPE_EARLY_EXP=EarlyExperience
BOT_TYPE_PBC_MARKET_RESEARCH=PBCMarketResearch

What Makes This Different
Most LLM demos call an API and print the response. This system:

Manages full multi-turn state without a framework — pure Python session management
Executes real code — generates and runs Python in a managed REPL, sends charts back to users
Generates and delivers PowerPoint decks from live SQL data — not static templates
Routes between two agents based on query type and user-selected profile
Handles production concerns — token refresh, connection pooling, file cleanup, async throughout, error recovery
Is containerised and Kubernetes-ready — Dockerfile + Helm-deployable to AWS EKS


What I Learned / What v2 Changed
This MVP was a proof of concept that directly informed the production v2:
DecisionMVP (This repo)Production v2Agent orchestrationPure Python + custom stateLangGraph StateGraphTool routingCustom factory + if/elseLangGraph conditional edgesState persistenceChainlit sessionLangGraph MemorySaver + checkpointingMulti-agentBot switching via UIAgent graph with dedicated nodesObservabilityPrint statementsDynatrace APM + structured logging
The MVP proved the product concept and the tool architecture — the BaseTool interface and ToolCallsExecutor registry pattern carried forward directly into v2.

Author
Shashi Kumar Vemula — AI/ML Engineer
Built at Setuserv Informatics Pvt. Ltd. — pharma enterprise AI 
