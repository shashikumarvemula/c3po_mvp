
# C3PO & R2D2: Dual-Agent AI Chatbot Platform (MVP v1)

![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![Framework](https://img.shields.io/badge/framework-Chainlit-red.svg)
![LLM](https://img.shields.io/badge/llm-Claude%203.5%20Sonnet-orange.svg)
![Deployment](https://img.shields.io/badge/deployment-AWS%20EKS%20%2F%20Docker-blue.svg)

This repository contains the Minimum Viable Product (MVP v1) of a dual-agent AI platform deployed for enterprise pharmaceutical clients. The application houses two distinct, specialized AI agents inside a single unified Chainlit interface, allowing users to seamlessly switch context depending on their analytical needs.

> 💡 **Key Architectural Choice:** This MVP implements the complete multi-agent orchestration loop—intent classification, tool routing, state management, and multi-turn memory—**entirely in pure Python** without relying on LangGraph or LangChain agents. Every state transition is explicit, and every tool call is completely traceable.

---

## 🏗️ System Architecture

```text
User (Chainlit UI)
        │
        ▼
┌───────────────────────────────────────────┐
│           chainlit_bot_main.py            │
│   - Password auth & session management    │
│   - Bot switching (C3PO ↔ R2D2)           │
│   - Message routing & history tracking    │
│   - Async message loop (MAX_ITER=15)      │
└────────────┬──────────────────────────────┘
             │
    ┌────────┴────────┐
    ▼                 ▼
┌─────────┐     ┌──────────────┐
│  C3PO   │     │     R2D2     │
│Structured│     │Semi-Structured│
│   Bot   │     │     Bot      │
└────┬────┘     └──────┬───────┘
     │                 │
     ▼                 ▼
┌──────────────────────────────┐
│         ToolCalls.py         │
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
┌─────────┐   ┌──────────────┐
│Databricks│   │Amazon        │
│SQL       │   │OpenSearch    │
│Warehouse │   │(Hybrid Search│
│          │   │BM25 + k-NN)  │
└─────────┘   └──────────────┘
🤖 The Agents
Agent	Core System	Capabilities & Workflow
🤖 C3PO	Structured Data Agent	Converts natural language questions into executable SQL → queries Databricks SQL Warehouse → returns analytical charts, data insights, and downloadable assets.
🔍 R2D2	Semi-Structured RAG Agent	Runs hybrid lexical/semantic search on Amazon OpenSearch to retrieve qualitative data such as HCP opinions, market research, and qualitative intelligence.
🛠️ Key Technical Decisions (Why No LangGraph?)
This MVP was built to validate core capabilities before moving to framework-driven orchestration in production v2. It demonstrates how complex multi-agent state machines can be robustly implemented in pure Python:

1. Explicit State Management
Instead of an abstraction like StateGraph, we utilize Chainlit's native session store (cl.user_session) as our source of truth. State is maintained explicitly per session:

Python
# State is tracked and modified manually per conversation session
message_history = cl.user_session.get("message_history")   # Full history context
chatbot_name    = cl.user_session.get("chatbot")           # Currently active agent
tool_params     = cl.user_session.get("tool_params")       # Current tool runtime state
2. Custom Factory Tool Registry
Instead of relying on magic wrappers or auto-decorators, tools are registered cleanly via a factory pattern map within a dedicated ToolCallsExecutor:

Python
self.tool_map = {
    "run_hybrid_search":        lambda: RunHybridSearch(self.opensearch),
    "run_sql_on_opensearch":    lambda: RunSqlOnOpensearch(self.opensearch),
    "generate_sql_code":        lambda: GenerateSQLCode(),
    "python_after_sql":         lambda: PythonAfterSQL(),
    "create_presentation_deck": lambda: CreatePresentationDeck(),
    "calculate_date_ranges":    lambda: CalculateDateRange(),
}
Each tool is implemented as an isolated class inheriting from a base contract, enforcing cleaner testability and independent deployment.

3. Multi-Turn Autonomous Agent Loop
The execution loop runs safely up to MAX_ITER=15. The LLM dynamically evaluates context at each turn to decide whether to continue calling tools or output a final answer:

Plaintext
User Query ──> LLM ──> Tool Call? ──> Execute Tool ──> Feed Back to LLM ──┐
                        ▲                                                 │
                        └─────────────────────────────────────────────────┘
                        ──> Final Answer? ──> Stream to UI ──> [Done]
🌟 Core Features
📊 C3PO — Structured Data Agent
Text-to-SQL Engine: Generates highly targeted Databricks SQL text directly from natural, conversational English queries.

Live Warehouse Execution: Communicates directly with Databricks SQL Warehouses utilizing robust connection pooling and automatic token refreshes.

Dynamic Chart Generation: Safely executes programmatic python plotting (Matplotlib/Plotly) on top of SQL returns to render native charts directly inline inside the chat window.

PowerPoint Generation: Features a CreatePresentationDeck tool that automatically updates templates and exports customized slide charts using python-pptx.

Temporal Intelligence: A specialized CalculateDateRanges module programmatically infers dynamic pharma timelines (like R4W, R12M, R13W, QTD) using the latest weekend processing date—removing any manual date selection requirements.

🔍 R2D2 — Semi-Structured RAG Agent
Hybrid Vector Search: Blends lexical BM25 matching and semantic k-NN vector search inside Amazon OpenSearch to optimize target match accuracy.

Multi-Index Routing: Automatically targets specific OpenSearch indices (e.g., PMR, Market Map, Early Experience, PBC Market Research) based on the user's active sub-profile.

OpenSearch SQL Retrieval: Executes structured SQL expressions against document stores for rapid, complex filtering of raw metadata.

Synthesis & Citation: Features a dedicated TextProcessor that synthesizes retrieved data blocks into localized summaries complete with source-document attributions.

🧱 Tools Directory
Tool Code Name	Bot	Purpose / Functional Description
generate_sql_code	C3PO	LLM translates incoming user query into Databricks SQL syntax.
python_after_sql	C3PO	Spawns python runtime tasks on tabular outputs to handle calculations and data visualization.
create_presentation_deck	C3PO	Rewrites and updates slide files using fresh query results.
calculate_date_ranges	C3PO	Dynamically calculates pharma-specific standard date windows (QTD, R12M, etc.).
run_hybrid_search	R2D2	Combines sparse and dense vectors for advanced OpenSearch queries.
run_sql_on_opensearch	R2D2	Queries OpenSearch indexes using SQL semantics rather than DSL.
text_processor	R2D2	Aggregates and synthesizes disparate context passages into comprehensive insights.
insights_response	Both	Formats structural analytical breakdowns post-processing loop.
calculate_nsp_capture_ratio	C3PO	Industry-specific calculation engine for Net Selling Price capture rates.
💻 Tech Stack
User Interface Framework: Chainlit (Async Python)

Large Language Model Layer: Claude 3.5 Sonnet (Via Databricks Mosaic AI Model Serving Endpoint)

Structured Storage Environment: Databricks SQL Warehouse (via high-performance JDBC connectivity)

Vector Search & Document Database: Amazon OpenSearch (k-NN + BM25 Search Engine)

Orchestration Engine: Pure Python Event Architecture + Factory Pattern Executors

Document Generation Suite: Matplotlib, Plotly, python-pptx, openpyxl, Pandas

Container Infrastructure & Scaling: Docker (Python 3.11-bookworm base) deployed to AWS EKS via Helm charts

📂 Project Structure
Plaintext
C3PO_MVP/
├── c3po/
│   ├── chainlit_bot_main.py          # Application core — entry point, auth loop, profile routing
│   ├── ToolCalls.py                  # Core Tool definitions and the ToolCallsExecutor registry
│   ├── sql_warehouse_handling.py     # Connectors & data streams for Databricks Warehouse
│   ├── CopyFolder.py                 # File handling utility for configuration payloads
│   ├── Dockerfile                    # Optimized multi-stage production Docker definition
│   ├── requirements.txt              # Application-wide python dependencies
│   │
│   ├── Structured_Bot/               # C3PO Specific Intelligence Module
│   │   ├── agent_tools.py            # SalesPerformanceMetrics execution, PowerPoint utilities
│   │   ├── llm_requests_new.py       # Databricks Claude 3.5 API management and schema prompts
│   │   └── helper.py                 # Core text parse systems and file clean up modules
│   │
│   ├── Semi_Structured_Bot/          # R2D2 Specific Intelligence Module
│   │   └── opensearch_execution.py   # Hybrid query logic and OpenSearch communication layers
│   │
│   ├── Gilead/                       # Pharmaceutical Domain Configuration
│   │   ├── chatbot_handler.py        # Configures interface settings upon user login
│   │   ├── chatbot_manager.py        # Directs agent routing tables based on active selections
│   │   └── sending_req_to_llm.py     # Executes asynchronous network calls out to model endpoints
│   │
│   └── Files_Images_Handling/        # User Interface Media Pipelines
│       └── ElementsHandling.py       # Assembles inline graphics, sheets, and slide presentations
🚀 How to Run Locally
Prerequisites
Python 3.11 Installed locally

Access to an active Databricks Workspace (with a configured SQL Warehouse)

A running Amazon OpenSearch Domain

Claude 3.5 Sonnet access provisioned via Databricks Mosaic AI Gateway

Installation Steps
Clone the project code:

Bash
git clone [https://github.com/ShashiKumarVemula/C3PO_MVP.git](https://github.com/ShashiKumarVemula/C3PO_MVP.git)
cd C3PO_MVP/c3po


2. **Establish a clean virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scripts\activate
   
Install application dependencies:

Bash
pip install -r requirements.txt


4. **Environment Variables Configuration:**
   ```bash
   cp .env.example .env
   # Open .env and fill in your target cluster credentials securely
Launch the local web server:

Bash
chainlit run chainlit_bot_main.py --host 0.0.0.0 --port 8000


### Running with Docker

*   **Build the Image locally:**
    ```bash
    docker build -t c3po-mvp .
    ```
*   **Run the Container:**
    ```bash
    docker run -p 80:80 --env-file .env c3po-mvp
    
🔒 Environment Variables Reference
Ensure your .env file contains these specific keys before booting up the application:

Code snippet
# Databricks Connectivity Parameters
DATABRICKS_TOKEN=your_secret_token_here
DATABRICKS_HOST=dbc-xxxxxxxx.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
DATABRICKS_ENDPOINT_URL=https://your-mosaic-endpoint/serving-endpoints/claude/invocations

# Amazon OpenSearch Vector Credentials
OPENSEARCH_HOST=your-opensearch-domain-endpoint
OPENSEARCH_USERNAME=your_username
OPENSEARCH_PASSWORD=your_password

# Client Sub-Profile Definitions
BOT_TYPE_PMR=PMR
BOT_TYPE_MARKET_MAP=MarketMap
BOT_TYPE_EARLY_EXP=EarlyExperience
BOT_TYPE_PBC_MARKET_RESEARCH=PBCMarketResearch
📈 Evolutionary Retrospective (v1 MVP vs. v2 Production)
Building this application without an orchestration framework provided foundational design patterns that directly scaled into our production release:

Architecture Design Dimension	MVP v1 (This Repository)	Production v2 Engine
Agent Orchestration	Pure Python Engine + cl.user_session loops	LangGraph StateGraph Ecosystem
Tool Execution Routing	Conditional Control Blocks (if/else Factory Map)	LangGraph Conditional Routing Edges
State Persistence	Transient Chainlit Context Sessions	LangGraph MemorySaver + Checkpointers
Multi-Agent Interacting	UI-driven active Profile Swapping	Autonomous Graph with Dedicated Execution Nodes
Observability & Analytics	Terminal Log Stream Expressions	Dynatrace APM Monitoring + Structured JSON Logs
🧑‍💻 Author
Shashi Kumar Vemula – AI/ML Engineer

Engineered at Setuserv Informatics Pvt. Ltd. – Custom Intelligent Agents for Enterprise Pharmaceutical Analytics.
