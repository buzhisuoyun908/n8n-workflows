#!/usr/bin/env python3
"""
FastAPI Server for N8N Workflow Documentation
High-performance API with sub-100ms response times and auto-reloading.
"""

# --- 1. Original and New Imports ---
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, field_validator
from typing import Optional, List, Dict, Any
import json
import os
import asyncio
from pathlib import Path
import uvicorn

# Imports for Watchdog and Lifespan functionality
import time
import threading
from contextlib import asynccontextmanager
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from workflow_db import WorkflowDatabase


# --- 2. Lifespan Manager and Watchdog Logic (New Section) ---

def run_full_indexing():
    """Helper function to run a full database re-indexing."""
    print("🚀 [Auto-Index] Starting workflow indexing...")
    # force_reindex=True ensures we always scan from the filesystem state
    stats = db.index_all_workflows(force_reindex=True)
    print(f"✅ [Auto-Index] Indexing complete. Processed {stats['processed']} workflows.")

class WorkflowEventHandler(FileSystemEventHandler):
    """Filesystem event handler with debouncing to prevent rapid re-indexing."""
    def __init__(self, callback, debounce_seconds=5):
        self.callback = callback
        self.debounce_seconds = debounce_seconds
        self.last_triggered_time = 0
        self.timer = None

    def _trigger_callback(self):
        """The actual trigger logic, wrapped to check debounce time."""
        current_time = time.time()
        if current_time - self.last_triggered_time > self.debounce_seconds:
            print("🔍 Filesystem change detected, triggering re-index...")
            self.last_triggered_time = current_time
            # Run indexing in a new thread to avoid blocking the main server
            threading.Thread(target=self.callback).start()

    def on_any_event(self, event):
        """Catches all events (created, modified, deleted) for .json files."""
        if event.src_path.endswith('.json'):
            if self.timer:
                self.timer.cancel()
            # Wait 1 second before triggering to bundle multiple rapid events (like file saves)
            self.timer = threading.Timer(1.0, self._trigger_callback)
            self.timer.start()

# Create a global observer instance
observer = Observer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the application's startup and shutdown events.
    Replaces the old @app.on_event("startup") decorator.
    """
    # === On Application Startup ===
    print("🚀 Application starting up...")
    
    # 1. Run an initial index to ensure the database is populated.
    run_full_indexing()
    
    # 2. Configure and start the file watcher.
    # This path is relative to the container's WORKDIR, which is /app
    path_to_watch = "workflows" 
    if not os.path.exists(path_to_watch):
        print(f"⚠️ Watch directory '{path_to_watch}' not found. Creating it.")
        os.makedirs(path_to_watch, exist_ok=True)
        
    event_handler = WorkflowEventHandler(callback=run_full_indexing)
    observer.schedule(event_handler, path_to_watch, recursive=True)
    observer.start()
    print(f"👀 Watching for file changes in: {os.path.abspath(path_to_watch)}")
    
    yield  # The application runs after this point
    
    # === On Application Shutdown ===
    print("🛑 Application shutting down...")
    db.close() # Gracefully close the shared database connection.
    if observer.is_alive():
        observer.stop()
        observer.join() # Wait for the watcher thread to terminate gracefully
    print("👋 Watcher stopped.")


# --- 3. FastAPI App Initialization (Modified) ---
app = FastAPI(
    title="N8N Workflow Documentation API",
    description="Fast API for Browse and searching workflow documentation with auto-sync",
    version="2.1.0", # Version bumped to reflect new feature
    lifespan=lifespan # Use the new lifespan manager
)

# --- 4. Middleware and DB Initialization (Unchanged) ---
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = WorkflowDatabase()

# NOTE: The old `@app.on_event("startup")` has been removed and its logic
# has been integrated into the `lifespan` manager above.

# --- 5. All Original Pydantic Models and API Endpoints (Unchanged) ---

# Response models
class WorkflowSummary(BaseModel):
    id: Optional[int] = None
    filename: str
    name: str
    active: bool
    description: str = ""
    trigger_type: str = "Manual"
    complexity: str = "low"
    node_count: int = 0
    integrations: List[str] = []
    tags: List[str] = []
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    
    class Config:
        validate_assignment = True
        
    @field_validator('active', mode='before')
    @classmethod
    def convert_active(cls, v):
        if isinstance(v, int):
            return bool(v)
        return v

class SearchResponse(BaseModel):
    workflows: List[WorkflowSummary]
    total: int
    page: int
    per_page: int
    pages: int
    query: str
    filters: Dict[str, Any]

class StatsResponse(BaseModel):
    total: int
    active: int
    inactive: int
    triggers: Dict[str, int]
    complexity: Dict[str, int]
    total_nodes: int
    unique_integrations: int
    last_indexed: str

@app.get("/")
async def root():
    """Serve the main documentation page."""
    static_dir = Path("static")
    index_file = static_dir / "index.html"
    if not index_file.exists():
        return HTMLResponse("""
        <html><body>
        <h1>Setup Required</h1>
        <p>Static files not found. Please ensure the static directory exists with index.html</p>
        <p>Current directory: """ + str(Path.cwd()) + """</p>
        </body></html>
        """)
    return FileResponse(str(index_file))

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "message": "N8N Workflow API is running"}

@app.get("/api/stats", response_model=StatsResponse)
async def get_stats():
    """Get workflow database statistics."""
    try:
        stats = db.get_stats()
        return StatsResponse(**stats)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching stats: {str(e)}")

@app.get("/api/workflows", response_model=SearchResponse)
async def search_workflows(
    q: str = Query("", description="Search query"),
    trigger: str = Query("all", description="Filter by trigger type"),
    complexity: str = Query("all", description="Filter by complexity"),
    active_only: bool = Query(False, description="Show only active workflows"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page")
):
    """Search and filter workflows with pagination."""
    try:
        offset = (page - 1) * per_page
        
        workflows, total = db.search_workflows(
            query=q,
            trigger_filter=trigger,
            complexity_filter=complexity,
            active_only=active_only,
            limit=per_page,
            offset=offset
        )
        
        workflow_summaries = []
        for workflow in workflows:
            try:
                # This cleanup is good practice, keeping it.
                clean_workflow = {
                    'id': workflow.get('id'),
                    'filename': workflow.get('filename', ''),
                    'name': workflow.get('name', ''),
                    'active': workflow.get('active', False),
                    'description': workflow.get('description', ''),
                    'trigger_type': workflow.get('trigger_type', 'Manual'),
                    'complexity': workflow.get('complexity', 'low'),
                    'node_count': workflow.get('node_count', 0),
                    'integrations': workflow.get('integrations', []),
                    'tags': workflow.get('tags', []),
                    'created_at': workflow.get('created_at'),
                    'updated_at': workflow.get('updated_at')
                }
                workflow_summaries.append(WorkflowSummary(**clean_workflow))
            except Exception as e:
                print(f"Error converting workflow {workflow.get('filename', 'unknown')}: {e}")
                continue
        
        pages = (total + per_page - 1) // per_page
        
        return SearchResponse(
            workflows=workflow_summaries,
            total=total,
            page=page,
            per_page=per_page,
            pages=pages,
            query=q,
            filters={
                "trigger": trigger,
                "complexity": complexity,
                "active_only": active_only
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching workflows: {str(e)}")

@app.get("/api/workflows/{filename}")
async def get_workflow_detail(filename: str):
    """Get detailed workflow information including raw JSON."""
    try:
        workflows, _ = db.search_workflows(f'filename:"{filename}"', limit=1)
        if not workflows:
            raise HTTPException(status_code=404, detail="Workflow not found in database")
        
        workflow_meta = workflows[0]
        
        file_path = os.path.join("workflows", filename)
        if not os.path.exists(file_path):
            print(f"Warning: File {file_path} not found on filesystem but exists in database")
            raise HTTPException(status_code=404, detail=f"Workflow file '{filename}' not found on filesystem")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_json = json.load(f)
        
        return {
            "metadata": workflow_meta,
            "raw_json": raw_json
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading workflow: {str(e)}")

@app.get("/api/workflows/{filename}/download")
async def download_workflow(filename: str):
    """Download workflow JSON file."""
    try:
        file_path = os.path.join("workflows", filename)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"Workflow file '{filename}' not found on filesystem")
        
        return FileResponse(
            file_path,
            media_type="application/json",
            filename=filename
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Workflow file '{filename}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error downloading workflow: {str(e)}")

@app.get("/api/workflows/{filename}/diagram")
async def get_workflow_diagram(filename: str):
    """Get Mermaid diagram code for workflow visualization."""
    try:
        file_path = os.path.join("workflows", filename)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"Workflow file '{filename}' not found on filesystem")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        nodes = data.get('nodes', [])
        connections = data.get('connections', {})
        
        diagram = generate_mermaid_diagram(nodes, connections)
        
        return {"diagram": diagram}
    except HTTPException:
        raise
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Workflow file '{filename}' not found")
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in workflow file: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating diagram: {str(e)}")

def generate_mermaid_diagram(nodes: List[Dict], connections: Dict) -> str:
    """Generate Mermaid.js flowchart code from workflow nodes and connections."""
    if not nodes:
        return "graph TD\n  EmptyWorkflow[No nodes found in workflow]"
    
    mermaid_ids = {}
    for i, node in enumerate(nodes):
        node_id = f"node{i}"
        # Use a unique identifier for the key, like the node name + index
        node_key = node.get('name', f'Node {i}') + str(i)
        mermaid_ids[node_key] = node_id
    
    mermaid_code = ["graph TD"]
    
    node_name_to_key = {node.get('name'): node.get('name', f'Node {i}') + str(i) for i, node in enumerate(nodes)}

    for i, node in enumerate(nodes):
        node_name = node.get('name', 'Unnamed')
        node_key = node.get('name', f'Node {i}') + str(i)
        node_id = mermaid_ids[node_key]
        node_type = node.get('type', '').replace('n8n-nodes-base.', '')
        
        style = "fill:#d9d9d9,stroke:#666666" # Default
        if any(x in node_type.lower() for x in ['trigger', 'webhook', 'cron']):
            style = "fill:#b3e0ff,stroke:#0066cc"
        elif any(x in node_type.lower() for x in ['if', 'switch']):
            style = "fill:#ffffb3,stroke:#e6e600"
        elif any(x in node_type.lower() for x in ['function', 'code']):
            style = "fill:#d9b3ff,stroke:#6600cc"
        elif 'error' in node_type.lower():
            style = "fill:#ffb3b3,stroke:#cc0000"
        
        clean_name = node_name.replace('"', "'")
        clean_type = node_type.replace('"', "'")
        label = f"{clean_name}<br>({clean_type})"
        mermaid_code.append(f"  {node_id}[\"{label}\"]")
        mermaid_code.append(f"  style {node_id} {style}")
    
    for source_name, source_connections in connections.items():
        if source_name not in node_name_to_key: continue
        source_key = node_name_to_key[source_name]

        if source_key not in mermaid_ids: continue
        
        if isinstance(source_connections, dict) and 'main' in source_connections:
            for i, output_connections in enumerate(source_connections['main']):
                if not isinstance(output_connections, list): continue
                for connection in output_connections:
                    if not isinstance(connection, dict) or 'node' not in connection: continue
                    target_name = connection['node']
                    if target_name not in node_name_to_key: continue
                    target_key = node_name_to_key[target_name]
                    if target_key not in mermaid_ids: continue
                    
                    label = f" -->|{i}| " if len(source_connections['main']) > 1 else " --> "
                    mermaid_code.append(f"  {mermaid_ids[source_key]}{label}{mermaid_ids[target_key]}")
    
    return "\n".join(mermaid_code)

@app.post("/api/reindex")
async def reindex_workflows(background_tasks: BackgroundTasks, force: bool = False):
    """Trigger workflow reindexing in the background."""
    def run_indexing():
        db.index_all_workflows(force_reindex=force)
    
    background_tasks.add_task(run_indexing)
    return {"message": "Reindexing started in background"}

@app.get("/api/integrations")
async def get_integrations():
    """Get list of all unique integrations."""
    try:
        stats = db.get_stats()
        return {"integrations": [], "count": stats['unique_integrations']}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching integrations: {str(e)}")

@app.get("/api/categories")
async def get_categories():
    """Get available workflow categories for filtering."""
    try:
        categories_file = Path("context/unique_categories.json")
        if categories_file.exists():
            with open(categories_file, 'r', encoding='utf-8') as f:
                return {"categories": json.load(f)}
        else:
            search_categories_file = Path("context/search_categories.json")
            if search_categories_file.exists():
                with open(search_categories_file, 'r', encoding='utf-8') as f:
                    search_data = json.load(f)
                unique_categories = set(item.get('category', 'Uncategorized') for item in search_data)
                return {"categories": sorted(list(unique_categories))}
            else:
                return {"categories": ["Uncategorized"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching categories: {str(e)}")

@app.get("/api/category-mappings")
async def get_category_mappings():
    """Get filename to category mappings for client-side filtering."""
    try:
        search_categories_file = Path("context/search_categories.json")
        if not search_categories_file.exists():
            return {"mappings": {}}
        
        with open(search_categories_file, 'r', encoding='utf-8') as f:
            search_data = json.load(f)
        
        mappings = {item['filename']: item.get('category', 'Uncategorized') for item in search_data if 'filename' in item}
        return {"mappings": mappings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching category mappings: {str(e)}")

@app.get("/api/workflows/category/{category}", response_model=SearchResponse)
async def search_workflows_by_category(
    category: str,
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page")
):
    """Search workflows by service category (messaging, database, ai_ml, etc.)."""
    try:
        offset = (page - 1) * per_page
        workflows, total = db.search_by_category(
            category=category,
            limit=per_page,
            offset=offset
        )
        workflow_summaries = [WorkflowSummary(**w) for w in workflows]
        pages = (total + per_page - 1) // per_page
        
        return SearchResponse(
            workflows=workflow_summaries,
            total=total,
            page=page,
            per_page=per_page,
            pages=pages,
            query=f"category:{category}",
            filters={"category": category}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching by category: {str(e)}")

# Custom exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {str(exc)}"}
    )

# Mount static files
static_dir = Path("static")
if static_dir.exists():
    app.mount("/static", StaticFiles(directory="static"), name="static")
    print(f"✅ Static files mounted from {static_dir.absolute()}")
else:
    print(f"❌ Warning: Static directory not found at {static_dir.absolute()}")

# --- 6. Main execution block for local development (Simplified) ---
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='N8N Workflow Documentation API Server')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind to')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload for development')
    
    args = parser.parse_args()
    
    # Directly run uvicorn, which is cleaner for development and mirrors production.
    # The setup logic is now handled by the lifespan manager.
    uvicorn.run(
        "api_server:app",
        host=args.host,
        port=args.port,
        reload=args.reload
    )
