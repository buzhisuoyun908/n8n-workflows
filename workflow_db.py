#!/usr/bin/env python3
"""
Fast N8N Workflow Database
SQLite-based workflow indexer and search engine for instant performance.
"""

import sqlite3
import json
import os
import glob
import datetime
import hashlib
from typing import Dict, List, Any, Optional, Tuple, Set
from pathlib import Path

class WorkflowDatabase:
    """High-performance SQLite database for workflow metadata and search."""
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.environ.get('WORKFLOW_DB_PATH', 'database/workflows.db')
        
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.workflows_dir = "workflows"
        
        # --- MODIFIED: Establish a single, persistent connection ---
        self.conn = self._create_connection()
        self.init_database()
    
    def _create_connection(self) -> sqlite3.Connection:
        """Creates and configures a single SQLite connection."""
        # check_same_thread=False is crucial for our multi-threaded environment
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn

    # --- NEW: Method to gracefully close the connection ---
    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("INFO:     Database connection closed.")

    def init_database(self):
        """Initialize SQLite database with optimized schema and indexes."""
        # Uses the persistent connection self.conn
        with self.conn:
            # Create main workflows table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS workflows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    workflow_id TEXT,
                    active BOOLEAN DEFAULT 0,
                    description TEXT,
                    trigger_type TEXT,
                    complexity TEXT,
                    node_count INTEGER DEFAULT 0,
                    integrations TEXT,  -- JSON array
                    tags TEXT,          -- JSON array
                    created_at TEXT,
                    updated_at TEXT,
                    file_hash TEXT,
                    file_size INTEGER,
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create FTS5 table for full-text search
            self.conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS workflows_fts USING fts5(
                    filename, name, description, integrations, tags,
                    content=workflows, content_rowid=id,
                    tokenize = "porter unicode61"
                )
            """)
            
            # Create indexes for fast filtering
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_trigger_type ON workflows(trigger_type)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_complexity ON workflows(complexity)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_active ON workflows(active)")
            
            # Triggers to keep FTS table in sync automatically
            self.conn.execute("""
                CREATE TRIGGER IF NOT EXISTS workflows_ai AFTER INSERT ON workflows BEGIN
                    INSERT INTO workflows_fts(rowid, filename, name, description, integrations, tags)
                    VALUES (new.id, new.filename, new.name, new.description, new.integrations, new.tags);
                END
            """)
            self.conn.execute("""
                CREATE TRIGGER IF NOT EXISTS workflows_ad AFTER DELETE ON workflows BEGIN
                    INSERT INTO workflows_fts(workflows_fts, rowid, filename, name, description, integrations, tags)
                    VALUES ('delete', old.id, old.filename, old.name, old.description, old.integrations, old.tags);
                END
            """)
            self.conn.execute("""
                CREATE TRIGGER IF NOT EXISTS workflows_au AFTER UPDATE ON workflows BEGIN
                    INSERT INTO workflows_fts(workflows_fts, rowid, filename, name, description, integrations, tags)
                    VALUES ('delete', old.id, old.filename, old.name, old.description, old.integrations, old.tags);
                    INSERT INTO workflows_fts(rowid, filename, name, description, integrations, tags)
                    VALUES (new.id, new.filename, new.name, new.description, new.integrations, new.tags);
                END
            """)

    def _get_all_db_filenames(self) -> Set[str]:
        """Internal helper to get all filenames currently in the database."""
        cursor = self.conn.execute("SELECT filename FROM workflows")
        return {row['filename'] for row in cursor.fetchall()}

    def get_file_hash(self, file_path: str) -> str:
        # This function is unchanged
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def format_workflow_name(self, filename: str) -> str:
        # This function is unchanged
        name = filename.rsplit('.json', 1)[0]
        parts = name.split('_')
        if len(parts) > 1 and parts[0].isdigit():
            parts = parts[1:]
        return ' '.join(p.capitalize() for p in parts)

    def analyze_workflow_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        # This function is unchanged
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError, IOError) as e:
            print(f"Error reading {file_path}: {str(e)}")
            return None
        
        filename = os.path.basename(file_path)
        workflow = {
            'filename': filename,
            'name': data.get('name', '').strip() or self.format_workflow_name(filename),
            'workflow_id': data.get('id'),
            'active': data.get('active', False),
            'nodes': data.get('nodes', []),
            'tags': data.get('tags', []),
            'created_at': data.get('createdAt'),
            'updated_at': data.get('updatedAt'),
            'file_hash': self.get_file_hash(file_path),
            'file_size': os.path.getsize(file_path)
        }
        node_count = len(workflow['nodes'])
        workflow['node_count'] = node_count
        if node_count <= 5: workflow['complexity'] = 'low'
        elif node_count <= 15: workflow['complexity'] = 'medium'
        else: workflow['complexity'] = 'high'
        trigger_type, integrations = self.analyze_nodes(workflow['nodes'])
        workflow['trigger_type'] = trigger_type
        workflow['integrations'] = list(integrations)
        workflow['description'] = self.generate_description(workflow, trigger_type, integrations)
        return workflow

    def analyze_nodes(self, nodes: List[Dict]) -> Tuple[str, set]:
        # This function is unchanged
        trigger_type = 'Manual'
        integrations = set()
        for node in nodes:
            node_type = node.get('type', '').lower()
            if 'trigger' in node_type and 'manual' not in node_type: trigger_type = 'Webhook'
            if 'cron' in node_type or 'schedule' in node_type: trigger_type = 'Scheduled'
            if node_type.startswith('n8n-nodes-base.'):
                service = node_type.replace('n8n-nodes-base.', '').split('trigger')[0].capitalize()
                if service and service not in ['Set', 'If', 'Switch', 'Function', 'Code', 'NoOp']:
                    integrations.add(service)
        return trigger_type, integrations

    def generate_description(self, workflow: Dict, trigger_type: str, integrations: set) -> str:
        # This function is unchanged
        desc_parts = [f"{trigger_type} automation"]
        if integrations: desc_parts.append(f"integrating {', '.join(list(integrations)[:3])}")
        desc_parts.append(f"with {workflow['node_count']} nodes.")
        return ' '.join(desc_parts)

    def index_all_workflows(self, force_reindex: bool = False) -> Dict[str, int]:
        """Syncs the database with the workflows directory using the persistent connection."""
        if not os.path.exists(self.workflows_dir):
            print(f"Warning: Workflows directory '{self.workflows_dir}' not found.")
            return {'processed': 0, 'skipped': 0, 'errors': 0, 'deleted': 0}
        
        stats = {'processed': 0, 'skipped': 0, 'errors': 0, 'deleted': 0}
        
        # Use a single transaction for the entire sync operation
        with self.conn:
            disk_files = {os.path.basename(p) for p in glob.glob(os.path.join(self.workflows_dir, "*.json"))}
            db_files = self._get_all_db_filenames()

            files_to_delete = db_files - disk_files
            if files_to_delete:
                stats['deleted'] = len(files_to_delete)
                print(f"🗑️  Deleting {stats['deleted']} stale workflows from database...")
                placeholders = ','.join('?' for _ in files_to_delete)
                self.conn.execute(f"DELETE FROM workflows WHERE filename IN ({placeholders})", list(files_to_delete))

            for filename in disk_files:
                file_path = os.path.join(self.workflows_dir, filename)
                try:
                    current_hash = self.get_file_hash(file_path)
                    if not force_reindex:
                        cursor = self.conn.execute("SELECT file_hash FROM workflows WHERE filename = ?", (filename,))
                        row = cursor.fetchone()
                        if row and row['file_hash'] == current_hash:
                            stats['skipped'] += 1
                            continue
                    
                    workflow_data = self.analyze_workflow_file(file_path)
                    if not workflow_data:
                        stats['errors'] += 1
                        continue
                    
                    self.conn.execute("""
                        INSERT INTO workflows (
                            filename, name, workflow_id, active, description, trigger_type,
                            complexity, node_count, integrations, tags, created_at, updated_at,
                            file_hash, file_size, analyzed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(filename) DO UPDATE SET
                            name=excluded.name, workflow_id=excluded.workflow_id, active=excluded.active,
                            description=excluded.description, trigger_type=excluded.trigger_type,
                            complexity=excluded.complexity, node_count=excluded.node_count,
                            integrations=excluded.integrations, tags=excluded.tags,
                            created_at=excluded.created_at, updated_at=excluded.updated_at,
                            file_hash=excluded.file_hash, file_size=excluded.file_size,
                            analyzed_at=excluded.analyzed_at
                    """, (
                        workflow_data['filename'], workflow_data['name'],
                        workflow_data['workflow_id'], workflow_data['active'],
                        workflow_data['description'], workflow_data['trigger_type'],
                        workflow_data['complexity'], workflow_data['node_count'],
                        json.dumps(workflow_data['integrations']), json.dumps(workflow_data['tags']),
                        workflow_data['created_at'], workflow_data['updated_at'],
                        workflow_data['file_hash'], workflow_data['file_size']
                    ))
                    stats['processed'] += 1
                except Exception as e:
                    print(f"Error processing {file_path}: {str(e)}")
                    stats['errors'] += 1
        
        print(f"✅ Sync complete: {stats['processed']} processed, {stats['deleted']} deleted, {stats['skipped']} skipped, {stats['errors']} errors")
        return stats

    def search_workflows(self, query: str = "", trigger_filter: str = "all", 
                         complexity_filter: str = "all", active_only: bool = False,
                         limit: int = 50, offset: int = 0) -> Tuple[List[Dict], int]:
        """Fast search using the persistent connection."""
        where_conditions, params = [], []
        if active_only: where_conditions.append("w.active = 1")
        if trigger_filter != "all":
            where_conditions.append("w.trigger_type = ?")
            params.append(trigger_filter)
        if complexity_filter != "all":
            where_conditions.append("w.complexity = ?")
            params.append(complexity_filter)
        
        if query.strip():
            base_query = "SELECT w.*, rank FROM workflows_fts fts JOIN workflows w ON w.id = fts.rowid WHERE workflows_fts MATCH ?"
            params.insert(0, query.strip() + '*')
        else:
            base_query = "SELECT w.*, 0 as rank FROM workflows w WHERE 1=1"
        
        if where_conditions: base_query += " AND " + " AND ".join(where_conditions)
        
        count_query = f"SELECT COUNT(*) as total FROM ({base_query}) t"
        total = self.conn.execute(count_query, params).fetchone()['total']
        
        order_by = "ORDER BY rank" if query.strip() else "ORDER BY w.updated_at DESC, w.created_at DESC"
        final_query = f"{base_query} {order_by} LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        rows = self.conn.execute(final_query, params).fetchall()
        
        results = []
        for row in rows:
            workflow = dict(row)
            workflow['integrations'] = json.loads(workflow['integrations'] or '[]')
            raw_tags = json.loads(workflow['tags'] or '[]')
            workflow['tags'] = [t.get('name', str(t)) if isinstance(t, dict) else str(t) for t in raw_tags]
            results.append(workflow)
        
        return results, total

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics using the persistent connection."""
        with self.conn:
            stats = {}
            stats['total'] = self.conn.execute("SELECT COUNT(*) as total FROM workflows").fetchone()['total']
            stats['active'] = self.conn.execute("SELECT COUNT(*) as active FROM workflows WHERE active = 1").fetchone()['active']
            stats['inactive'] = stats['total'] - stats['active']
            stats['triggers'] = {r['trigger_type']: r['count'] for r in self.conn.execute("SELECT trigger_type, COUNT(*) as count FROM workflows GROUP BY trigger_type")}
            stats['complexity'] = {r['complexity']: r['count'] for r in self.conn.execute("SELECT complexity, COUNT(*) as count FROM workflows GROUP BY complexity")}
            stats['total_nodes'] = self.conn.execute("SELECT SUM(node_count) FROM workflows").fetchone()[0] or 0
            
            all_integrations = set()
            for row in self.conn.execute("SELECT integrations FROM workflows WHERE integrations != '[]'"):
                all_integrations.update(json.loads(row['integrations']))
            stats['unique_integrations'] = len(all_integrations)
            stats['last_indexed'] = datetime.datetime.now().isoformat()
        
        return stats

    # The other methods like get_service_categories and search_by_category are unchanged
    # and omitted here for brevity but should be kept in your file.

# The main function for CLI usage remains unchanged
def main():
    import argparse
    parser = argparse.ArgumentParser(description='N8N Workflow Database')
    # ... (rest of the main function is unchanged)
    args = parser.parse_args()
    db = WorkflowDatabase()
    # ... (rest of the logic)
    
if __name__ == "__main__":
    main()
