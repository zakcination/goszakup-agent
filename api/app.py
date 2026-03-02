"""
api/app.py
----------
Strict agent API entrypoint.
"""

from __future__ import annotations

from fastapi import FastAPI

from agent.agent import AgentWorkflow
from agent.schemas import QueryRequest
from agent.tools import tools_manifest

app = FastAPI(title="goszakup-agent-api", version="0.2")
workflow = AgentWorkflow()


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/tools")
async def tools() -> dict:
    return {
        "workflow": [
            "Parse Intent",
            "Build Tool Call",
            "Check Cache",
            "Execute Tool",
            "Enrich Context",
            "Verbalize",
            "Drill-Down Ready",
        ],
        "tools": tools_manifest(),
    }


@app.post("/query")
async def query(req: QueryRequest) -> dict:
    response = await workflow.handle_query(req.question, req.language)
    return response.model_dump()


# Backward compatibility with current UI.
@app.post("/ask")
async def ask(req: QueryRequest) -> dict:
    response = await workflow.handle_query(req.question, req.language)
    return response.model_dump()

