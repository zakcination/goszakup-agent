"""
services/agent_api/main.py
--------------------------
Compatibility entrypoint for Docker CMD.
"""

from api.app import app  # noqa: F401

