"""MCP server: Boomi lineage tools for Claude (Desktop / Code) and the triage agent.

    python -m boomi_ops.lineage.mcp_server --graph data/live/viewer/lineage.json

Read-only. It reads lineage.json (written by `python -m boomi_ops.run lineage viewer`) and re-reads it when the
file changes, so a nightly crawl shows up without restarting the server. It never calls Boomi itself.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from mcp.server.mcpserver import MCPServer

from .queries import Graph

DEFAULT_GRAPH = os.environ.get("BOOMI_LINEAGE_GRAPH", "data/live/viewer/lineage.json")


class _Cache:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._graph: Graph | None = None
        self._mtime = None

    def get(self) -> Graph:
        mtime = self.path.stat().st_mtime  # FileNotFoundError reaches the client as a tool error
        if self._graph is None or mtime != self._mtime:
            self._graph, self._mtime = Graph.load(self.path), mtime
        return self._graph


def build_server(graph_path: str | Path = DEFAULT_GRAPH) -> MCPServer:
    cache = _Cache(graph_path)
    server = MCPServer(
        "boomi-lineage",
        instructions=(
            "Lineage of a Boomi account: which processes use which connections, operations, maps, profiles and "
            "sub-processes, weighted by each process's runs and failures in the last 30 days. Use "
            "get_blast_radius before changing a component, and get_shared_dependencies when a process fails, "
            "to see whether the cause is shared with other processes."),
    )

    @server.tool()
    def find_component(query: str) -> dict:
        """Find Boomi components by name (or part of a name). Returns name, kind and id for each match."""
        hits = cache.get().find(query)
        return {"matches": [{"name": n["name"], "kind": n["kind"], "id": n["id"]} for n in hits[:25]],
                "total": len(hits)}

    @server.tool()
    def get_blast_radius(component: str) -> dict:
        """What breaks if this component changes: every process that uses it (directly or through maps and
        sub-processes), how many are deployed, and their runs and failures in the last 30 days. Also lists what
        the component itself uses. `component` is a name, part of one, or an id."""
        return cache.get().blast_radius(component)

    @server.tool()
    def get_shared_dependencies(process: str, failure_at: str | None = None, window_minutes: int = 60) -> dict:
        """For a failed run of a process: the connections, operations, maps and sub-processes it shares with
        other processes, and which of those processes failed at about the same time (within window_minutes of
        failure_at, given as 2026-11-18T06:10Z; default: the process's latest failure). The `summary` says
        whether the failure looks shared (check that component first) or local to this process."""
        return cache.get().shared_dependencies(process, failure_at, window_minutes)

    @server.tool()
    def get_lineage_health() -> dict:
        """Account-wide lineage checks: orphan components, API/XML drift, links to deleted components,
        sub-process cycles, and the riskiest shared components."""
        return cache.get().health()

    return server


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--graph", default=DEFAULT_GRAPH, help="lineage.json written by `lineage viewer`")
    build_server(ap.parse_args().graph).run("stdio")


if __name__ == "__main__":
    main()
