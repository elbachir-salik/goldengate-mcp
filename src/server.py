"""
GoldenGate MCP Server — entry point.

Initialises the FastMCP application, registers all tool modules, and starts
the server.  Dependencies (Oracle pool, schema mapper, audit log) are
initialised on startup and torn down gracefully on shutdown.
"""
