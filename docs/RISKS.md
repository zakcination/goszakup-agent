# Risks & Limitations

- **KATO coverage:** region mapping depends on `kato_ref` being loaded.
- **Contract items coverage:** not all contracts have units; Fair Price may be limited.
- **Macro indices:** national constants only; no regional deflators yet.
- **Data drift:** incremental updates rely on `/v3/journal` shape; monitor for API changes.
- **LLM layer:** current agent is rule‑based; full NL understanding requires LLM integration.
