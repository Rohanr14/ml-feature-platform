"""
LLM-powered query agent for the ML platform.

Uses LangChain + pgvector to:
- Query pipeline metadata ("what features does user X have?")
- Explain model predictions in natural language
- Surface data quality issues
- Answer questions about feature definitions
"""

# TODO Phase 4: Implement
# 1. Index feature store metadata, dbt docs, model cards into pgvector
# 2. Build LangChain RetrievalQA chain
# 3. Add tools for querying Feast, MLflow, Delta Lake directly
# 4. Expose via FastAPI endpoint (or add to serving app)
