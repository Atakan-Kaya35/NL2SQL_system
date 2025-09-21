# NL2SQL System

<p align="center">
  <img src="./NL2SQL_struct.drawio.png" alt="BSP Logo" width="100"/>
</p>

## Overview

This project implements a **Natural Language to SQL (NL2SQL)** pipeline, enabling users to query relational databases using plain language instead of SQL. The system emphasizes flexibility, transparency, and production-readiness while remaining database-agnostic.

!!! There has been a heavy reduction in the code for the confidentiality of a state entity and its inner operations. The places that are redacted are labeled with "redacted".

## Key Technical Achievements

* **Transformer-based LLM Integration**

  * Deployed open-source models (e.g., Llama 3.1, Mistral, Qwen, Gemma, Phi-3).
  * Evaluated schema-aware research models (e.g., RAT-SQL, BRIDGE, PICARD).
  * Supported LoRA fine-tuning and RAG-grounded prompting.

* **Grounding Strategies**

  * **Prompt Grounding:** Dynamically inject schema metadata into prompts.
  * **Retrieval-Augmented Generation (RAG):** Vector database storing schema/column/row examples, retrieved at query time.
  * **Fine-Tuning:** Schema-specific tuning for stable, fixed schemas.

* **Automation Spectrum**

  * Supported modes ranging from **suggest-only** (human-in-the-loop) to **semi-automatic approval** workflows.
  * Integrated user-confirmation step for safety and transparency.
  * Hybrid automation with symbolic guardrails.

* **Error Handling & Self-Repair**

  * Implemented iterative retry mechanism (failed queries → error feedback → auto-correction).
  * Logging and replay of corrections for auditability.

* **Guardrails & Safety**

  * Enforced **read-only queries** (`SELECT` only).
  * Schema/role-based restrictions to block sensitive access.
  * Rate limiting to protect backend stability.
  * Query approval/rejection layer to prevent misuse (e.g., prompt injection).

* **(Future Advancement) Advanced Reasoning**

  * Chain-of-thought decomposition for multi-step queries.
  * Incremental query execution for complex reasoning tasks.
  * Interactive disambiguation: system asks clarifying questions when queries are underspecified.

* **Infrastructure & Ecosystem**

  * Containerized deployment (Docker Compose) with modular services.
  * Open-source orchestration frameworks: **LangChain**, **LlamaIndex**.
  * PostgreSQL backend for evaluation and structured schema handling.
  * GPU-accelerated model inference (6-8 GB VRAM baseline).

* **Performance & Cost Profiling**

  * Benchmarked models by inference cost (kWh/query) and flexibility.
  * Compared lightweight Seq2SQL vs. generalist LLMs vs. tuned hybrids.
  * Achieved balance of scalability and low operational overhead.

## Suggested Production-Grade Extensions

* Schema-specific fine-tuning for very large or complex databases.
* Enterprise guardrails: role-based access control, more granular logging.
* Caching of frequent queries and embeddings for faster responses.
* Dashboard for real-time monitoring of query flows, costs, and errors.
* Integration with enterprise authentication systems (e.g., SSO).

## References

* [NL2SQL Handbook (HKUST)](https://github.com/HKUSTDial/NL2SQL_Handbook)
* [Google Cloud NL2SQL Blog](https://cloud.google.com/blog/products/data-analytics/nl2sql-with-bigquery-and-gemini)
* [Microsoft Semantic Kernel Example](https://devblogs.microsoft.com/semantic-kernel/guest-blog-bridging-business-and-technology-transforming-natural-language-queries-into-sql-with-semantic-kernel-part-2)
* [Demo Video](https://www.youtube.com/watch?v=fss6CrmQU2Y)
