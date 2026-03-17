# Project Roadmap: Literary Evolution

This roadmap tracks the development of the "Literature Development Map" as defined in the official project proposal.

##  Phase 1: Data Acquisition & Infrastructure (February)
*Target: Large-scale ingestion of book metadata and film connection.*
**Repository Setup**: Initialize project architecture and Docker orchestration.
**Initial Scraper**: Developed multi-threaded `scraper_gr.py` for Goodreads IDs.
**Scale Collection**: Reach initial dataset of **100,000 entities** (books/metadata).
**Expanded Sources**: Implement scrapers for **Wikipedia** (film adaptations) and **OnlineBookClub/SFF Chronicles** (social trends).
**Live Component**: Set up continuous scraping for **50,000 new entries per day**.

## Phase 2: AI-Native Pipeline & Enrichment (Early March)
*Target: Leverage GenAI agents for data cleaning and structuring.
**Query Optimization**: Use LLM to determine optimal query order and grouping for API requests.
**Genre Ontology**: Apply embeddings and clustering to unify incompatible tags (e.g., "Sci-Fi" vs "Science Fiction").
**Thematic Classification**: Use LLM to classify each book into two main genres based on plot summaries.
**Content Analysis**: Use Generative AI to identify literary techniques, slang, and keywords in book blurbs.

## Phase 3: Backend & Database Integration (Mid March)
Target: Build a high-performance system for time-series and vector data.
**Time-Series DB**: Implement **PostgreSQL with TimescaleDB** for efficient publication date queries.
**Vector Storage**: Use **pgvector** or JSONB to store text annotations and LLM-generated embeddings.
**API Layer**: Build a **FastAPI** backend to serve pre-processed data to the frontend.
**Caching**: Implement server-side caching for heavy calculations and filtered results.

## Phase 4: Hero Visualization & Frontend (April)
Create interactive 2D graphs and choropleth maps.
**Streamgraph**: Build the main interactive canvas using **D3.js** to show genre popularity shifts.
**Interactive Map**: Implement a **Leaflet.js** choropleth map for country/region filtering.
**Event Layers**: Add visual markers for historical events and film adaptation releases on the timeline.
**UI/UX**: Build the **React/TypeScript** application with smooth animated transitions.

## Phase 5: AI Audit & Final Delivery (May)
 **Performance Tuning**: Implement adaptive sampling for the timeline (decades vs. years).
**AI Audit**: Complete final documentation on GenAI usage and ethics.
**Final Report**: Finalize data visualization insights and technical findings.

---
