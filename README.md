# 🕉️ Bhashyam AI - Research Suite

Bhashyam AI is an advanced research platform designed for the exploration and analysis of Sanatana Dharma scriptures. By combining the power of Graph Databases (Neo4j & ArcadeDB) with Large Language Models (OpenAI), it provides a semantic and relational interface to sacred texts, enabling scholars and researchers to uncover deep connections across various scriptures.

## 🚀 Key Features

- **💬 Research Chat:** A context-aware chatbot that utilizes Graph-RAG (Retrieval-Augmented Generation) to answer complex philosophical queries. It combines Full-Text Search (FTS) with graph traversal to provide cited responses from scriptures.
- **🏷️ Topic & Character Index:** Exploratory interfaces to browse verses indexed by specific philosophical topics or historical/divine characters.
- **📜 Scripture Browser:** A detailed view of various scriptures (e.g., Bhagavad Gita, Upanishads) with enrichment metrics tracking the progress of translations, word-by-word meanings, and entity linking.
- **📊 Enrichment Analytics:** Real-time statistics on database coverage, including the percentage of verses with translations, word-by-word analysis, and topic tags.
- **🗄️ Multi-Database Support:** Implementation support for both **Neo4j** (standard graph) and **ArcadeDB** (multi-model database).
- **🎨 Interactive Visualizations:** Integration with `vis-network` for graph-based visualization of scriptural relationships.
- **🗺️ Scripture Explorer:** Dedicated visualization tools (`app_vis.py` and `app_vis_arcade.py`) that allow users to drill down into the hierarchy of scriptures, from chapters down to individual verses and linked metadata (locations, authors).

## 🛠️ Tech Stack

- **Frontend:** [Gradio](https://gradio.app/) (Python-based interactive UI)
- **Backend:** Python (Flask for lightweight API needs and proxying graph requests)
- **AI/LLM:** OpenAI GPT-4o / GPT-4o-mini
- **Graph Databases:** Neo4j (Cypher), ArcadeDB
- **Data Cache:** SQLite (for LLM result caching and persistence)
- **Visualization:** [Pyvis](https://pyvis.readthedocs.io/) & [vis.js](https://visjs.org/)

## 📋 Prerequisites

- Python 3.13+
- A running instance of Neo4j or ArcadeDB
- OpenAI API Key

## ⚙️ Setup & Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd llm-knowledge-graph-demo
   ```

2. **Install dependencies using `uv` or `pip`:**
   ```bash
   uv sync
   # OR
   pip install -r requirements.txt
   ```

3. **Environment Configuration:**
   Create a `.env` file in the root directory and add your credentials:
   ```env
   OPENAI_API_KEY=your_openai_key
   NEO4J_URI=bolt://localhost:7687
   NEO4J_USER=neo4j
   NEO4J_PASSWORD=your_password
   # ArcadeDB settings
   ARCADE_DB=BhashyamDB
   ARCADE_USER=root
   ARCADE_PASSWORD=your_arcade_password
   ```

4. **Launch the Application:**
   To run the main Research Suite (Neo4j):
   ```bash
   python app.py
   ```
   To run the Scripture Explorer Visualization (Neo4j):
   ```bash
   python app_vis.py
   ```
   To run the ArcadeDB-backed visualization (includes a background Flask proxy):
   ```bash
   python app_vis_arcade.py
   ```

## 📂 Project Structure

- `app.py`: Main Gradio application using Neo4j for research and chat.
- `app_arcadedb.py`: Gradio application implementation for ArcadeDB.
- `app_vis.py`: Neo4j-based scripture hierarchy visualization.
- `app_vis_arcade.py`: ArcadeDB-based scripture hierarchy visualization with dynamic drill-down support.
- `arcadedb_utils.py`: Utility functions for ArcadeDB operations.
- `reload_arcade.py`: Maintenance script to cleanup ArcadeDB and perform a full reload from Neo4j.
- `lib/`: Contains custom JavaScript and CSS for advanced UI components (Tom-Select, Vis-Network).
- `pyproject.toml`: Project metadata and dependency definitions.

## 🤝 Contributing

Contributions to the Bhashyam AI project are welcome. Please ensure that any new features or bug fixes include appropriate documentation and align with the project's goal of preserving and exploring scriptural knowledge.
