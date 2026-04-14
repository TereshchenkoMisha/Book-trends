# Literary Evolution: Tracking Historical Trends through books

## Overview

**Literary Evolution** is an interactive data visualization project that explores how literature has developed over time and across different countries. By analyzing a large dataset of books published between 1970 and 2020, we uncover patterns in genre popularity, the influence of historical events, reader rating trends, and the geographic distribution of authorship.

The project answers questions like:
- How did book production evolve by genre over five decades?
- Which historical events (e.g., World War II, Civil Rights Movement) inspired the most books?
- Do readers rate books about certain events higher than others?
- Are modern books rated more highly than older ones?
- How do genres and historical events correlate?
- Which authors write most prolifically about specific events?

## Features

- **Scrollytelling interface** – a single‑page narrative that guides users through seven interactive visualizations.
- **Dynamic charts** built with Plotly (Python) and embedded as HTML iframes in a React frontend.
- **Dropdown interactivity** – many graphs allow switching between events, countries, or genres.
- **Smooth scroll animations** – text and graphs fade in as you scroll, creating a guided storytelling experience.
- **Custom fonts and dark theme** – designed for readability and visual immersion.

## Data Source

We collected and cleaned a large dataset of books published from **1970 to 2020**. The raw data includes:
- Book titles, authors, publication years
- Genres (Adventure, Biography & Memoir, Children, Fantasy, Fiction, Historical Fiction, Horror, Mystery & Thriller, Non‑Fiction, Philosophy & Religion, Poetry & Drama, Romance, Short Stories, War, Young Adult)
- Historical events mentioned in the book (extracted from descriptions or tags)
- Average reader ratings and number of ratings
- Author country of origin

Due to the large file size, the raw dataset is stored on Google Drive:

> **[📁 Raw data folder (Google Drive)](https://drive.google.com/drive/folders/1y9T4hy28kPubAQgYPswVDS-7vir1zfAL?usp=sharing)** – contains the initial scraped JSON files and processed database.

## Tech Stack

| Layer       | Technologies                                                                 |
|-------------|------------------------------------------------------------------------------|
| Data processing | Python (pandas, sqlite3, json)                                         |
| Visualization  | Plotly (choropleth maps, scatter plots, bar charts, heatmaps, lollipop charts) |
| Frontend       | React (with hooks: useState, useEffect, useRef), Intersection Observer API |
| Styling        | Custom CSS, CSS animations, responsive design                               |
| Fonts          | BarlowCondensed (body), Thernaly (headings) – locally hosted                |
| Database       | SQLite (books.db)                                                           |


## Visualizations Included

1. **Cumulative Timeline by Genre** – Stacked area chart showing the growth of each genre from 1970 to 2020.
2. **Top Historical Events & Figures** – Bar chart of the most frequently mentioned events and people.
3. **Rating vs. Popularity (by Event)** – Scatter plot comparing average rating with number of ratings for each event.
4. **Rating Waterfall by Decade** – Waterfall chart showing the change in average book ratings relative to the 1970s baseline.
5. **Genre–Event Heatmap** – Heatmap showing what percentage of books in a genre mention a specific historical event.
6. **Top Authors Writing About Historical Events** – Lollipop chart comparing total books by an author vs. books about a chosen event.
7. **Choropleth: Civil Wars by Country** – World map with a dropdown to select a civil war; color intensity shows number of books written by authors from each country.

## How to Run Locally

### Prerequisites
- Node.js (v14+)
- Python 3.8+ (if you want to regenerate graphs)
- SQLite3

### Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/literary-evolution.git
   cd literary-evolution
   
2. **Set up the Frontend**

   ```bash
   cd frontend
   npm install
   npm start

   ```
   
**(Optional) Regenerate the Graphs from Scratch**
   ```bash
cd ../data-processing
pip install -r requirements.txt
python generate_graphs.py
   ```

This will create new HTML files in:
../frontend/public/graphs/

3. **Ensure the Database is in Place**


   Download books.db from the Google Drive folder and place it in:
frontend/public/

## Team: 
Saida Musaeva, Anna Tikhonova, Mikhail Tereshchenko