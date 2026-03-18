import asyncio
import json
import logging
import os
import time
import re
import pandas as pd
import aiofiles
from typing import List, Dict, Any
from ollama import AsyncClient

# --- КОНФИГУРАЦИЯ ---
OLLAMA_MODEL = "qwen2.5:7b"  # Оптимально для вашего размера
OLLAMA_HOST = "http://localhost:11434"
MAX_CONCURRENT = 3  
BATCH_SIZE = 50     
INPUT_FILE = "./data/raw/goodreads_ds_sample.csv" 
OUTPUT_DIR = "output_qwen"
FINAL_JSON = "enriched_books_final.json"
FINAL_CSV = "enriched_books_final.csv"
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "checkpoint.json")

# Жанры для фильтрации
GENRE_LIST = [
    "Science Fiction", "Fantasy", "Mystery", "Thriller", "Romance",
    "Historical Fiction", "Biography", "Non-fiction", "Poetry", "Drama",
    "War", "Adventure", "Classic", "Satire", "Philosophical Fiction"
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)
os.makedirs(OUTPUT_DIR, exist_ok=True)
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# --- ЛОГИКА ОБРАБОТКИ ---

async def llm_call(prompt: str, system: str) -> str:
    try:
        async with semaphore:
            response = await AsyncClient(host=OLLAMA_HOST).chat(
                model=OLLAMA_MODEL,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt}
                ],
                options={"temperature": 0.0}
            )
            return response['message']['content'].strip()
    except Exception as e:
        logger.error(f"Error: {e}")
        return ""

async def enrich_book(book: Dict) -> Dict:
    item = book.copy()
    
    # 1. Жанры: Очистка (Только из списка)
    tags = str(item.get("genres", ""))
    sys_g = f"Filter tags. Return ONLY categories from: {', '.join(GENRE_LIST)}. No prose, just comma-separated list or 'Unknown'."
    res_g = await llm_call(f"Tags: {tags}", sys_g)
    item["genres_cleaned"] = [g.strip() for g in res_g.split(',') if g.strip() in GENRE_LIST] or ["Unknown"]

    # 2. Страна: Реальный мир
    sys_c = "Identify the real-world country of origin. Return ONLY the country name. If fictional world, return 'null'."
    info = f"Title: {item.get('title')} | Desc: {str(item.get('description'))[:200]}"
    res_c = await llm_call(info, sys_c)
    item["country_origin"] = re.sub(r'[^a-zA-Z\s]', '', res_c).strip() if "null" not in res_c.lower() else None

    # 3. История: События и личности
    sys_h = "Extract REAL historical events/figures. JSON ONLY: {\"events\": [], \"figures\": []}. Fictional characters = Forbidden."
    res_h = await llm_call(f"Text: {str(item.get('description'))[:500]}", sys_h)
    
    item["historical"] = {"events": [], "figures": []}
    match = re.search(r'\{.*\}', res_h, re.DOTALL)
    if match:
        try: item["historical"] = json.loads(match.group(0))
        except: pass

    return item

# --- ПАЙПЛАЙН ---

async def main():
    # Загрузка
    if INPUT_FILE.endswith('.csv'):
        df_in = pd.read_csv(INPUT_FILE)
        data = df_in.to_dict(orient='records')
    else:
        with open(INPUT_FILE, 'r') as f: data = json.load(f)

    start_idx = 0
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f: start_idx = json.load(f).get('last_idx', 0)

    # Запуск батчей
    for i in range(start_idx, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        logger.info(f"Processing {i} to {i+len(batch)}...")
        
        results = await asyncio.gather(*[enrich_book(b) for b in batch])
        
        async with aiofiles.open(os.path.join(OUTPUT_DIR, f"b_{i}.json"), 'w') as f:
            await f.write(json.dumps(results, ensure_ascii=False))
            
        with open(CHECKPOINT_FILE, 'w') as f: json.dump({'last_idx': i + BATCH_SIZE}, f)

    # --- ФИНАЛЬНОЕ ОБЪЕДИНЕНИЕ ---
    logger.info("Merging results...")
    all_results = []
    for file in sorted(os.listdir(OUTPUT_DIR)):
        if file.startswith("b_") and file.endswith(".json"):
            with open(os.path.join(OUTPUT_DIR, file), 'r') as f:
                all_results.extend(json.load(f))

    # Сохраняем JSON
    with open(FINAL_JSON, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    # Сохраняем CSV (разворачиваем списки в строки для удобства)
    df_final = pd.DataFrame(all_results)
    df_final['genres_cleaned'] = df_final['genres_cleaned'].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    df_final['hist_events'] = df_final['historical'].apply(lambda x: ", ".join(x.get('events', [])))
    df_final['hist_figures'] = df_final['historical'].apply(lambda x: ", ".join(x.get('figures', [])))
    df_final.drop(columns=['historical']).to_csv(FINAL_CSV, index=False)

    logger.info(f"Done! Saved to {FINAL_JSON} and {FINAL_CSV}")

if __name__ == "__main__":
    asyncio.run(main())