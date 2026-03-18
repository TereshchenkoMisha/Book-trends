#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Final genre classification script for large-scale book datasets.
Uses a compact genre list, strict prompt, and post-processing to handle "Other".
Includes retry mechanism and concurrency control.
"""

import os
import json
import re
import asyncio
from openai import AsyncOpenAI
from openai import APITimeoutError, APIConnectionError, RateLimitError, APIStatusError

# ======================== НАСТРОЙКИ ========================
ZHIPU_API_KEY = os.environ.get("ZHIPU_API_KEY", "3f9309b05fcc44798346932a9ac95c75.2AgAcYv9olBknU0J")
BASE_URL = "https://open.bigmodel.cn/api/paas/v4/"
MODEL_NAME = "glm-4-flash"
CONCURRENCY = 5          # Для 105k книг можно увеличить до 10, следите за лимитами API
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 30.0
INPUT_JSON = "./data/processed/books_enriched.json"      # ваш входной файл
OUTPUT_JSON = "./data/processed/books_genre_final.json"   # результат

# ======================== ЦЕЛЕВОЙ СПИСОК ЖАНРОВ (15-16) ========================
TARGET_GENRES = [
    "Fiction",
    "Science Fiction & Fantasy",
    "Mystery & Thriller",
    "Romance",
    "Historical Fiction",
    "Biography & Memoir",
    "Non-Fiction",
    "Philosophy & Religion",
    "Poetry & Drama",
    "Children",
    "Young Adult",
    "Horror",
    "Adventure",
    "War",
    "Short Stories",
    "Other"
]

VALID_GENRES_SET = set(TARGET_GENRES)

# ======================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========================
def safe_print_error(e):
    try:
        print(str(e).encode('ascii', 'ignore').decode('ascii'))
    except:
        pass

def extract_json(text):
    if not text or not text.strip():
        return None
    match = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
    if match:
        candidate = match.group(1)
        try:
            return json.loads(candidate)
        except:
            pass
    try:
        return json.loads(text)
    except:
        return None

def should_retry(error):
    retryable_errors = (APITimeoutError, APIConnectionError, RateLimitError)
    if isinstance(error, retryable_errors):
        return True
    if isinstance(error, APIStatusError):
        return error.status_code >= 500 or error.status_code == 429
    return False

async def call_with_retry(func, *args, **kwargs):
    last_error = None
    delay = INITIAL_RETRY_DELAY
    for attempt in range(MAX_RETRIES + 1):
        try:
            if attempt > 0:
                await asyncio.sleep(delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)
            return await func(*args, **kwargs)
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES and should_retry(e):
                safe_print_error(f"⚠️ Retry {attempt+1}/{MAX_RETRIES}: {e}")
                continue
            else:
                raise e
    raise last_error

# ======================== ИНИЦИАЛИЗАЦИЯ КЛИЕНТА ========================
if not ZHIPU_API_KEY:
    raise ValueError("❌ API-ключ Zhipu AI не указан!")

aclient = AsyncOpenAI(api_key=ZHIPU_API_KEY, base_url=BASE_URL)

# ======================== ФУНКЦИЯ КЛАССИФИКАЦИИ ЖАНРОВ ========================
async def get_genres(title, description):
    genres_str = ", ".join(TARGET_GENRES)
    prompt = f"""
You are a literary expert. Based on the book title and description, classify its genre(s) from the following list ONLY:
[{genres_str}].

### IMPORTANT RULES ###
1. **Biography & Memoir**: Use ONLY for NON-FICTION works that tell the real-life story of a real person. If the book is a novel (fiction) — even if it includes real historical figures like Napoleon — do NOT use Biography & Memoir.
2. **Historical Fiction**: Use for fictional stories set in the past that may include real historical events or figures. If the description mentions "novel", "fiction", or invented characters, prefer Historical Fiction over Biography.
3. **War**: Use for books that focus on warfare, battles, military campaigns, or are set during a war. Even if the book is also Historical Fiction, include "War" if war is a central theme. For example, "War and Peace" should have "War" in addition to "Historical Fiction".
4. **Fiction**: Use for general literary fiction that is not strongly tied to a specific subgenre.
5. **Non-Fiction**: Use for factual books (e.g., guides, reference, textbooks, art, design). Do not use Fiction for these.
6. **Science Fiction & Fantasy**, **Mystery & Thriller**, **Romance**, etc.: Use according to the dominant theme.
7. **Children** vs **Young Adult**: Children's books are for younger audience (simple language, illustrations), Young Adult targets teenagers (coming-of-age, identity).
8. **Other**: Use ONLY if the book does NOT fit into any of the other categories. Never combine "Other" with other genres — if you assign any specific genre, do NOT include "Other".

Choose 1–3 genres. Be specific but avoid over-tagging. If uncertain, pick the most relevant ones.

### Examples ###
- Title: "War and Peace" Description: "Epic novel set during Napoleon's invasion of Russia, following fictional characters Pierre, Andrei, and Natasha." → Genres: ["Historical Fiction", "War", "Fiction"]
- Title: "The Diary of a Young Girl" Description: "Anne Frank's diary of hiding during WWII" → Genres: ["Biography & Memoir", "Non-Fiction"]
- Title: "Logo Design Workbook" Description: "Step-by-step guide to creating logos" → Genres: ["Non-Fiction"]
- Title: "Dune" Description: "Science fiction saga on a desert planet with political intrigue" → Genres: ["Science Fiction & Fantasy", "Adventure"]
- Title: "The Name of the Rose" Description: "Historical murder mystery set in an Italian monastery" → Genres: ["Historical Fiction", "Mystery & Thriller"]
- Title: "All Quiet on the Western Front" Description: "A story of German soldiers in WWI" → Genres: ["War", "Fiction"]
- Title: "A Very Obscure Pamphlet" Description: "A collection of random notes with no clear topic" → Genres: ["Other"]

Return a JSON object with a key "genres" containing an array of strings. Only output valid JSON, no additional text.

Title: {title}
Description: {description}
"""
    async def _make_call():
        return await aclient.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=250  # немного увеличим, чтобы модель могла дать развёрнутый ответ
        )
    
    try:
        response = await call_with_retry(_make_call)
        content = response.choices[0].message.content
        parsed = extract_json(content)
        if parsed and isinstance(parsed, dict) and "genres" in parsed:
            valid_genres = [g for g in parsed["genres"] if g in VALID_GENRES_SET]
            
            # Дополнительная проверка: если есть "Historical Fiction" и "Biography & Memoir", удаляем Biography
            if "Historical Fiction" in valid_genres and "Biography & Memoir" in valid_genres:
                valid_genres = [g for g in valid_genres if g != "Biography & Memoir"]
            
            # Правило для "Other": удаляем, если есть хотя бы один другой жанр
            if "Other" in valid_genres and len(valid_genres) > 1:
                valid_genres = [g for g in valid_genres if g != "Other"]
            
            # Если после всех фильтров список пуст, ставим Other
            return valid_genres if valid_genres else ["Other"]
        else:
            print(f"⚠️ Не удалось распарсить ответ. Получено: {content[:200]}")
            return ["Other"]
    except Exception as e:
        safe_print_error(e)
        return ["Other"]

# ======================== ОСНОВНАЯ ОБРАБОТКА ========================
async def process_book(book):
    title = book.get('title', '')
    description = book.get('description', '')
    if not title and not description:
        print(f"⚠️ Книга {book.get('book_id', 'unknown')} без названия и описания, пропуск.")
        return book
    
    print(f"🔄 Обрабатывается: {title}")
    genres = await get_genres(title, description)
    book['genres'] = genres
    return book

async def process_all(books, concurrency=CONCURRENCY):
    semaphore = asyncio.Semaphore(concurrency)
    async def bounded_process(book):
        async with semaphore:
            return await process_book(book)
    tasks = [bounded_process(book) for book in books]
    return await asyncio.gather(*tasks)

async def test_api_key():
    try:
        await aclient.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": "test"}],
            max_tokens=5
        )
        print("✅ Ключ действителен.")
    except Exception as e:
        raise RuntimeError(f"❌ Ошибка ключа: {e}")

def main():
    # Загрузка данных
    try:
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            books = json.load(f)
    except FileNotFoundError:
        print(f"❌ Файл {INPUT_JSON} не найден.")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка парсинга JSON: {e}")
        return

    print(f"📚 Загружено {len(books)} книг.")

    # Проверка ключа
    try:
        asyncio.run(test_api_key())
    except RuntimeError as e:
        print(e)
        return

    # Запуск обработки
    updated_books = asyncio.run(process_all(books, concurrency=CONCURRENCY))

    # Сохранение
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(updated_books, f, indent=2, ensure_ascii=False)

    print(f"✅ Обработка завершена. Результат в {OUTPUT_JSON}")

if __name__ == "__main__":
    main()