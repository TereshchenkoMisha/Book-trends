#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Improved AI-pipeline for book data enrichment using Zhipu AI (GLM-4-Flash).
Includes retry mechanism with exponential backoff for handling timeouts and transient errors.
Reads from CSV, processes each book, and saves enriched JSON.
"""

import os
import json
import re
import asyncio
import time
import pandas as pd
from openai import AsyncOpenAI
from openai import APITimeoutError, APIConnectionError, RateLimitError, APIStatusError

# ======================== НАСТРОЙКИ ========================
ZHIPU_API_KEY = os.environ.get("ZHIPU_API_KEY", "3f9309b05fcc44798346932a9ac95c75.2AgAcYv9olBknU0J")
BASE_URL = "https://open.bigmodel.cn/api/paas/v4/"
MODEL_NAME = "glm-4-flash"
CONCURRENCY = 1  # Пока оставим 1 для надёжности
MAX_RETRIES = 5  # Максимальное количество повторных попыток
INITIAL_RETRY_DELAY = 1.0  # Начальная задержка в секундах
MAX_RETRY_DELAY = 30.0  # Максимальная задержка
INPUT_CSV = "./data/raw/goodreads_ds_sample.csv"
OUTPUT_JSON = "./data/processed/books_enriched.json"
ENCODING = "utf-8"

COL_TITLE = "title"
COL_AUTHOR = "author"
COL_DESCRIPTION = "description"
COL_LANGUAGE = "language"

# ======================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========================
def safe_print_error(e):
    """Безопасно выводит ошибку, игнорируя проблемы кодировки."""
    try:
        print(str(e).encode('ascii', 'ignore').decode('ascii'))
    except:
        print("Произошла ошибка, но не удалось вывести детали.")

def extract_json(text):
    """Извлекает JSON из текста ответа модели, поддерживает пояснения."""
    if not text or not text.strip():
        return None
    # Ищем JSON-объект (в фигурных скобках) или массив (в квадратных)
    match = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
    if match:
        candidate = match.group(1)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
    # Если не нашли, пробуем распарсить весь текст
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

def should_retry(error):
    """Определяет, стоит ли повторить запрос при данной ошибке."""
    # Транзиентные ошибки, которые имеют смысл повторить
    retryable_errors = (
        APITimeoutError,           # Таймаут
        APIConnectionError,        # Проблемы с соединением
        RateLimitError,            # 429 Too Many Requests
    )
    if isinstance(error, retryable_errors):
        return True
    # Для APIStatusError (4xx, 5xx) повторим только 5xx и 429
    if isinstance(error, APIStatusError):
        return error.status_code >= 500 or error.status_code == 429
    return False

async def call_with_retry(func, *args, **kwargs):
    """Универсальная функция для вызова API с повторными попытками."""
    last_error = None
    delay = INITIAL_RETRY_DELAY
    
    for attempt in range(MAX_RETRIES + 1):  # +1 для первой попытки
        try:
            if attempt > 0:
                print(f"🔄 Повторная попытка {attempt}/{MAX_RETRIES} после задержки {delay:.1f}с...")
                await asyncio.sleep(delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)  # Экспоненциальная задержка
            
            return await func(*args, **kwargs)
            
        except Exception as e:
            last_error = e
            
            if attempt < MAX_RETRIES and should_retry(e):
                safe_print_error(f"⚠️ Ошибка (попытка {attempt+1}/{MAX_RETRIES+1}): {e}")
                # Для rate limit (429) можно использовать Retry-After из заголовков
                if isinstance(e, RateLimitError) and hasattr(e, 'response'):
                    retry_after = e.response.headers.get('retry-after')
                    if retry_after:
                        try:
                            delay = float(retry_after)
                            print(f"⏳ Сервер просит подождать {delay}с")
                        except:
                            pass
                continue
            else:
                # Неповторяемая ошибка или кончились попытки
                raise e
    
    # Если все попытки исчерпаны
    raise last_error

# ======================== ПРОВЕРКА КЛЮЧА ========================
if not ZHIPU_API_KEY:
    raise ValueError("❌ API-ключ Zhipu AI не указан!")

aclient = AsyncOpenAI(api_key=ZHIPU_API_KEY, base_url=BASE_URL)

async def test_api_key():
    """Проверяет, работает ли ключ, выполняя простой запрос."""
    async def _test():
        return await aclient.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": "Say 'test'"}],
            temperature=0,
            max_tokens=5
        )
    
    try:
        response = await call_with_retry(_test)
        print("✅ Ключ действителен, начинаем обработку...")
    except Exception as e:
        safe_print_error(e)
        raise RuntimeError("Проверьте ключ, base_url и имя модели.") from e

# ======================== УЛУЧШЕННЫЕ ПРОМПТЫ С RETRY ========================
async def get_country(title, author, description, language):
    prompt = f"""
Based on the following book information, guess the most likely country of origin (where the author is from or the book was first published). 
Use the author's nationality, the book's language, and any clues in the description. For example:
- English language books by American authors are often from the United States.
- English language books by British authors are often from the United Kingdom.
- Russian language books are likely from Russia.
If uncertain, return null. Output a JSON object with a key "country" (string or null). Only output valid JSON.

Title: {title}
Author: {author}
Description: {description}
Language: {language}
"""
    async def _make_call():
        return await aclient.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=100
        )
    
    try:
        response = await call_with_retry(_make_call)
        content = response.choices[0].message.content
        parsed = extract_json(content)
        if parsed and isinstance(parsed, dict) and "country" in parsed:
            return parsed
        else:
            print(f"⚠️ Не удалось распарсить ответ для страны. Получено: {content[:300]}")
            return {"country": None}
    except Exception as e:
        safe_print_error(e)
        return {"country": None}

async def get_historical_links(title, description, characters=""):
    prompt = f"""
Analyze the book description and the list of characters (if provided) to identify any references to real historical events, periods, or historical figures.

- "events": list of strings (names of real historical events, e.g., "World War II", "French Revolution", "Napoleon's invasion of Russia in 1812")
- "figures": list of strings (names of real historical figures, e.g., "Napoleon Bonaparte", "Mahatma Gandhi", "Mikhail Kutuzov")
- "time_period": string or null (e.g., "19th century", "1920s", "Middle Ages")

CRITICAL RULES — YOU MUST FOLLOW THEM EXACTLY:

1. **STRICTLY EXCLUDE ALL FICTIONAL CHARACTERS.** This includes, but is not limited to:
   - Characters from novels: Pierre Bezukhov, Natasha Rostova, Prince Andrei Bolkonsky (War and Peace)
   - Characters from science fiction/fantasy: Paul Muad'Dib, Chani, Stilgar, Thufir Hawat, Wellington Yueh, Duncan Idaho (Dune series)
   - Characters from other fiction: Howard Roark, Dominique Francon (The Fountainhead), Dagny Taggart, John Galt (Atlas Shrugged)
   - Any character created by the author for a work of fiction, regardless of genre.

2. **SPECIAL RULE FOR SCIENCE FICTION AND FANTASY BOOKS:** If the book's genres include "Science Fiction", "Fantasy", "Space Opera", or similar, assume that ALL characters are fictional unless there is explicit, undeniable evidence that a character is a real historical person (e.g., a historical figure like Napoleon appears in an alternate history story). In such cases, include only that historical figure, not any invented characters.

3. **DO NOT include the book's author** unless the book is explicitly ABOUT that author as a historical figure (e.g., a biography of Leo Tolstoy). If the book is a work OF the author (like a novel), the author is NOT a historical figure in that context.

4. **Only include people who actually lived in history:** rulers, politicians, military leaders, philosophers, activists, scientists, artists, etc. If you are unsure whether a person is real, do a quick mental check: would they appear in a history book? If not, exclude them.

5. **If a list of characters is provided, you MUST evaluate each name individually.** For each character, ask: "Did this person exist in real history, or are they a fictional creation?" If they are fictional, exclude them. If they are real, include them.

6. **EXAMPLES OF CORRECT BEHAVIOR:**
   - For "War and Peace": Napoleon (real) → include; Mikhail Kutuzov (real) → include; Pierre Bezukhov (fictional) → exclude.
   - For "Hunters of Dune": Paul Muad'Dib (fictional) → exclude; Chani (fictional) → exclude; Stilgar (fictional) → exclude. All characters are fictional, so "figures" should be [].
   - For "The Kingdom of God Is Within You": Leo Tolstoy is the author, but the book is not about him as a historical figure → exclude; Mahatma Gandhi is mentioned as a real person influenced by the book → include.
   - For "Tolstoy Lied: A Love Story": Leo Tolstoy is mentioned as a historical figure (the book discusses his ideas) → include.

7. If no real historical figures are mentioned or listed, return an empty list [].

If a historical period is mentioned or implied (e.g., "set during the Napoleonic Wars"), provide it. If none, return null.

Return a JSON object with the keys "events", "figures", "time_period". Only output valid JSON.

Title: {title}
Description: {description}
Characters: {characters}
"""
    async def _make_call():
        return await aclient.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=600
        )
    
    try:
        response = await call_with_retry(_make_call)
        content = response.choices[0].message.content
        parsed = extract_json(content)
        if parsed and isinstance(parsed, dict):
            return {
                "events": parsed.get("events", []),
                "figures": parsed.get("figures", []),
                "time_period": parsed.get("time_period")
            }
        else:
            print(f"⚠️ Не удалось распарсить ответ для истории. Получено: {content[:300]}")
            return {"events": [], "figures": [], "time_period": None}
    except Exception as e:
        safe_print_error(e)
        return {"events": [], "figures": [], "time_period": None}

# ======================== ОБРАБОТКА КНИГИ ========================
async def process_book(book):
    title = book.get(COL_TITLE, "")
    print(f"🔄 Обрабатывается: {title}")
    author = book.get(COL_AUTHOR, "")
    description = book.get(COL_DESCRIPTION, "")
    language = book.get(COL_LANGUAGE, "")
    characters = book.get("characters", "")
    if pd.isna(characters):
        characters = ""
    
    country_task = asyncio.create_task(get_country(title, author, description, language))
    historical_task = asyncio.create_task(get_historical_links(title, description, characters))
    
    country_result = await country_task
    historical_result = await historical_task

    # Удаляем автора из historical_figures, если книга не является биографией
    if historical_result.get("figures"):
        author_name = book.get(COL_AUTHOR, "")
        # Проверяем жанры на наличие биографического жанра
        genres_field = book.get("genres", "")
        is_biography = False
        if isinstance(genres_field, str):
            genres_lower = genres_field.lower()
            if "biography" in genres_lower or "autobiography" in genres_lower:
                is_biography = True
        # Если книга не биография и автор указан, удаляем его из списка
        if not is_biography and author_name and pd.notna(author_name):
            author_lower = author_name.lower().strip()
            # Фильтруем список, оставляя только те имена, которые не совпадают с автором
            filtered = [fig for fig in historical_result["figures"] if fig.lower().strip() != author_lower]
            historical_result["figures"] = filtered
    
    enriched = book.copy()
    enriched["country"] = country_result.get("country")
    enriched["historical_events"] = historical_result.get("events", [])
    enriched["historical_figures"] = historical_result.get("figures", [])
    enriched["time_period"] = historical_result.get("time_period")
    return enriched

async def process_all(books, concurrency=CONCURRENCY):
    semaphore = asyncio.Semaphore(concurrency)
    async def bounded_process(book):
        async with semaphore:
            return await process_book(book)
    tasks = [bounded_process(book) for book in books]
    results = await asyncio.gather(*tasks)
    return results

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Файл {INPUT_CSV} не найден.")
        return
    try:
        df = pd.read_csv(INPUT_CSV, encoding=ENCODING)
    except Exception as e:
        print(f"❌ Ошибка чтения CSV: {e}")
        return
    print(f"📚 Загружено {len(df)} книг.")
    required_cols = [COL_TITLE, COL_AUTHOR, COL_DESCRIPTION, COL_LANGUAGE]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"❌ В CSV отсутствуют колонки: {missing}")
        return
    try:
        asyncio.run(test_api_key())
    except RuntimeError as e:
        print(e)
        return
    books = df.to_dict(orient="records")
    enriched_books = asyncio.run(process_all(books, concurrency=CONCURRENCY))
    with open(OUTPUT_JSON, "w", encoding=ENCODING) as f:
        json.dump(enriched_books, f, indent=2, ensure_ascii=False)
    print(f"✅ Обработка завершена. Результаты сохранены в {OUTPUT_JSON}")

if __name__ == "__main__":
    main()