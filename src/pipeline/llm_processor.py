import asyncio
import json
import logging
import os
import time
from typing import List, Dict, Any
from ollama import AsyncClient
import aiofiles
import re
import pandas as pd
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация
OLLAMA_MODEL = "llama3.1:8b"  # или phi3 для скорости
OLLAMA_HOST = "http://localhost:11434"
MAX_CONCURRENT = 3  # количество параллельных запросов
BATCH_SIZE = 100    # книг в батче
MAX_RETRIES = 3     # повторные попытки при ошибке
RETRY_DELAY = 2     # начальная задержка (сек)

INPUT_FILE = "./data/raw/goodreads_ds_sample.csv"           # исходный датасет
OUTPUT_DIR = "output"                     # папка для результатов
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "checkpoint.json")  # файл с номером последнего батча
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Список жанров
GENRE_LIST = [
    "Science Fiction", "Fantasy", "Mystery", "Thriller", "Romance",
    "Historical Fiction", "Biography", "Non-fiction", "Poetry", "Drama",
    "War", "Adventure", "Classic", "Satire", "Philosophical Fiction"
]

# Семафор для ограничения параллельных запросов
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

async def llm_call_ollama(prompt: str, system_prompt: str = None) -> str:
    """Асинхронный вызов Ollama с повторными попытками"""
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    for attempt in range(MAX_RETRIES):
        try:
            async with semaphore:
                response = await AsyncClient(host=OLLAMA_HOST).chat(
                    model=OLLAMA_MODEL,
                    messages=messages,
                    options={
                        "temperature": 0.0,
                        "num_predict": 300,
                    }
                )
            return response['message']['content'].strip()
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAY * (2 ** attempt)
                logger.info(f"Retrying in {wait} seconds...")
                await asyncio.sleep(wait)
            else:
                logger.error(f"All attempts failed for prompt: {prompt[:100]}...")
                return ""

async def enrich_book(book: Dict) -> Dict:
    """Обогащает одну книгу (возвращает копию с новыми полями)"""
    enriched = book.copy()
    enriched_id = book.get('id') or book.get('title', 'unknown')  # для отладки

    # Жанры
    if not enriched.get("genres"):
        system_prompt = f"""Ты классификатор жанров книг. Отвечай ТОЛЬКО списком жанров через запятую из списка: {', '.join(GENRE_LIST)}.
Примеры:
- "Эпопея о наполеоновских войнах..." → Historical Fiction, War, Classic
- "Роман-антиутопия о тоталитарном обществе" → Science Fiction, Drama
Если ни один жанр не подходит, верни "Unknown"."""
        prompt = f"Определи жанр(ы) книги по описанию. Описание: {enriched.get('description', '')[:500]}"
        genres_str = await llm_call_ollama(prompt, system_prompt)
        if genres_str:
            # Извлекаем только те жанры, которые есть в списке
            found = [g.strip() for g in genres_str.split(',') if g.strip() in GENRE_LIST]
            enriched["genres"] = found if found else ["Unknown"]
        else:
            enriched["genres"] = ["Unknown"]

    # Страна
    if not enriched.get("country"):
        system_prompt = "Ты определяешь страну происхождения книги. Отвечай ТОЛЬКО названием страны (одним словом) или 'null'."
        prompt = f"Предположи страну происхождения этой книги:\nНазвание: {enriched.get('title', '')}\nАвтор: {enriched.get('author', '')}\nОписание: {enriched.get('description', '')[:300]}"
        country = await llm_call_ollama(prompt, system_prompt)
        if country and country.lower() != "null":
            country = re.sub(r'[^\w\s-]', '', country).strip()
            enriched["country"] = country if country else None
        else:
            enriched["country"] = None

    # Исторические связи
    system_prompt = """Ты анализируешь текст и находишь связи с реальной историей. Отвечай ТОЛЬКО в формате JSON:
{"events": ["событие1", "событие2"], "figures": ["реальная личность1", "реальная личность2"]}
ВАЖНО: figures - только реальные исторические личности (не автор, не вымышленные). Если ничего нет - пустые списки."""
    prompt = f"Найди исторические события или личности в описании книги. Описание: {enriched.get('description', '')[:500]}"
    historical_str = await llm_call_ollama(prompt, system_prompt)
    if historical_str:
        try:
            start = historical_str.find('{')
            end = historical_str.rfind('}') + 1
            if start != -1 and end > start:
                enriched["historical"] = json.loads(historical_str[start:end])
            else:
                enriched["historical"] = {"events": [], "figures": []}
        except:
            enriched["historical"] = {"events": [], "figures": []}
    else:
        enriched["historical"] = {"events": [], "figures": []}

    return enriched

async def process_batch(books: List[Dict]) -> List[Dict]:
    """Обрабатывает батч книг параллельно"""
    tasks = [enrich_book(book) for book in books]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    processed = []
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            logger.error(f"Error processing book {i}: {res}")
            # Возвращаем исходную книгу с пометкой об ошибке
            err_book = books[i].copy()
            err_book["error"] = str(res)
            processed.append(err_book)
        else:
            processed.append(res)
    return processed

def load_books(file_path: str) -> List[Dict]:
    """
    Загружает книги из JSON или CSV файла.
    Возвращает список словарей.
    """
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.json':
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    elif ext == '.csv':
        df = pd.read_csv(file_path)
        # Приводим названия колонок к нижнему регистру и заменяем пробелы на подчёркивания
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        # Пытаемся определить стандартные поля (title, description, author)
        # Если нужных колонок нет, оставляем как есть – потом будем обращаться через .get()
        return df.to_dict(orient='records')
    else:
        raise ValueError(f"Неподдерживаемый формат файла: {ext}")

async def main():
    # Загружаем весь датасет
    logger.info(f"Загрузка данных из {INPUT_FILE}")
    all_books = load_books(INPUT_FILE)
    total_books = len(all_books)
    logger.info(f"Загружено {total_books} книг")

    # Определяем, с какого батча начинать (если есть чекпоинт)
    start_batch = 0
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            start_batch = json.load(f).get('last_batch', 0)
        logger.info(f"Найден чекпоинт: начинаем с батча {start_batch}")

    # Разбиваем на батчи
    batches = [all_books[i:i+BATCH_SIZE] for i in range(0, total_books, BATCH_SIZE)]
    logger.info(f"Всего батчей: {len(batches)}")

    # Основной цикл
    for batch_idx, batch in enumerate(batches):
        if batch_idx < start_batch:
            continue  # пропускаем уже обработанные батчи

        logger.info(f"Обработка батча {batch_idx+1}/{len(batches)} (книги {batch_idx*BATCH_SIZE+1}-{min((batch_idx+1)*BATCH_SIZE, total_books)})")
        start_time = time.time()

        # Обработка батча
        processed = await process_batch(batch)

        # Сохраняем результат батча в отдельный файл
        batch_file = os.path.join(OUTPUT_DIR, f"batch_{batch_idx:04d}.json")
        async with aiofiles.open(batch_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(processed, indent=2, ensure_ascii=False))

        # Обновляем чекпоинт
        checkpoint = {"last_batch": batch_idx + 1, "timestamp": time.time()}
        async with aiofiles.open(CHECKPOINT_FILE, 'w') as f:
            await f.write(json.dumps(checkpoint))

        elapsed = time.time() - start_time
        logger.info(f"Батч {batch_idx+1} завершён за {elapsed:.2f} сек. Примерно {elapsed/len(batch):.2f} сек/книга")

        # Небольшая пауза между батчами, чтобы дать системе остыть (опционально)
        await asyncio.sleep(1)

    # По окончании объединяем все батчи в один итоговый файл
    logger.info("Обработка завершена. Объединение результатов...")
    all_enriched = []
    for batch_idx in range(len(batches)):
        batch_file = os.path.join(OUTPUT_DIR, f"batch_{batch_idx:04d}.json")
        if os.path.exists(batch_file):
            with open(batch_file, 'r', encoding='utf-8') as f:
                all_enriched.extend(json.load(f))
    final_file = os.path.join(OUTPUT_DIR, "enriched_all.json")
    with open(final_file, 'w', encoding='utf-8') as f:
        json.dump(all_enriched, f, indent=2, ensure_ascii=False)
    logger.info(f"Итоговый файл сохранён: {final_file}")

if __name__ == "__main__":
    asyncio.run(main())
=======
import openai
import asyncio
from typing import List, Dict

async def llm_call(prompt: str, model="gpt-3.5-turbo") -> str:
    try:
        response = await openai.ChatCompletion.acreate(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"LLM error: {e}")
        return ""

async def enrich_book(book: Dict) -> Dict:
    # Определяем жанры, если нет
    if not book.get("genres"):
        prompt = f"Classify the genre of this book from the description. Choose from: {genre_list}. Description: {book['description']}"
        genres_str = await llm_call(prompt)
        book["genres"] = parse_genres(genres_str)
    
    # Определяем страну, если неизвестна
    if not book.get("country"):
        prompt = f"Guess the country of origin for this book. Title: {book['title']}, Author: {book['author']}, Description: {book['description']}. Return country name or null."
        country = await llm_call(prompt)
        book["country"] = country if country != "null" else None
    
    # Ищем исторические связи
    prompt = f"Identify any historical events or figures mentioned in this book description. Return JSON with keys 'events' and 'figures'. Description: {book['description']}"
    historical = await llm_call(prompt)
    book["historical"] = parse_json(historical)
    
    return book

async def process_batch(books: List[Dict]):
    tasks = [enrich_book(book) for book in books]
    return await asyncio.gather(*tasks)
