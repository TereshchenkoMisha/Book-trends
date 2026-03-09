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