import json

def is_non_fiction(genres):
    """Возвращает True, если книга явно относится к документальным жанрам."""
    if not isinstance(genres, list):
        return False
    genres_lower = ' '.join([genres_str.lower() for genres_str in genres])
    non_fiction_indicators = [
        'biography', 'nonfiction', 'histor', 'science', 'reference',
        'philosophy', 'religion', 'cookbook'
    ]
    return any(word in genres_lower for word in non_fiction_indicators)

def main():
    # Загружаем данные
    with open('./data/processed/books_genre_final.json', 'r', encoding='utf-8') as f:
        books = json.load(f)

    cleaned_count = 0
    for book in books:
        genres = book.get('genres', '')
        if not is_non_fiction(genres):
            # Удаляем исторические данные
            if book.get('historical_events') or book.get('historical_figures'):
                cleaned_count += 1
            book['historical_events'] = []
            book['historical_figures'] = []
            # Опционально: book['time_period'] = None

    print(f"Очищено книг: {cleaned_count}")

    # Сохраняем результат
    with open('./data/processed/books_enriched_clean.json', 'w', encoding='utf-8') as f:
        json.dump(books, f, indent=2, ensure_ascii=False)

    print("Готово. Результат сохранён в books_enriched_clean.json")

if __name__ == "__main__":
    main()