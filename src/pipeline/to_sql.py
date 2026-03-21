import pandas as pd
import sqlite3
import json
import os

if __name__ == "__main__":

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    json_path = os.path.join(BASE_DIR, "output_qwen", "b_0.json")
    db_path = os.path.join(BASE_DIR, "src", "pipeline", "books.db")

    if not os.path.exists(json_path):
        print(f"Файл {json_path} не найден")
        exit(1)

    with open(json_path, "r", encoding="utf-8") as f:
        text = f.read().replace("NaN", "null")
        data = json.loads(text)

    if isinstance(data, dict):
        data = [data]
    df = pd.DataFrame(data)

    def convert_to_json(x):
        if isinstance(x, (list, dict)):
            return json.dumps(x, ensure_ascii=False)
        return x

    df = df.apply(lambda col: col.map(convert_to_json))
    df = df.fillna("")
    df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

    conn = sqlite3.connect(db_path)
    df.to_sql("books", conn, if_exists="replace", index=False)
    conn.close()