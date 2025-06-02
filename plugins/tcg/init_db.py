import sqlite3
import os

DB_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, "tcg.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # drop old groups table so we can recreate with tcgplayer_id
    #c.execute("DROP TABLE IF EXISTS tcgplayer_groups;")
    c.execute("""
        CREATE TABLE IF NOT EXISTS tcg_sets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_name TEXT NOT NULL,
            tcgplayer_id INTEGER UNIQUE,
            mkm_id INTEGER UNIQUE,
            game TEXT NOT NULL,
            release_date TEXT
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS tcgplayer_groups (
            tcgplayer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            abbreviation TEXT,
            is_supplemental BOOLEAN,
            published_on TEXT,
            modified_on TEXT,
            category_id INTEGER NOT NULL
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS pokemon_sets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_name TEXT NOT NULL,
            release_date TEXT,
            booster_product_id INTEGER,
            booster_box_product_id INTEGER,
            group_id INTEGER,
            FOREIGN KEY (group_id) REFERENCES tcgplayer_groups(tcgplayer_id)
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku_id TEXT NOT NULL,
            variant TEXT,
            language TEXT,
            condition TEXT,
            market_price DECIMAL(10,2),
            quantity_sold INTEGER,
            low_sale_price DECIMAL(10,2),
            high_sale_price DECIMAL(10,2),
            bucket_start_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(sku_id, bucket_start_date)
        );
    """)
    conn.commit()
    conn.close()
    print(f"Initialized database at {DB_PATH}")

if __name__ == "__main__":
    init_db()
