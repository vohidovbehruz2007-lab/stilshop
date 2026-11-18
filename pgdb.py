import asyncio
from typing import Optional

import asyncpg

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS users (
        user_id    BIGINT PRIMARY KEY,
        full_name  TEXT,
        username   TEXT,
        joined_at  TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS categories (
        id    SERIAL PRIMARY KEY,
        name  TEXT NOT NULL
    )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS categories_name_lower ON categories (lower(name))",
    """CREATE TABLE IF NOT EXISTS products (
        id           SERIAL PRIMARY KEY,
        name         TEXT NOT NULL,
        category_id  INTEGER NOT NULL REFERENCES categories(id),
        description  TEXT NOT NULL,
        price        BIGINT NOT NULL,
        photo_id     TEXT NOT NULL,
        created_at   TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS orders (
        id            SERIAL PRIMARY KEY,
        user_id       BIGINT NOT NULL,
        product_id    INTEGER REFERENCES products(id) ON DELETE SET NULL,
        product_name  TEXT NOT NULL,
        price         BIGINT NOT NULL,
        phone         TEXT NOT NULL,
        comment       TEXT DEFAULT '',
        created_at    TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS settings (
        key    TEXT PRIMARY KEY,
        value  TEXT
    )""",
]

_PRODUCT_SELECT = """
SELECT p.id, p.name, p.description, p.price, p.photo_id,
       p.category_id, c.name AS category
FROM products p JOIN categories c ON c.id = p.category_id
"""


class PgDatabase:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def init(self) -> None:
        if not self.dsn:
            raise SystemExit("DATABASE_URL topilmadi! Render -> Environment ga qo'shing.")
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=1,
            max_size=5,
            statement_cache_size=0,
            max_inactive_connection_lifetime=60,
        )
        for stmt in SCHEMA:
            await self._call("execute", stmt)

    async def _call(self, method: str, sql: str, *args):
        last_error = None
        for _ in range(3):
            try:
                async with self.pool.acquire() as conn:
                    return await getattr(conn, method)(sql, *args)
            except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError) as e:
                last_error = e
                await asyncio.sleep(1)
        raise last_error

    # --- settings ---
    async def get_setting(self, key: str, default: str = "") -> str:
        row = await self._call("fetchrow", "SELECT value FROM settings WHERE key = $1", key)
        return row["value"] if row else default

    async def set_setting(self, key: str, value: str) -> None:
        await self._call(
            "execute",
            """INSERT INTO settings (key, value) VALUES ($1, $2)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""",
            key,
            value,
        )

    # --- users ---
    async def upsert_user(self, user_id: int, full_name: str, username: Optional[str]) -> None:
        await self._call(
            "execute",
            """INSERT INTO users (user_id, full_name, username) VALUES ($1, $2, $3)
               ON CONFLICT (user_id) DO UPDATE SET
               full_name = EXCLUDED.full_name, username = EXCLUDED.username""",
            user_id,
            full_name,
            username,
        )

    # --- categories / products ---
    async def get_or_create_category(self, name: str) -> int:
        name = name.strip()
        row = await self._call(
            "fetchrow", "SELECT id FROM categories WHERE lower(name) = lower($1)", name
        )
        if row:
            return row["id"]
        new_id = await self._call(
            "fetchval",
            """INSERT INTO categories (name) VALUES ($1)
               ON CONFLICT (lower(name)) DO NOTHING RETURNING id""",
            name,
        )
        if new_id:
            return new_id
        row = await self._call(
            "fetchrow", "SELECT id FROM categories WHERE lower(name) = lower($1)", name
        )
        return row["id"]

    async def get_categories(self):
        return await self._call(
            "fetch",
            """SELECT c.id, c.name, COUNT(p.id) AS cnt FROM categories c
               JOIN products p ON p.category_id = c.id
               GROUP BY c.id, c.name ORDER BY c.name""",
        )

    async def add_product(self, name, category_id, description, price, photo_id) -> int:
        return await self._call(
            "fetchval",
            """INSERT INTO products (name, category_id, description, price, photo_id)
               VALUES ($1, $2, $3, $4, $5) RETURNING id""",
            name,
            category_id,
            description,
            price,
            photo_id,
        )

    async def get_product(self, product_id: int):
        return await self._call("fetchrow", _PRODUCT_SELECT + " WHERE p.id = $1", product_id)

    async def get_products_by_category(self, category_id: int, limit: int, offset: int):
        return await self._call(
            "fetch",
            _PRODUCT_SELECT + " WHERE p.category_id = $1 ORDER BY p.id DESC LIMIT $2 OFFSET $3",
            category_id,
            limit,
            offset,
        )

    async def count_products_in_category(self, category_id: int) -> int:
        return await self._call(
            "fetchval", "SELECT COUNT(*) FROM products WHERE category_id = $1", category_id
        )

    async def get_all_products(self):
        return await self._call("fetch", _PRODUCT_SELECT + " ORDER BY c.name, p.id DESC")

    async def delete_product(self, product_id: int) -> bool:
        status = await self._call("execute", "DELETE FROM products WHERE id = $1", product_id)
        await self._call(
            "execute",
            "DELETE FROM categories WHERE id NOT IN (SELECT DISTINCT category_id FROM products)",
        )
        return int(status.split()[-1]) > 0

    # --- orders / stats ---
    async def create_order(self, user_id: int, product, phone: str, comment: str) -> int:
        return await self._call(
            "fetchval",
            """INSERT INTO orders (user_id, product_id, product_name, price, phone, comment)
               VALUES ($1, $2, $3, $4, $5, $6) RETURNING id""",
            user_id,
            product["id"],
            product["name"],
            product["price"],
            phone,
            comment,
        )

    async def get_stats(self) -> dict:
        row = await self._call(
            "fetchrow",
            """SELECT
                 (SELECT COUNT(*) FROM users)      AS users,
                 (SELECT COUNT(*) FROM products)   AS products,
                 (SELECT COUNT(*) FROM categories) AS categories,
                 (SELECT COUNT(*) FROM orders)     AS orders,
                 (SELECT COUNT(*) FROM orders WHERE created_at::date = CURRENT_DATE) AS orders_today,
                 (SELECT COALESCE(SUM(price), 0)::BIGINT FROM orders) AS revenue""",
        )
        return dict(row)
      
