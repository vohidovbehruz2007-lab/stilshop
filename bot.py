import asyncio
import logging
import os
import re
from contextlib import closing
from html import escape
from typing import Optional, Any
import sqlite3

from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiogram.utils import executor

# --- CONFIG ---
BOT_TOKEN = "9087065515:AAHlQQPUtprWZZT3Qe9JrAeX1RidGDcU2xM"
ADMIN_ID = 0  # O'z Telegram ID raqamingizni yozishingiz mumkin
DB_PATH = "stilshop.db"

SHOP_NAME = "StilShop"
PAGE_SIZE = 5

ABOUT_TEXT = (
    f"🏬 <b>{SHOP_NAME}</b>\n\n"
    "We offer stylish, high-quality products at fair prices "
    "with fast delivery across Uzbekistan.\n\n"
    "✅ Quality guaranteed\n🚚 Fast delivery\n💬 Friendly support"
)
CONTACTS_TEXT = (
    "📞 <b>Contacts</b>\n\n"
    "Telegram: @StilShopUz_bot\n"
    "Phone: +998 90 000 00 00\n"
    "Working hours: 09:00 – 21:00"
)

# --- DATABASE ---
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id    INTEGER PRIMARY KEY,
    full_name  TEXT,
    username   TEXT,
    joined_at  TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS categories (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name  TEXT NOT NULL UNIQUE COLLATE NOCASE
);
CREATE TABLE IF NOT EXISTS products (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    category_id  INTEGER NOT NULL REFERENCES categories(id),
    description  TEXT NOT NULL,
    price        INTEGER NOT NULL,
    photo_id     TEXT NOT NULL,
    created_at   TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS orders (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id       INTEGER NOT NULL,
    product_id    INTEGER REFERENCES products(id) ON DELETE SET NULL,
    product_name  TEXT NOT NULL,
    price         INTEGER NOT NULL,
    phone         TEXT NOT NULL,
    comment       TEXT DEFAULT '',
    created_at    TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

_PRODUCT_SELECT = """
SELECT p.id, p.name, p.description, p.price, p.photo_id,
       p.category_id, c.name AS category
FROM products p JOIN categories c ON c.id = p.category_id
"""

class Database:
    def __init__(self, path: str) -> None:
        self.path = path

    def _execute(self, sql: str, params: tuple = (), fetch: str = "none") -> Any:
        with closing(sqlite3.connect(self.path)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            cur = conn.execute(sql, params)
            if fetch == "one":
                result = cur.fetchone()
            elif fetch == "all":
                result = cur.fetchall()
            elif fetch == "count":
                result = cur.rowcount
            else:
                result = cur.lastrowid
            conn.commit()
            return result

    async def _run(self, sql: str, params: tuple = (), fetch: str = "none") -> Any:
        return await asyncio.to_thread(self._execute, sql, params, fetch)

    async def init(self) -> None:
        await asyncio.to_thread(lambda: sqlite3.connect(self.path).executescript(SCHEMA))

    async def upsert_user(self, user_id: int, full_name: str, username: Optional[str]) -> None:
        await self._run(
            """INSERT INTO users (user_id, full_name, username) VALUES (?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET
               full_name = excluded.full_name, username = excluded.username""",
            (user_id, full_name, username),
        )

    async def get_or_create_category(self, name: str) -> int:
        name = name.strip()
        row = await self._run("SELECT id FROM categories WHERE name = ?", (name,), "one")
        if row:
            return row["id"]
        return await self._run("INSERT INTO categories (name) VALUES (?)", (name,))

    async def get_categories(self, only_nonempty: bool = True):
        join = "JOIN" if only_nonempty else "LEFT JOIN"
        return await self._run(
            f"""SELECT c.id, c.name, COUNT(p.id) AS cnt FROM categories c
                {join} products p ON p.category_id = c.id
                GROUP BY c.id ORDER BY c.name""",
            fetch="all",
        )

    async def add_product(self, name: str, category_id: int, description: str, price: int, photo_id: str) -> int:
        return await self._run(
            """INSERT INTO products (name, category_id, description, price, photo_id)
               VALUES (?, ?, ?, ?, ?)""",
            (name, category_id, description, price, photo_id),
        )

    async def get_product(self, product_id: int):
        return await self._run(_PRODUCT_SELECT + " WHERE p.id = ?", (product_id,), "one")

    async def get_products_by_category(self, category_id: int, limit: int, offset: int):
        return await self._run(
            _PRODUCT_SELECT + " WHERE p.category_id = ? ORDER BY p.id DESC LIMIT ? OFFSET ?",
            (category_id, limit, offset), "all",
        )

    async def count_products_in_category(self, category_id: int) -> int:
        row = await self._run(
            "SELECT COUNT(*) AS n FROM products WHERE category_id = ?", (category_id,), "one"
        )
        return row["n"]

    async def get_all_products(self, limit: Optional[int] = None):
        sql = _PRODUCT_SELECT + " ORDER BY c.name, p.id DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return await self._run(sql, fetch="all")

    async def delete_product(self, product_id: int) -> bool:
        deleted = await self._run("DELETE FROM products WHERE id = ?", (product_id,), "count")
        await self._run(
            "DELETE FROM categories WHERE id NOT IN (SELECT DISTINCT category_id FROM products)"
        )
        return deleted > 0

    async def create_order(self, user_id: int, product, phone: str, comment: str) -> int:
        return await self._run(
            """INSERT INTO orders (user_id, product_id, product_name, price, phone, comment)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, product["id"], product["name"], product["price"], phone, comment),
        )

    async def get_stats(self) -> dict:
        row = await self._run(
            """SELECT
                 (SELECT COUNT(*) FROM users)      AS users,
                 (SELECT COUNT(*) FROM products)   AS products,
                 (SELECT COUNT(*) FROM categories) AS categories,
                 (SELECT COUNT(*) FROM orders)     AS orders,
                 (SELECT COUNT(*) FROM orders WHERE date(created_at) = date('now')) AS orders_today,
                 (SELECT COALESCE(SUM(price), 0) FROM orders) AS revenue""",
            fetch="one",
        )
        return dict(row)

# --- UTILS ---
def format_price(price: int) -> str:
    return f"{price:,}".replace(",", " ") + " so'm"

def parse_price(text: str) -> Optional[int]:
    digits = re.sub(r"[\s,']", "", text)
    if digits.isdigit() and int(digits) > 0:
        return int(digits)
    return None

def product_caption(p) -> str:
    desc = p["description"]
    if len(desc) > 700:
        desc = desc[:700] + "…"
    return (
        f"<b>{escape(p['name'])}</b>\n"
        f"<i>{escape(p['category'])}</i>\n\n"
        f"{escape(desc)}\n\n"
        f"💰 <b>{format_price(p['price'])}</b>"
    )

# --- STATES ---
class OrderStates(StatesGroup):
    phone = State()
    comment = State()

class AddProduct(StatesGroup):
    name = State()
    category = State()
    description = State()
    price = State()
    photo = State()

# --- KEYBOARDS ---
BTN_CATALOG = "🛍 Catalog"
BTN_ABOUT = "ℹ️ About Us"
BTN_CONTACTS = "📞 Contacts"
BTN_CANCEL = "❌ Cancel"
BTN_SKIP = "⏭ Skip"
BTN_SEND_PHONE = "📱 Share my number"

BTN_ADD = "➕ Add Product"
BTN_DELETE = "🗑 Delete Product"
BTN_ALL = "📦 View All Products"
BTN_STATS = "📊 Statistics"
BTN_EXIT_ADMIN = "⬅️ Exit admin"

def main_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(BTN_CATALOG))
    kb.row(KeyboardButton(BTN_ABOUT), KeyboardButton(BTN_CONTACTS))
    return kb

def phone_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, input_field_placeholder="or type: +998 90 123 45 67")
    kb.add(KeyboardButton(BTN_SEND_PHONE, request_contact=True))
    kb.add(KeyboardButton(BTN_CANCEL))
    return kb

def skip_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton(BTN_SKIP), KeyboardButton(BTN_CANCEL))
    return kb

def categories_kb(categories) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    for c in categories:
        kb.insert(InlineKeyboardButton(f"{c['name']} ({c['cnt']})", callback_data=f"cat:{c['id']}:0"))
    return kb

def product_kb(product_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🛒 Order", callback_data=f"order:{product_id}"))
    return kb

def pagination_kb(category_id: int, next_offset: Optional[int]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    if next_offset is not None:
        kb.add(InlineKeyboardButton("➡️ Show more", callback_data=f"cat:{category_id}:{next_offset}"))
    kb.add(InlineKeyboardButton("⬅️ Categories", callback_data="back:cats"))
    return kb

def admin_menu_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton(BTN_ADD), KeyboardButton(BTN_DELETE))
    kb.row(KeyboardButton(BTN_ALL), KeyboardButton(BTN_STATS))
    kb.add(KeyboardButton(BTN_EXIT_ADMIN))
    return kb

def cancel_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(BTN_CANCEL))
    return kb

def delete_list_kb(products) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for p in products:
        kb.add(InlineKeyboardButton(f"🗑 #{p['id']} {p['name'][:28]} — {format_price(p['price'])}", callback_data=f"del:{p['id']}"))
    return kb

def confirm_delete_kb(product_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("✅ Yes, delete", callback_data=f"delok:{product_id}"),
           InlineKeyboardButton("↩️ No", callback_data="delno"))
    return kb

# --- BOT SETUP ---
bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# --- HANDLERS: USER ---
async def cmd_start(message: Message) -> None:
    db: Database = dp["db"]
    await db.upsert_user(message.from_user.id, message.from_user.full_name, message.from_user.username)
    await message.answer(
        f"Assalomu alaykum, <b>{escape(message.from_user.first_name)}</b>!\n\n"
        f"Xush kelibsiz! Marhamat, katalogimizdagi mahsulotlarni ko'rishingiz mumkin.",
        reply_markup=main_menu(),
    )

async def cmd_about(message: Message) -> None:
    await message.answer(ABOUT_TEXT, reply_markup=main_menu())

async def cmd_contacts(message: Message) -> None:
    await message.answer(CONTACTS_TEXT, reply_markup=main_menu())

async def cmd_catalog(message: Message) -> None:
    db: Database = dp["db"]
    cats = await db.get_categories(only_nonempty=True)
    if not cats:
        await message.answer("😔 Hozircha katalogda mahsulotlar mavjud emas.")
        return
    await message.answer("📁 Kategoriyani tanlang:", reply_markup=categories_kb(cats))

async def show_category_products(callback: CallbackQuery) -> None:
    db: Database = dp["db"]
    _, cat_id_str, offset_str = callback.data.split(":")
    cat_id, offset = int(cat_id_str), int(offset_str)

    products = await db.get_products_by_category(cat_id, PAGE_SIZE, offset)
    if not products and offset == 0:
        await callback.answer("Bu kategoriyada mahsulotlar yo'q.", show_alert=True)
        return

    for p in products:
        await callback.message.answer_photo(
            photo=p["photo_id"],
            caption=product_caption(p),
            reply_markup=product_kb(p["id"]),
        )

    total = await db.count_products_in_category(cat_id)
    next_offset = offset + PAGE_SIZE if offset + PAGE_SIZE < total else None

    await callback.message.answer(
        f"Sahifa: {offset // PAGE_SIZE + 1} / max {(total - 1) // PAGE_SIZE + 1}",
        reply_markup=pagination_kb(cat_id, next_offset),
    )
    await callback.answer()

async def back_to_categories(callback: CallbackQuery) -> None:
    db: Database = dp["db"]
    cats = await db.get_categories(only_nonempty=True)
    await callback.message.edit_text("📁 Kategoriyani tanlang:", reply_markup=categories_kb(cats))
    await callback.answer()

async def start_order(callback: CallbackQuery, state: FSMContext) -> None:
    db: Database = dp["db"]
    product_id = int(callback.data.split(":")[1])
    product = await db.get_product(product_id)
    if not product:
        await callback.answer("Mahsulot topilmadi.", show_alert=True)
        return

    async with state.proxy() as data:
        data["product"] = dict(product)

    await OrderStates.phone.set()
    await callback.message.answer(
        "📱 Buyurtma berish uchun telefon raqamingizni yuboring (masalan: +998 90 123 45 67):",
        reply_markup=phone_kb(),
    )
    await callback.answer()

async def cancel_order(message: Message, state: FSMContext) -> None:
    await state.finish()
    await message.answer("Buyurtma bekor qilindi.", reply_markup=main_menu())

async def process_phone(message: Message, state: FSMContext) -> None:
    phone = message.contact.phone_number if message.contact else message.text
    if not phone or len(phone.strip()) < 7:
        await message.answer("Iltimos, to'g'ri telefon raqam kiriting yoki tugmani bosing.")
        return

    async with state.proxy() as data:
        data["phone"] = phone

    await OrderStates.comment.set()
    await message.answer("💬 Qo'shimcha izoh yoki manzil yozib qoldiring (yoki 'Skip' tugmasini bosing):", reply_markup=skip_kb())

async def process_comment(message: Message, state: FSMContext) -> None:
    db: Database = dp["db"]
    comment = "" if message.text == BTN_SKIP else message.text
    async with state.proxy() as data:
        product = data["product"]
        phone = data["phone"]

    user_id = message.from_user.id
    await db.create_order(user_id, product, phone, comment)
    await state.finish()

    await message.answer("✅ Buyurtmangiz muvaffaqiyatli qabul qilindi! Tez orada operatorlarimiz siz bilan bog'lanishadi.", reply_markup=main_menu())

    if ADMIN_ID:
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🚨 <b>Yangi buyurtma!</b>\n\n"
                f"🛍 Mahsulot: {product['name']}\n"
                f"💰 Narxi: {format_price(product['price'])}\n"
                f"👤 Mijoz: {message.from_user.full_name} (@{message.from_user.username or 'yoq'})\n"
                f"📱 Tel: {phone}\n"
                f"💬 Izoh: {comment}"
            )
        except Exception:
            pass

# --- HANDLERS: ADMIN ---
async def cmd_admin(message: Message) -> None:
    if ADMIN_ID and message.from_user.id != ADMIN_ID:
        return
    await message.answer("👑 Admin paneliga xush kelibsiz!", reply_markup=admin_menu_kb())

async def exit_admin(message: Message) -> None:
    await message.answer("Admin panelidan chiqildi.", reply_markup=main_menu())

async def admin_stats(message: Message) -> None:
    db: Database = dp["db"]
    s = await db.get_stats()
    text = (
        f"📊 <b>Statistika:</b>\n\n
