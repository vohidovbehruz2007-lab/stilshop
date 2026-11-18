import asyncio
import logging
import os
import re
import sqlite3
from contextlib import closing
from html import escape
from typing import Any, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiogram.utils import executor

logging.basicConfig(level=logging.INFO)

# ================= CONFIG =================
# Token Render -> Environment -> BOT_TOKEN ga qo'yiladi
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = [6401643766, 7306881346]
DB_PATH = os.getenv("DB_PATH", "stilshop.db")

SHOP_NAME = "StilShop"
PAGE_SIZE = 5

ABOUT_TEXT = (
    f"🏬 <b>{SHOP_NAME}</b>\n\n"
    "Biz sizga sifatli va zamonaviy mahsulotlarni hamyonbop narxlarda "
    "taklif qilamiz. O'zbekiston bo'ylab tez yetkazib berish.\n\n"
    "✅ Sifat kafolati\n🚚 Tez yetkazib berish\n💬 Do'stona xizmat"
)

# ================= DATABASE =================
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
CREATE TABLE IF NOT EXISTS settings (
    key    TEXT PRIMARY KEY,
    value  TEXT
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

    def _init_sync(self) -> None:
        with closing(sqlite3.connect(self.path)) as conn:
            conn.executescript(SCHEMA)
            conn.commit()

    async def init(self) -> None:
        await asyncio.to_thread(self._init_sync)

    # --- settings ---
    async def get_setting(self, key: str, default: str = "") -> str:
        row = await self._run("SELECT value FROM settings WHERE key = ?", (key,), "one")
        return row["value"] if row else default

    async def set_setting(self, key: str, value: str) -> None:
        await self._run(
            """INSERT INTO settings (key, value) VALUES (?, ?)
               ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
            (key, value),
        )

    # --- users ---
    async def upsert_user(self, user_id: int, full_name: str, username: Optional[str]) -> None:
        await self._run(
            """INSERT INTO users (user_id, full_name, username) VALUES (?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET
               full_name = excluded.full_name, username = excluded.username""",
            (user_id, full_name, username),
        )

    # --- categories / products ---
    async def get_or_create_category(self, name: str) -> int:
        name = name.strip()
        row = await self._run("SELECT id FROM categories WHERE name = ?", (name,), "one")
        if row:
            return row["id"]
        return await self._run("INSERT INTO categories (name) VALUES (?)", (name,))

    async def get_categories(self):
        return await self._run(
            """SELECT c.id, c.name, COUNT(p.id) AS cnt FROM categories c
               JOIN products p ON p.category_id = c.id
               GROUP BY c.id ORDER BY c.name""",
            fetch="all",
        )

    async def add_product(self, name, category_id, description, price, photo_id) -> int:
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
            (category_id, limit, offset),
            "all",
        )

    async def count_products_in_category(self, category_id: int) -> int:
        row = await self._run(
            "SELECT COUNT(*) AS n FROM products WHERE category_id = ?", (category_id,), "one"
        )
        return row["n"]

    async def get_all_products(self):
        return await self._run(_PRODUCT_SELECT + " ORDER BY c.name, p.id DESC", fetch="all")

    async def delete_product(self, product_id: int) -> bool:
        deleted = await self._run("DELETE FROM products WHERE id = ?", (product_id,), "count")
        await self._run(
            "DELETE FROM categories WHERE id NOT IN (SELECT DISTINCT category_id FROM products)"
        )
        return deleted > 0

    # --- orders / stats ---
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


# ================= UTILS =================
def format_price(price: int) -> str:
    return f"{price:,}".replace(",", " ") + " so'm"


def parse_price(text: str) -> Optional[int]:
    digits = re.sub(r"[\s,']", "", text or "")
    if digits.isdigit() and int(digits) > 0:
        return int(digits)
    return None


def valid_phone(text: str) -> bool:
    digits = re.sub(r"\D", "", text or "")
    return 9 <= len(digits) <= 15


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


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ================= STATES =================
class OrderStates(StatesGroup):
    phone = State()
    comment = State()


class AddProduct(StatesGroup):
    name = State()
    category = State()
    description = State()
    price = State()
    photo = State()


class SetShopPhone(StatesGroup):
    phone = State()


# ================= BUTTONS =================
BTN_CATALOG = "🛍 Tovarlar"
BTN_ABOUT = "ℹ️ Biz haqimizda"
BTN_CONTACTS = "📞 Telefon"
BTN_CANCEL = "❌ Bekor qilish"
BTN_SKIP = "⏭ O'tkazib yuborish"
BTN_SEND_PHONE = "📱 Raqamimni yuborish"

BTN_ADD = "➕ Tovar qo'shish"
BTN_DELETE = "🗑 Tovarni o'chirish"
BTN_ALL = "📦 Barcha tovarlar"
BTN_SET_PHONE = "☎️ Telefon raqamni o'zgartirish"
BTN_STATS = "📊 Statistika"
BTN_EXIT_ADMIN = "⬅️ Admin paneldan chiqish"


def main_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(BTN_CATALOG))
    kb.row(KeyboardButton(BTN_ABOUT), KeyboardButton(BTN_CONTACTS))
    return kb


def admin_menu_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(BTN_ADD))
    kb.row(KeyboardButton(BTN_DELETE), KeyboardButton(BTN_ALL))
    kb.add(KeyboardButton(BTN_SET_PHONE))
    kb.row(KeyboardButton(BTN_STATS), KeyboardButton(BTN_CATALOG))
    kb.add(KeyboardButton(BTN_EXIT_ADMIN))
    return kb


def menu_for(user_id: int) -> ReplyKeyboardMarkup:
    return admin_menu_kb() if is_admin(user_id) else main_menu()


def phone_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, input_field_placeholder="+998 90 123 45 67")
    kb.add(KeyboardButton(BTN_SEND_PHONE, request_contact=True))
    kb.add(KeyboardButton(BTN_CANCEL))
    return kb


def skip_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton(BTN_SKIP), KeyboardButton(BTN_CANCEL))
    return kb


def cancel_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(BTN_CANCEL))
    return kb


def categories_kb(categories) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    for c in categories:
        kb.insert(
            InlineKeyboardButton(f"{c['name']} ({c['cnt']})", callback_data=f"cat:{c['id']}:0")
        )
    return kb


def product_kb(product_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🛒 Buyurtma berish", callback_data=f"order:{product_id}"))
    return kb


def pagination_kb(category_id: int, next_offset: Optional[int]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    if next_offset is not None:
        kb.add(InlineKeyboardButton("➡️ Yana ko'rsatish", callback_data=f"cat:{category_id}:{next_offset}"))
    kb.add(InlineKeyboardButton("⬅️ Kategoriyalar", callback_data="back:cats"))
    return kb


# ================= BOT =================
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN topilmadi! Render -> Environment ga BOT_TOKEN qo'shing.")

bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())
db = Database(DB_PATH)


# ---------- cancel (har qanday holatda, eng birinchi) ----------
@dp.message_handler(text=BTN_CANCEL, state="*")
async def cancel_any(message: Message, state: FSMContext):
    await state.finish()
    await message.answer("Bekor qilindi.", reply_markup=menu_for(message.from_user.id))


# ---------- ADMIN: tovar qo'shish (FSM) ----------
@dp.message_handler(lambda m: is_admin(m.from_user.id), text=BTN_ADD, state="*")
async def add_start(message: Message, state: FSMContext):
    await state.finish()
    await message.answer("Yangi tovar nomini kiriting:", reply_markup=cancel_kb())
    await AddProduct.name.set()


@dp.message_handler(state=AddProduct.name, content_types=["text"])
async def add_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await message.answer("Kategoriyani kiriting (masalan: Kiyimlar):")
    await AddProduct.category.set()


@dp.message_handler(state=AddProduct.category, content_types=["text"])
async def add_category(message: Message, state: FSMContext):
    await state.update_data(category=message.text.strip())
    await message.answer("Tovar tavsifini kiriting:")
    await AddProduct.description.set()


@dp.message_handler(state=AddProduct.description, content_types=["text"])
async def add_description(message: Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
    await message.answer("Narxini raqamlarda kiriting (masalan: 150000):")
    await AddProduct.price.set()


@dp.message_handler(state=AddProduct.price, content_types=["text"])
async def add_price(message: Message, state: FSMContext):
    price = parse_price(message.text)
    if not price:
        await message.answer("Narxni to'g'ri raqam bilan kiriting (masalan: 150000):")
        return
    await state.update_data(price=price)
    await message.answer("Endi tovar rasmini (foto) yuboring:")
    await AddProduct.photo.set()


@dp.message_handler(state=AddProduct.photo, content_types=["photo"])
async def add_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    cat_id = await db.get_or_create_category(data["category"])
    await db.add_product(
        data["name"], cat_id, data["description"], data["price"], message.photo[-1].file_id
    )
    await state.finish()
    await message.answer("✅ Tovar qo'shildi!", reply_markup=admin_menu_kb())


@dp.message_handler(state=AddProduct.photo)
async def add_photo_wrong(message: Message):
    await message.answer("Iltimos, rasm (foto) yuboring.")
    # ---------- ADMIN: do'kon telefon raqamini o'zgartirish ----------
@dp.message_handler(lambda m: is_admin(m.from_user.id), text=BTN_SET_PHONE, state="*")
async def setphone_start(message: Message, state: FSMContext):
    await state.finish()
    current = await db.get_setting("shop_phone", "kiritilmagan")
    await message.answer(
        f"Hozirgi raqam: <b>{escape(current)}</b>\n\nYangi telefon raqamni yozing "
        "(masalan: +998 90 123 45 67):",
        reply_markup=cancel_kb(),
    )
    await SetShopPhone.phone.set()


@dp.message_handler(state=SetShopPhone.phone, content_types=["text"])
async def setphone_save(message: Message, state: FSMContext):
    if not valid_phone(message.text):
        await message.answer("Raqam noto'g'ri. Qaytadan kiriting (masalan: +998 90 123 45 67):")
        return
    await db.set_setting("shop_phone", message.text.strip())
    await state.finish()
    await message.answer("✅ Telefon raqam saqlandi!", reply_markup=admin_menu_kb())


# ---------- ADMIN: o'chirish, ko'rish, statistika ----------
@dp.message_handler(lambda m: is_admin(m.from_user.id), text=BTN_DELETE, state="*")
async def delete_list(message: Message, state: FSMContext):
    await state.finish()
    products = await db.get_all_products()
    if not products:
        await message.answer("O'chirish uchun tovarlar yo'q.", reply_markup=admin_menu_kb())
        return
    kb = InlineKeyboardMarkup(row_width=1)
    for p in products:
        kb.add(
            InlineKeyboardButton(
                f"❌ {p['name']} ({format_price(p['price'])})", callback_data=f"delprod:{p['id']}"
            )
        )
    await message.answer("O'chirmoqchi bo'lgan tovarni tanlang:", reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("delprod:"), state="*")
async def delete_confirm(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz!", show_alert=True)
        return
    await db.delete_product(int(callback.data.split(":")[1]))
    await callback.answer("Tovar o'chirildi!")
    await callback.message.edit_text("✅ Tovar o'chirildi. Yana o'chirish uchun tugmani qayta bosing.")


@dp.message_handler(lambda m: is_admin(m.from_user.id), text=BTN_ALL, state="*")
async def admin_all(message: Message, state: FSMContext):
    await state.finish()
    products = await db.get_all_products()
    if not products:
        await message.answer("Hozircha tovarlar yo'q.")
        return
    for p in products:
        await message.answer_photo(photo=p["photo_id"], caption=product_caption(p))


@dp.message_handler(lambda m: is_admin(m.from_user.id), text=BTN_STATS, state="*")
async def admin_stats(message: Message, state: FSMContext):
    await state.finish()
    s = await db.get_stats()
    await message.answer(
        "📊 <b>Statistika</b>\n\n"
        f"👥 Foydalanuvchilar: {s['users']}\n"
        f"📦 Tovarlar: {s['products']}\n"
        f"📁 Kategoriyalar: {s['categories']}\n"
        f"🛒 Jami buyurtmalar: {s['orders']}\n"
        f"📥 Bugungi buyurtmalar: {s['orders_today']}\n"
        f"💰 Tushum: {format_price(s['revenue'])}"
    )


@dp.message_handler(text=BTN_EXIT_ADMIN, state="*")
async def admin_exit(message: Message, state: FSMContext):
    await state.finish()
    await message.answer("Oddiy menyu.", reply_markup=main_menu())


@dp.message_handler(commands=["admin"], state="*")
async def cmd_admin(message: Message, state: FSMContext):
    await state.finish()
    if not is_admin(message.from_user.id):
        await message.answer("❌ Siz admin emassiz!")
        return
    await message.answer("👑 Admin paneliga xush kelibsiz!", reply_markup=admin_menu_kb())


# ---------- BUYURTMA (FSM) ----------
@dp.callback_query_handler(lambda c: c.data and c.data.startswith("order:"), state="*")
async def order_start(callback: CallbackQuery, state: FSMContext):
    product = await db.get_product(int(callback.data.split(":")[1]))
    if not product:
        await callback.answer("Tovar topilmadi.", show_alert=True)
        return
    await state.finish()
    await state.update_data(product=dict(product))
    await OrderStates.phone.set()
    await callback.message.answer(
        "📱 Buyurtma uchun telefon raqamingizni yuboring:", reply_markup=phone_kb()
    )
    await callback.answer()


@dp.message_handler(state=OrderStates.phone, content_types=["contact", "text"])
async def order_phone(message: Message, state: FSMContext):
    phone = message.contact.phone_number if message.contact else message.text
    if not valid_phone(phone):
        await message.answer("To'g'ri telefon raqam kiriting yoki tugmani bosing.")
        return
    await state.update_data(phone=phone.strip())
    await OrderStates.comment.set()
    await message.answer(
        "💬 Manzil yoki izoh yozing (yoki «O'tkazib yuborish» tugmasini bosing):",
        reply_markup=skip_kb(),
    )


@dp.message_handler(state=OrderStates.comment, content_types=["text"])
async def order_comment(message: Message, state: FSMContext):
    comment = "" if message.text == BTN_SKIP else message.text
    data = await state.get_data()
    product, phone = data["product"], data["phone"]
    await db.create_order(message.from_user.id, product, phone, comment)
    await state.finish()

    await message.answer(
        "✅ Buyurtmangiz qabul qilindi! Tez orada siz bilan bog'lanamiz.",
        reply_markup=menu_for(message.from_user.id),
    )

    username = f"@{message.from_user.username}" if message.from_user.username else "yo'q"
    text = (
        "🚨 <b>Yangi buyurtma!</b>\n\n"
        f"🛍 Tovar: {escape(product['name'])}\n"
        f"💰 Narxi: {format_price(product['price'])}\n"
        f"👤 Mijoz: {escape(message.from_user.full_name)} ({escape(username)})\n"
        f"📱 Tel: {escape(phone)}\n"
        f"💬 Izoh: {escape(comment) or '—'}"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception as e:
            logging.warning("Adminga yuborilmadi %s: %s", admin_id, e)


# ---------- OMMAVIY: start, tovarlar, telefon, biz haqimizda ----------
@dp.message_handler(commands=["start"], state="*")
async def cmd_start(message: Message, state: FSMContext):
    await state.finish()
    await db.upsert_user(
        message.from_user.id, message.from_user.full_name, message.from_user.username
    )
    await message.answer(
        f"Assalomu alaykum, <b>{escape(message.from_user.first_name)}</b>!\n\n"
        "Xush kelibsiz! Tovarlarimizni ko'rish uchun «🛍 Tovarlar» tugmasini bosing.",
        reply_markup=menu_for(message.from_user.id),
    )


@dp.message_handler(text=BTN_ABOUT, state="*")
async def cmd_about(message: Message):
    await message.answer(ABOUT_TEXT)


@dp.message_handler(text=BTN_CONTACTS, state="*")
async def cmd_contacts(message: Message):
    phone = await db.get_setting("shop_phone", "")
    if phone:
        text = f"📞 <b>Aloqa</b>\n\nTelefon: {escape(phone)}"
    else:
        text = "📞 Telefon raqam hozircha kiritilmagan."
    await message.answer(text)


@dp.message_handler(text=BTN_CATALOG, state="*")
async def cmd_catalog(message: Message, state: FSMContext):
    await state.finish()
    cats = await db.get_categories()
    if not cats:
        await message.answer("😔 Hozircha tovarlar yo'q.")
        return
    await message.answer("📁 Kategoriyani tanlang:", reply_markup=categories_kb(cats))


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("cat:"), state="*")
async def show_category(callback: CallbackQuery):
    _, cat_id_s, offset_s = callback.data.split(":")
    cat_id, offset = int(cat_id_s), int(offset_s)

    products = await db.get_products_by_category(cat_id, PAGE_SIZE, offset)
    if not products:
        await callback.answer("Bu kategoriyada tovar yo'q.", show_alert=True)
        return

    for p in products:
        await callback.message.answer_photo(
            photo=p["photo_id"], caption=product_caption(p), reply_markup=product_kb(p["id"])
        )

    total = await db.count_products_in_category(cat_id)
    next_offset = offset + PAGE_SIZE if offset + PAGE_SIZE < total else None
    await callback.message.answer(
        f"Sahifa: {offset // PAGE_SIZE + 1} / {(total - 1) // PAGE_SIZE + 1}",
        reply_markup=pagination_kb(cat_id, next_offset),
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "back:cats", state="*")
async def back_to_categories(callback: CallbackQuery):
    cats = await db.get_categories()
    if not cats:
        await callback.message.edit_text("😔 Hozircha tovarlar yo'q.")
    else:
        await callback.message.edit_text("📁 Kategoriyani tanlang:", reply_markup=categories_kb(cats))
    await callback.answer()


# ================= RUN =================
async def start_web_server() -> None:
    """Render Web Service port kutadi. Bu kichik server UptimeRobot uchun ham kerak."""
    from aiohttp import web

    async def health(_request):
        return web.Response(text="StilShop bot ishlayapti")

    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "10000"))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logging.info("Veb-server %s portda ishga tushdi", port)


async def keep_alive() -> None:
    """Har 10 daqiqada o'z manziliga so'rov yuboradi, Render uxlab qolmasin."""
    import aiohttp

    url = os.getenv("RENDER_EXTERNAL_URL")
    if not url:
        logging.info("RENDER_EXTERNAL_URL yo'q, keep-alive o'chirildi")
        return
    while True:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    logging.info("Keep-alive: %s", resp.status)
        except Exception as e:
            logging.warning("Keep-alive xato: %s", e)


async def on_startup(_: Dispatcher) -> None:
    await start_web_server()
    asyncio.create_task(keep_alive())
    await db.init()
    me = await bot.get_me()
    logging.info("Bot ishga tushdi: @%s", me.username)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
    
