import asyncio
import logging
import os
import json
import sqlite3
import aiohttp
import aiosqlite
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    Message, CallbackQuery, LabeledPrice, PreCheckoutQuery, ReplyKeyboardRemove, ChatJoinRequest
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters.callback_data import CallbackData
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from cachetools import TTLCache
from dotenv import load_dotenv

# Загрузка .env
load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = [int(x) for x in os.getenv('ADMIN_IDS', '').split(',') if x]
ADMIN_CONTACT = os.getenv('ADMIN_CONTACT', '@admin')
ADMIN_URL = ADMIN_CONTACT if ADMIN_CONTACT.startswith("http") else f"https://t.me/{ADMIN_CONTACT.replace('@', '')}"

CRYPTO_BOT_TOKEN = os.getenv('CRYPTO_BOT_TOKEN')
TON_WALLET = os.getenv('TON_WALLET', 'Укажите кошелек в .env')
TONCENTER_API_KEY = os.getenv('TONCENTER_API_KEY', '')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("private_channel_bot")

# --- УМНАЯ ЗАГРУЗКА КАНАЛОВ (JSON) ---
CHANNELS_FILE = 'channels.json'


def load_channels():
    if not os.path.exists(CHANNELS_FILE):
        channels_data = {}
        for i in range(1, 20):
            ch_id = os.getenv(f'CHANNEL_{i}_ID')
            if not ch_id:
                continue
            channels_data[str(i)] = {
                'id': ch_id,
                'name': os.getenv(f'CHANNEL_{i}_NAME', f'Канал {i}'),
                'description': "🌟 Эксклюзивный контент\n🔥 Ежедневные обновления\n💬 Поддержка 24/7",
                'link': os.getenv(f'CHANNEL_{i}_INVITE_LINK', ''),
                'prices': {
                    'monthly_rub': int(os.getenv(f'CHANNEL_{i}_MONTHLY_PRICE', 300)),
                    'lifetime_rub': int(os.getenv(f'CHANNEL_{i}_LIFETIME_PRICE', 1000)),
                    'monthly_stars': int(os.getenv(f'CHANNEL_{i}_MONTHLY_STARS', 150)),
                    'lifetime_stars': int(os.getenv(f'CHANNEL_{i}_LIFETIME_STARS', 500)),
                    'monthly_ton': float(os.getenv(f'CHANNEL_{i}_MONTHLY_TON', 1.5)),
                    'lifetime_ton': float(os.getenv(f'CHANNEL_{i}_LIFETIME_TON', 5.0)),
                }
            }
        if not channels_data:
            channels_data["1"] = {
                "id": os.getenv('CHANNEL_ID', ''),
                "name": os.getenv('CHANNEL_NAME', 'Приватный канал'),
                "description": "Описание вашего канала.\nВы можете менять его в файле channels.json!",
                "link": os.getenv('CHANNEL_INVITE_LINK', ''),
                "prices": {
                    "monthly_rub": int(os.getenv('MONTHLY_PRICE', 300)),
                    "lifetime_rub": int(os.getenv('LIFETIME_PRICE', 1000)),
                    "monthly_stars": int(os.getenv('MONTHLY_STARS', 150)),
                    "lifetime_stars": int(os.getenv('LIFETIME_STARS', 500)),
                    "monthly_ton": float(os.getenv('MONTHLY_TON_PRICE', 1.5)),
                    "lifetime_ton": float(os.getenv('LIFETIME_TON_PRICE', 5.0))
                }
            }
        with open(CHANNELS_FILE, 'w', encoding='utf-8') as f:
            json.dump(channels_data, f, ensure_ascii=False, indent=4)

    with open(CHANNELS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


CHANNELS = load_channels()
FIRST_CHANNEL_KEY = list(CHANNELS.keys())[0] if CHANNELS else "1"


# --- АДАПТЕРЫ DATETIME ---
def adapt_datetime(val):
    return val.isoformat()


def convert_datetime(val):
    return datetime.fromisoformat(val.decode())


sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("datetime", convert_datetime)


def parse_dt(val):
    if isinstance(val, str):
        return datetime.fromisoformat(val)
    return val


# --- БИЗНЕС-ЛОГИКА ---
def get_expiration_date(tariff_type: str) -> datetime:
    if tariff_type == 'monthly':
        return datetime.now() + timedelta(days=30)
    return datetime.now() + timedelta(days=365 * 10)


def apply_discount(price, discount_percent):
    if discount_percent <= 0:
        return price
    if isinstance(price, int):
        return max(1, int(price * (1 - discount_percent / 100)))
    return max(0.1, round(price * (1 - discount_percent / 100), 2))


async def preserve_promo_state(state: FSMContext):
    data = await state.get_data()
    promo = data.get('promo_code')
    discount = data.get('discount')
    await state.clear()
    if promo:
        await state.update_data(promo_code=promo, discount=discount)


# --- БАЗА ДАННЫХ ---
class AsyncDatabase:
    def __init__(self, db_path='database/private_channel.db'):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    @asynccontextmanager
    async def get_db(self):
        async with aiosqlite.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES) as db:
            db.row_factory = aiosqlite.Row
            yield db

    async def init_db(self):
        try:
            async with self.get_db() as db:
                await db.execute(
                    '''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT, registered_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
                await db.execute(
                    '''CREATE TABLE IF NOT EXISTS payments (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, username TEXT, first_name TEXT, channel_key TEXT DEFAULT '1', tariff_type TEXT NOT NULL, amount INTEGER NOT NULL, method TEXT, status TEXT DEFAULT 'pending', invoice_id TEXT, telegram_payment_charge_id TEXT, promo_code TEXT, abandoned_reminded BOOLEAN DEFAULT 0, access_granted BOOLEAN DEFAULT 0, admin_notified BOOLEAN DEFAULT 0, expires_at DATETIME, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, completed_at DATETIME)''')
                await db.execute(
                    '''CREATE TABLE IF NOT EXISTS access_log (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, username TEXT, first_name TEXT, channel_key TEXT DEFAULT '1', tariff_type TEXT NOT NULL, amount INTEGER NOT NULL, granted_by TEXT, expires_at DATETIME, kicked BOOLEAN DEFAULT 0, granted_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
                await db.execute(
                    '''CREATE TABLE IF NOT EXISTS renewal_reminders (id INTEGER PRIMARY KEY AUTOINCREMENT, access_log_id INTEGER NOT NULL, reminder_type TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
                await db.execute(
                    '''CREATE TABLE IF NOT EXISTS promo_codes (code TEXT PRIMARY KEY, discount_percent INTEGER NOT NULL, max_uses INTEGER DEFAULT 0, uses INTEGER DEFAULT 0, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
                await db.execute(
                    '''CREATE TABLE IF NOT EXISTS scheduled_broadcasts (id INTEGER PRIMARY KEY AUTOINCREMENT, target TEXT NOT NULL, from_chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL, send_at DATETIME NOT NULL, status TEXT DEFAULT 'pending', created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

                async with db.execute("PRAGMA table_info(access_log)") as cursor:
                    cols = [row['name'] for row in await cursor.fetchall()]
                    if 'kicked' not in cols:
                        await db.execute('ALTER TABLE access_log ADD COLUMN kicked BOOLEAN DEFAULT 0')
                    if 'channel_key' not in cols:
                        await db.execute(
                            f"ALTER TABLE access_log ADD COLUMN channel_key TEXT DEFAULT '{FIRST_CHANNEL_KEY}'")

                async with db.execute("PRAGMA table_info(payments)") as cursor:
                    cols = [row['name'] for row in await cursor.fetchall()]
                    if 'channel_key' not in cols:
                        await db.execute(
                            f"ALTER TABLE payments ADD COLUMN channel_key TEXT DEFAULT '{FIRST_CHANNEL_KEY}'")
                    if 'promo_code' not in cols:
                        await db.execute("ALTER TABLE payments ADD COLUMN promo_code TEXT")
                    if 'abandoned_reminded' not in cols:
                        await db.execute("ALTER TABLE payments ADD COLUMN abandoned_reminded BOOLEAN DEFAULT 0")

                await db.commit()
        except sqlite3.Error as e:
            logger.error(f"DB Init Error: {e}")

    async def get_user(self, user_id):
        async with self.get_db() as db:
            async with db.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None

    async def add_user(self, user):
        try:
            async with self.get_db() as db:
                await db.execute(
                    'INSERT OR IGNORE INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)',
                    (user.id, user.username, user.first_name, user.last_name or ''))
                await db.commit()
        except sqlite3.Error as e:
            logger.error(f"DB Error (add_user): {e}")

    async def create_payment(self, user_id, username, first_name, channel_key, tariff_type, amount, expires_at,
                             method='admin', promo_code=None):
        try:
            now = datetime.now()
            async with self.get_db() as db:
                cursor = await db.execute(
                    'INSERT INTO payments (user_id, username, first_name, channel_key, tariff_type, amount, method, expires_at, promo_code, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "pending", ?)',
                    (user_id, username, first_name, channel_key, tariff_type, amount, method, expires_at, promo_code,
                     now)
                )
                await db.commit()
                return cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"DB Error (create_payment): {e}")
            return None

    async def update_payment(self, payment_id, status=None, invoice_id=None, telegram_payment_charge_id=None):
        try:
            async with self.get_db() as db:
                if telegram_payment_charge_id:
                    await db.execute('UPDATE payments SET telegram_payment_charge_id = ? WHERE id = ?',
                                     (telegram_payment_charge_id, payment_id))
                if invoice_id:
                    await db.execute('UPDATE payments SET invoice_id = ? WHERE id = ?', (invoice_id, payment_id))
                if status:
                    completed_at = datetime.now() if status == 'completed' else None
                    await db.execute('UPDATE payments SET status = ?, completed_at = ? WHERE id = ?',
                                     (status, completed_at, payment_id))
                await db.commit()
        except sqlite3.Error as e:
            logger.error(f"DB Error (update_payment): {e}")

    async def mark_admin_notified(self, payment_id):
        try:
            async with self.get_db() as db:
                await db.execute('UPDATE payments SET admin_notified = 1 WHERE id = ?', (payment_id,))
                await db.commit()
        except sqlite3.Error:
            pass

    async def grant_access(self, payment_id, user_id, username, first_name, channel_key, tariff_type, amount,
                           expires_at, granted_by='admin'):
        try:
            async with self.get_db() as db:
                # 1. Засчитываем промокод
                async with db.execute('SELECT promo_code FROM payments WHERE id = ?', (payment_id,)) as cursor:
                    row = await cursor.fetchone()
                    if row and row['promo_code'] and row['promo_code'] != "WELCOME15":
                        await db.execute('UPDATE promo_codes SET uses = uses + 1 WHERE code = ?', (row['promo_code'],))

                # 2. Выдаем доступ
                await db.execute(
                    'INSERT INTO access_log (user_id, username, first_name, channel_key, tariff_type, amount, granted_by, expires_at, kicked) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)',
                    (user_id, username, first_name, channel_key, tariff_type, amount, granted_by, expires_at))

                # 3. Помечаем платеж
                await db.execute(
                    'UPDATE payments SET access_granted = 1, status = "completed", completed_at = ? WHERE id = ?',
                    (datetime.now(), payment_id))
                await db.commit()
        except sqlite3.Error as e:
            logger.error(f"DB Error (grant_access): {e}")

    async def get_payment_by_id(self, payment_id):
        async with self.get_db() as db:
            async with db.execute('SELECT * FROM payments WHERE id = ?', (payment_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None

    async def get_pending_payments(self):
        async with self.get_db() as db:
            async with db.execute("SELECT * FROM payments WHERE status = 'pending' ORDER BY created_at DESC") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def get_completed_unnotified_payments(self):
        async with self.get_db() as db:
            async with db.execute(
                    "SELECT * FROM payments WHERE status = 'completed' AND admin_notified = 0 AND method != 'admin' AND method != 'ton_direct'") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def get_user_active_accesses(self, user_id):
        async with self.get_db() as db:
            async with db.execute(
                    'SELECT * FROM access_log WHERE user_id = ? AND expires_at > ? AND kicked = 0 ORDER BY granted_at DESC',
                    (user_id, datetime.now())) as cursor:
                rows = await cursor.fetchall()
                accesses = {}
                for row in rows:
                    if row['channel_key'] not in accesses:
                        accesses[row['channel_key']] = dict(row)
                return accesses

    async def get_user_active_access_for_channel(self, user_id, channel_key):
        async with self.get_db() as db:
            async with db.execute(
                    'SELECT * FROM access_log WHERE user_id = ? AND channel_key = ? AND expires_at > ? AND kicked = 0 ORDER BY granted_at DESC LIMIT 1',
                    (user_id, channel_key, datetime.now())) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None

    async def get_stats(self):
        async with self.get_db() as db:
            async with db.execute('SELECT COUNT(*) FROM users') as c:
                total_users = (await c.fetchone())[0]
            async with db.execute('SELECT COUNT(*) FROM payments WHERE status = "completed"') as c:
                total_payments = (await c.fetchone())[0]

            now = datetime.now()
            async with db.execute('SELECT COUNT(DISTINCT user_id) FROM access_log WHERE expires_at > ? AND kicked = 0',
                                  (now,)) as c:
                active_users = (await c.fetchone())[0]
            async with db.execute(
                    "SELECT COUNT(DISTINCT user_id) FROM access_log WHERE expires_at > ? AND kicked = 0 AND tariff_type = 'monthly'",
                    (now,)) as c:
                active_monthly = (await c.fetchone())[0]
            async with db.execute(
                    "SELECT COUNT(DISTINCT user_id) FROM access_log WHERE expires_at > ? AND kicked = 0 AND tariff_type = 'lifetime'",
                    (now,)) as c:
                active_lifetime = (await c.fetchone())[0]

            async with db.execute('SELECT SUM(amount) FROM payments WHERE status = "completed"') as c:
                total_revenue = (await c.fetchone())[0] or 0
            async with db.execute(
                    "SELECT method, SUM(amount) as sum FROM payments WHERE status = 'completed' GROUP BY method") as c:
                rev_by_method = {row['method']: row['sum'] for row in await c.fetchall()}

            return {
                'total_users': total_users,
                'active_users': active_users,
                'inactive_users': total_users - active_users,
                'active_monthly': active_monthly,
                'active_lifetime': active_lifetime,
                'total_payments': total_payments,
                'total_revenue': total_revenue,
                'rev_by_method': rev_by_method
            }

    async def get_promo(self, code: str):
        async with self.get_db() as db:
            async with db.execute("SELECT * FROM promo_codes WHERE code = ?", (code.upper(),)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None

    async def add_promo(self, code: str, discount: int, max_uses: int):
        try:
            async with self.get_db() as db:
                await db.execute(
                    "INSERT OR REPLACE INTO promo_codes (code, discount_percent, max_uses) VALUES (?, ?, ?)",
                    (code.upper(), discount, max_uses))
                await db.commit()
            return True
        except sqlite3.Error:
            return False

    async def get_all_promos(self):
        async with self.get_db() as db:
            async with db.execute("SELECT * FROM promo_codes ORDER BY created_at DESC") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def delete_promo(self, code: str):
        async with self.get_db() as db:
            await db.execute("DELETE FROM promo_codes WHERE code = ?", (code.upper(),))
            await db.commit()

    async def add_scheduled_broadcast(self, target, from_chat_id, message_id, send_at):
        try:
            async with self.get_db() as db:
                await db.execute(
                    "INSERT INTO scheduled_broadcasts (target, from_chat_id, message_id, send_at) VALUES (?, ?, ?, ?)",
                    (target, from_chat_id, message_id, send_at))
                await db.commit()
            return True
        except sqlite3.Error:
            return False

    async def get_pending_broadcasts(self):
        async with self.get_db() as db:
            async with db.execute("SELECT * FROM scheduled_broadcasts WHERE status = 'pending' AND send_at <= ?",
                                  (datetime.now(),)) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def mark_broadcast_completed(self, b_id):
        async with self.get_db() as db:
            await db.execute("UPDATE scheduled_broadcasts SET status = 'completed' WHERE id = ?", (b_id,))
            await db.commit()

    async def get_expired_subscriptions(self):
        async with self.get_db() as db:
            async with db.execute("SELECT id, user_id, channel_key FROM access_log WHERE expires_at < ? AND kicked = 0",
                                  (datetime.now(),)) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def has_active_subscription(self, user_id, channel_key):
        async with self.get_db() as db:
            async with db.execute(
                    "SELECT 1 FROM access_log WHERE user_id = ? AND channel_key = ? AND expires_at > ? AND kicked = 0 LIMIT 1",
                    (user_id, channel_key, datetime.now())) as cursor:
                return await cursor.fetchone() is not None

    async def mark_as_kicked(self, log_id):
        try:
            async with self.get_db() as db:
                await db.execute("UPDATE access_log SET kicked = 1 WHERE id = ?", (log_id,))
                await db.commit()
        except sqlite3.Error:
            pass

    async def get_active_expiring_subs(self):
        async with self.get_db() as db:
            async with db.execute(
                    "SELECT id, user_id, channel_key, expires_at FROM access_log WHERE expires_at > ? AND kicked = 0 AND tariff_type != 'lifetime'",
                    (datetime.now(),)) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def has_reminder(self, access_log_id, reminder_type):
        async with self.get_db() as db:
            async with db.execute(
                    "SELECT 1 FROM renewal_reminders WHERE access_log_id = ? AND reminder_type = ? LIMIT 1",
                    (access_log_id, reminder_type)) as cursor:
                return await cursor.fetchone() is not None

    async def add_reminder(self, access_log_id, reminder_type):
        try:
            async with self.get_db() as db:
                await db.execute("INSERT INTO renewal_reminders (access_log_id, reminder_type) VALUES (?, ?)",
                                 (access_log_id, reminder_type))
                await db.commit()
        except sqlite3.Error:
            pass

    async def get_users_page(self, limit, offset, filter_type="all"):
        async with self.get_db() as db:
            if filter_type == "active":
                query = "SELECT u.user_id, u.first_name, a.tariff_type, a.granted_at, a.channel_key FROM users u JOIN access_log a ON u.user_id = a.user_id WHERE a.expires_at > ? AND a.kicked = 0 ORDER BY a.granted_at DESC LIMIT ? OFFSET ?"
                params = (datetime.now(), limit, offset)
            else:
                query = "SELECT user_id, first_name FROM users ORDER BY registered_at DESC LIMIT ? OFFSET ?"
                params = (limit, offset)
            async with db.execute(query, params) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def get_users_count(self, filter_type="all"):
        async with self.get_db() as db:
            if filter_type == "active":
                async with db.execute(
                        "SELECT COUNT(DISTINCT user_id) FROM access_log WHERE expires_at > ? AND kicked = 0",
                        (datetime.now(),)) as cursor:
                    return (await cursor.fetchone())[0]
            else:
                async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                    return (await cursor.fetchone())[0]

    async def extend_access(self, user_id, channel_key, days):
        access = await self.get_user_active_access_for_channel(user_id, channel_key)
        if not access:
            return False
        exp_date = parse_dt(access['expires_at'])
        new_expires = exp_date + timedelta(days=days)
        try:
            async with self.get_db() as db:
                await db.execute("UPDATE access_log SET expires_at = ? WHERE id = ?", (new_expires, access['id']))
                await db.commit()
            return True
        except sqlite3.Error:
            return False

    async def revoke_access(self, user_id, channel_key):
        try:
            async with self.get_db() as db:
                await db.execute(
                    "UPDATE access_log SET expires_at = ?, kicked = 1 WHERE user_id = ? AND channel_key = ? AND kicked = 0",
                    (datetime.now() - timedelta(days=1), user_id, channel_key))
                await db.commit()
        except sqlite3.Error:
            pass

    async def get_all_user_ids(self):
        async with self.get_db() as db:
            async with db.execute("SELECT user_id FROM users") as cursor:
                return [row[0] for row in await cursor.fetchall()]

    async def get_active_user_ids(self):
        async with self.get_db() as db:
            async with db.execute("SELECT DISTINCT user_id FROM access_log WHERE expires_at > ? AND kicked = 0",
                                  (datetime.now(),)) as cursor:
                return [row[0] for row in await cursor.fetchall()]

    async def get_inactive_user_ids(self):
        async with self.get_db() as db:
            async with db.execute(
                    "SELECT user_id FROM users WHERE user_id NOT IN (SELECT DISTINCT user_id FROM access_log WHERE expires_at > ? AND kicked = 0)",
                    (datetime.now(),)) as cursor:
                return [row[0] for row in await cursor.fetchall()]


# --- CRYPTOBOT API ---
class AsyncCryptoBot:
    def __init__(self, token):
        self.token = token
        self.base_url = "https://pay.crypt.bot/api"
        self.headers = {"Crypto-Pay-API-Token": token, "Content-Type": "application/json"}

    async def _request(self, method, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        async with aiohttp.ClientSession() as session:
            try:
                if method == "GET":
                    async with session.get(url, headers=self.headers, params=params, timeout=15) as resp:
                        res = await resp.json()
                else:
                    async with session.post(url, headers=self.headers, json=params, timeout=15) as resp:
                        res = await resp.json()

                if res.get('ok'):
                    return {'success': True, 'result': res['result']}
                else:
                    return {'success': False, 'error': res.get('error', 'Error')}
            except Exception as e:
                return {'success': False, 'error': str(e)}

    async def create_invoice(self, amount_rub, description, bot_username):
        payload = {
            "currency_type": "fiat",
            "fiat": "RUB",
            "amount": str(amount_rub),
            "description": description,
            "paid_btn_name": "openBot",
            "paid_btn_url": f"https://t.me/{bot_username}",
            "expires_in": 3600
        }
        res = await self._request("POST", "createInvoice", payload)
        if res['success']:
            return {'success': True, 'invoice_id': res['result']['invoice_id'], 'pay_url': res['result']['pay_url']}
        return res

    async def check_invoice(self, invoice_id):
        res = await self._request("GET", "getInvoices", {"invoice_ids": invoice_id})
        if res['success'] and res['result'].get('items'):
            return {'success': True, 'status': res['result']['items'][0]['status']}
        return {'success': False, 'error': 'Not found'}


# --- УМНЫЙ ПАРСЕР БЛОКЧЕЙНА TON ---
class AsyncTonAPI:
    def __init__(self, wallet, api_key=None):
        self.wallet = wallet
        self.api_key = api_key
        self.base_url = "https://toncenter.com/api/v2/getTransactions"
        self._cache = []
        self._last_fetch = 0

    async def get_recent_transactions(self):
        if not self.wallet or self.wallet.startswith('Укажите'):
            return []

        now = asyncio.get_event_loop().time()
        if now - self._last_fetch < 15:
            return self._cache

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        async with aiohttp.ClientSession() as session:
            try:
                params = {"address": self.wallet, "limit": 50}
                async with session.get(self.base_url, params=params, headers=headers, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('ok'):
                            self._cache = data.get('result', [])
                    elif resp.status == 429:
                        logger.warning("TON API Rate Limit (429). Ждем...")
            except asyncio.TimeoutError:
                logger.error("TON API Error: Timeout")
            except Exception as e:
                logger.error(f"TON API Error: {type(e).__name__} - {e}")
            finally:
                self._last_fetch = now

        return self._cache

    async def check_payment(self, payment_id: int, required_amount_ton: float):
        txs = await self.get_recent_transactions()
        expected_memo = f"pay-{payment_id}"

        required_nano = int(required_amount_ton * 1_000_000_000)
        min_nano = int(required_nano * 0.99)

        for tx in txs:
            in_msg = tx.get('in_msg', {})
            if not in_msg:
                continue

            memo = in_msg.get('message', '')
            value = int(in_msg.get('value', 0))

            if memo == expected_memo and value >= min_nano:
                return True
        return False


# --- CALLBACK DATA КЛАССЫ ---
class MenuCB(CallbackData, prefix="menu"): action: str


class SelectChanCB(CallbackData, prefix="schan"): ch_id: str


class TariffCB(CallbackData, prefix="tariff"): type: str; ch_id: str


class PayCB(CallbackData, prefix="pay"): method: str; type: str; ch_id: str


class CheckPayCB(CallbackData, prefix="check"): method: str; id: str


class AdminCB(CallbackData, prefix="admin"): action: str


class GrantCB(CallbackData, prefix="grant"): user_id: int; payment_id: int


class RejectCB(CallbackData, prefix="reject"): payment_id: int


class UsersPageCB(CallbackData, prefix="upage"): page: int; f_type: str


class UserProfileCB(CallbackData, prefix="uprof"): user_id: int


class UserActionCB(CallbackData, prefix="uact"): action: str; user_id: int; ch_id: str


class BroadcastCB(CallbackData, prefix="br"): target: str


class PromoCB(CallbackData, prefix="promo"): action: str; code: str = ""


# --- FSM СОСТОЯНИЯ ---
class BroadcastState(StatesGroup):
    waiting_for_target = State()
    waiting_for_message = State()
    confirm = State()
    waiting_for_date = State()


class SearchUserState(StatesGroup):
    waiting_for_id = State()


class UserPromoState(StatesGroup):
    waiting_for_code = State()


class AdminPromoState(StatesGroup):
    waiting_for_code = State()
    waiting_for_percent = State()
    waiting_for_uses = State()


# --- MIDDLEWARE ДЛЯ АНТИСПАМА ---
class RateLimitMiddleware(BaseMiddleware):
    def __init__(self):
        self.cache = TTLCache(maxsize=10000, ttl=1.5)

    async def __call__(self, handler, event, data):
        user_id = event.from_user.id
        if user_id in ADMIN_IDS:
            return await handler(event, data)
        if user_id in self.cache:
            if isinstance(event, CallbackQuery):
                try:
                    await event.answer("⏳ Не так быстро...", show_alert=False)
                except Exception:
                    pass
            return
        self.cache[user_id] = True
        return await handler(event, data)


# --- РОУТЕРЫ ---
user_router = Router()
admin_router = Router()


def get_main_kb(is_admin=False):
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Купить доступ", callback_data=MenuCB(action="buy"))
    kb.button(text="👤 Мой профиль", callback_data=MenuCB(action="my_access"))
    kb.button(text="❓ Помощь", callback_data=MenuCB(action="help"))
    #kb.button(text="🖥 Сервер от 49₽", url="https://rdp-onedash.ru/r/f1e21fd")

    if is_admin:
        kb.button(text="👑 Админ панель", callback_data=AdminCB(action="panel"))
        kb.adjust(1, 2, 1, 1)
    else:
        kb.adjust(1, 2, 1)
    return kb.as_markup()


# --- ЕДИНЫЙ ЦЕНТР ОБРАБОТКИ УСПЕШНЫХ ПЛАТЕЖЕЙ ---
async def process_successful_payment(bot: Bot, db: AsyncDatabase, payment_id: int):
    payment = await db.get_payment_by_id(payment_id)

    if not payment or payment['status'] == 'completed':
        return False

    expires_at = get_expiration_date(payment['tariff_type'])

    await db.grant_access(payment_id, payment['user_id'], payment['username'], payment['first_name'],
                          payment['channel_key'], payment['tariff_type'], payment['amount'], expires_at,
                          payment['method'])

    ch_info = CHANNELS.get(payment['channel_key'])
    link_sent = False

    if not ch_info or not ch_info['id']:
        try:
            await bot.send_message(payment['user_id'],
                                   f"🎉 <b>Доступ активирован!</b> Обратитесь к администратору за ссылкой.",
                                   reply_markup=get_main_kb(payment['user_id'] in ADMIN_IDS))
            link_sent = True
        except Exception:
            pass
    else:
        try:
            invite = await bot.create_chat_invite_link(chat_id=ch_info['id'], member_limit=1,
                                                       name=f"Purchase_{payment['user_id']}",
                                                       expire_date=datetime.now() + timedelta(days=1))
            await bot.send_message(payment['user_id'],
                                   f"🎉 <b>Ваш доступ к каналу «{ch_info['name']}» активирован!</b>\n👉 <b>Присоединяйтесь по индивидуальной ссылке:</b>\n{invite.invite_link}\n\n<i>⚠️ Ссылка одноразовая, не передавайте её никому.</i>",
                                   reply_markup=get_main_kb(payment['user_id'] in ADMIN_IDS))
            link_sent = True
        except Exception as e:
            logger.error(f"Invite error for channel {payment['channel_key']}: {e}")
            fallback_link = ch_info['link'] or "Обратитесь к администратору"
            try:
                await bot.send_message(payment['user_id'],
                                       f"🎉 <b>Ваш доступ к каналу «{ch_info['name']}» активирован!</b>\n👉 <b>Присоединяйтесь:</b>\n{fallback_link}",
                                       reply_markup=get_main_kb(payment['user_id'] in ADMIN_IDS))
                link_sent = True
            except Exception:
                pass

    if not link_sent:
        ch_name = ch_info.get('name', 'Неизвестный канал') if ch_info else 'Неизвестный канал'
        text = f"⚠️ <b>ВНИМАНИЕ! ПРОБЛЕМА С ДОСТАВКОЙ ССЫЛКИ</b>\n\nПользователь <code>{payment['user_id']}</code> успешно оплатил доступ к <b>{ch_name}</b>, но <b>заблокировал бота</b>.\nПожалуйста, свяжитесь с ним вручную!"
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, text)
            except Exception:
                pass

    t_name = "Месячный" if payment['tariff_type'] == 'monthly' else "НАВСЕГДА"
    ch_name = ch_info.get('name', 'Неизвестный канал') if ch_info else 'Неизвестный канал'
    promo_text = f"\n🎟 <b>Промокод:</b> {payment['promo_code']}" if payment.get('promo_code') else ""

    admin_text = (f"✅ <b>УСПЕШНЫЙ ПЛАТЕЖ ({payment['method'].upper()})</b>\n\n📢 <b>Канал:</b> {ch_name}\n"
                  f"👤 <b>Пользователь:</b> {payment['first_name']} (@{payment['username']})\n"
                  f"🆔 <b>ID:</b> <code>{payment['user_id']}</code>\n💰 <b>Тариф:</b> {t_name}\n⭐ <b>Сумма:</b> {payment['amount']}{promo_text}\n"
                  f"🆔 <b>ID платежа:</b> {payment['id']}\n\n✅ <b>Доступ выдан автоматически.</b>")

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, admin_text)
        except Exception:
            pass

    await db.mark_admin_notified(payment_id)
    return True


# --- АВТО-ОДОБРЕНИЕ ЗАЯВОК (JOIN REQUESTS) ---
@user_router.chat_join_request()
async def handle_join_request(update: ChatJoinRequest, db: AsyncDatabase):
    target_ch_key = None
    target_ch_name = "Приватный канал"
    for key, info in CHANNELS.items():
        if str(update.chat.id) == str(info['id']):
            target_ch_key = key
            target_ch_name = info['name']
            break

    if target_ch_key:
        has_sub = await db.has_active_subscription(update.from_user.id, target_ch_key)
        if has_sub:
            try:
                await update.approve()
            except Exception:
                pass
            try:
                await update.bot.send_message(update.from_user.id,
                                              f"✅ <b>Ваша заявка в «{target_ch_name}» одобрена!</b> Добро пожаловать.")
            except Exception:
                pass
        else:
            try:
                await update.decline()
            except Exception:
                pass
            try:
                await update.bot.send_message(update.from_user.id,
                                              f"❌ <b>Заявка отклонена.</b>\nУ вас нет активной подписки на «{target_ch_name}». Оформите её через меню бота.",
                                              reply_markup=get_main_kb())
            except Exception:
                pass


# --- ЮЗЕРСКАЯ ЧАСТЬ ---
@user_router.message(CommandStart())
async def start_cmd(message: Message, db: AsyncDatabase, state: FSMContext):
    await preserve_promo_state(state)
    await db.add_user(message.from_user)

    try:
        msg = await message.answer("Загрузка...", reply_markup=ReplyKeyboardRemove())
        await msg.delete()
    except Exception:
        pass

    args = message.text.split()
    if len(args) > 1 and args[1].startswith('promo_'):
        code = args[1].replace('promo_', '').upper()
        promo = await db.get_promo(code)
        if promo:
            if promo['max_uses'] > 0 and promo['uses'] >= promo['max_uses']:
                await state.update_data(promo_code="WELCOME15", discount=15)
                try:
                    await message.answer(
                        f"❌ <b>Лимит исчерпан!</b>\n\nПромокод <b>{code}</b> закончился.\n🎁 Но мы автоматически применили для вас утешительный промокод <b>WELCOME15</b> на скидку <b>15%</b>!")
                except Exception:
                    pass
            else:
                await state.update_data(promo_code=code, discount=promo['discount_percent'])
                try:
                    await message.answer(
                        f"✅ <b>Промокод {code} успешно применен!</b>\nСкидка {promo['discount_percent']}% будет действовать на вашу покупку.")
                except Exception:
                    pass
        else:
            try:
                await message.answer("❌ Промокод из ссылки не найден или недействителен.")
            except Exception:
                pass

    user = await db.get_user(message.from_user.id)
    lto_active = False
    if user:
        reg_time = parse_dt(user['registered_at'])
        time_passed = datetime.now() - reg_time
        if time_passed < timedelta(hours=24):
            lto_active = True
            hours_left = int(24 - time_passed.total_seconds() / 3600)
            data = await state.get_data()
            current_discount = data.get('discount', 0)
            if 15 > current_discount:
                await state.update_data(discount=15, promo_code="WELCOME15")

    text = f"🔮 <b>Добро пожаловать!</b>\n\nЗдесь вы можете приобрести доступ к нашим эксклюзивным закрытым каналам.\n"
    if lto_active:
        text += f"\n🎁 <b>Вам доступна приветственная скидка 15%!</b>\nОна сгорит через {hours_left} часов.\n"

    text += "\n👇 <b>Используйте меню ниже:</b>"

    try:
        await message.answer(text, reply_markup=get_main_kb(message.from_user.id in ADMIN_IDS))
    except Exception:
        pass


@user_router.callback_query(MenuCB.filter(F.action == "main"))
async def main_menu(call: CallbackQuery, state: FSMContext):
    await preserve_promo_state(state)
    try:
        await call.message.edit_text(f"🔮 <b>Главное меню</b>\n\n👇 <b>Выберите действие:</b>",
                                     reply_markup=get_main_kb(call.from_user.id in ADMIN_IDS))
    except Exception:
        pass


@user_router.callback_query(MenuCB.filter(F.action == "help"))
async def help_menu(call: CallbackQuery):
    text = f"❓ <b>Помощь</b>\n\nВыберите канал и тариф, оплатите удобным способом, и бот автоматически выдаст вам одноразовую ссылку.\n\n📞 <b>Поддержка:</b> <a href='{ADMIN_URL}'>Администратор</a>"
    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data=MenuCB(action="main")).as_markup()
    try:
        await call.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        pass


@user_router.callback_query(MenuCB.filter(F.action == "buy"))
@user_router.callback_query(F.data == "show_tariffs")
async def show_channels(call: CallbackQuery):
    kb = InlineKeyboardBuilder()
    for key, info in CHANNELS.items():
        kb.button(text=f"📢 {info['name']}", callback_data=SelectChanCB(ch_id=key))
    kb.button(text="🔙 Назад", callback_data=MenuCB(action="main"))
    kb.adjust(1)
    try:
        await call.message.edit_text("Выбор канала\n\n👇 <b>К какому каналу вы хотите приобрести доступ?</b>",
                                     reply_markup=kb.as_markup())
    except Exception:
        pass


@user_router.callback_query(PromoCB.filter(F.action == "enter"))
async def enter_promo(call: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data=MenuCB(action="buy")).as_markup()
    try:
        await call.message.edit_text("🎟 <b>Ввод промокода</b>\n\nОтправьте ваш промокод ответным сообщением:",
                                     reply_markup=kb)
    except Exception:
        pass
    await state.set_state(UserPromoState.waiting_for_code)


@user_router.message(UserPromoState.waiting_for_code)
async def process_user_promo(message: Message, state: FSMContext, db: AsyncDatabase):
    code = message.text.strip().upper()
    promo = await db.get_promo(code)
    kb = InlineKeyboardBuilder().button(text="🔙 К выбору канала", callback_data=MenuCB(action="buy")).as_markup()

    if not promo:
        try:
            await message.answer("❌ Промокод не найден или истек.", reply_markup=kb)
        except Exception:
            pass
        return

    if promo['max_uses'] > 0 and promo['uses'] >= promo['max_uses']:
        await state.update_data(promo_code="WELCOME15", discount=15)
        try:
            await message.answer(
                f"❌ <b>Лимит исчерпан!</b>\n\nК сожалению, количество активаций промокода <b>{code}</b> закончилось.\n\n🎁 Но не расстраивайтесь! Мы автоматически применили для вас утешительный промокод <b>WELCOME15</b> на скидку <b>15%</b>!",
                reply_markup=InlineKeyboardBuilder().button(text="Продолжить выбор",
                                                            callback_data=MenuCB(action="buy")).as_markup())
        except Exception:
            pass
        return

    await state.update_data(promo_code=code, discount=promo['discount_percent'])
    try:
        await message.answer(
            f"✅ <b>Промокод применен!</b>\nСкидка <b>{promo['discount_percent']}%</b> будет действовать на вашу следующую покупку.",
            reply_markup=InlineKeyboardBuilder().button(text="Продолжить выбор",
                                                        callback_data=MenuCB(action="buy")).as_markup())
    except Exception:
        pass


@user_router.callback_query(SelectChanCB.filter())
async def show_tariffs(call: CallbackQuery, callback_data: SelectChanCB, state: FSMContext, db: AsyncDatabase):
    ch_key = callback_data.ch_id
    ch_info = CHANNELS.get(ch_key)
    if not ch_info:
        try:
            await call.answer("Канал не найден", show_alert=True)
        except Exception:
            pass
        return

    data = await state.get_data()
    discount = data.get('discount', 0)
    promo_code = data.get('promo_code', '')

    if promo_code and promo_code != "WELCOME15":
        promo = await db.get_promo(promo_code)
        if not promo or (promo['max_uses'] > 0 and promo['uses'] >= promo['max_uses']):
            await state.update_data(promo_code=None, discount=0)
            discount = 0
            promo_code = ""
            try:
                await call.answer("⚠️ Ваш промокод больше недействителен (исчерпан лимит)", show_alert=True)
            except Exception:
                pass

    prices = ch_info['prices']
    m_rub, l_rub = prices['monthly_rub'], prices['lifetime_rub']
    m_stars, l_stars = prices['monthly_stars'], prices['lifetime_stars']

    promo_text = f"\n🎟 <i>Активен промокод <b>{promo_code}</b> (-{discount}%)</i>\n" if discount > 0 else ""
    text = f"🏆 <b>Тарифы: {ch_info['name']}</b>\n\n📖 <i>{ch_info['description']}</i>\n{promo_text}\n👇 <b>Выберите подходящий вариант:</b>"

    if discount > 0:
        btn_m = f"📅 Месяц: {apply_discount(m_stars, discount)}⭐ | {apply_discount(m_rub, discount)}₽ (вместо {m_rub}₽)"
        btn_l = f"🎉 НАВСЕГДА: {apply_discount(l_stars, discount)}⭐ | {apply_discount(l_rub, discount)}₽ (вместо {l_rub}₽)"
    else:
        btn_m = f"📅 Месячный - {m_stars} ⭐ ({m_rub} ₽)"
        btn_l = f"🎉 НАВСЕГДА - {l_stars} ⭐ ({l_rub} ₽)"

    kb = InlineKeyboardBuilder()
    kb.button(text=btn_m, callback_data=TariffCB(type="monthly", ch_id=ch_key))
    kb.button(text=btn_l, callback_data=TariffCB(type="lifetime", ch_id=ch_key))
    if discount == 0:
        kb.button(text="🎟 Ввести промокод", callback_data=PromoCB(action="enter"))
    kb.button(text="🔙 Выбор канала", callback_data=MenuCB(action="buy"))
    kb.adjust(1)

    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        pass


@user_router.callback_query(TariffCB.filter())
async def show_pay_methods(call: CallbackQuery, callback_data: TariffCB, crypto_bot: AsyncCryptoBot, state: FSMContext):
    t_type = callback_data.type
    ch_key = callback_data.ch_id
    ch_info = CHANNELS.get(ch_key)

    data = await state.get_data()
    discount = data.get('discount', 0)

    stars = apply_discount(
        ch_info['prices']['monthly_stars'] if t_type == 'monthly' else ch_info['prices']['lifetime_stars'], discount)
    price_rub = apply_discount(
        ch_info['prices']['monthly_rub'] if t_type == 'monthly' else ch_info['prices']['lifetime_rub'], discount)
    t_name = "Месячный доступ" if t_type == 'monthly' else "Доступ НАВСЕГДА"

    kb = InlineKeyboardBuilder()
    kb.button(text="💎 Прямой перевод TON (Автоматически)",
              callback_data=PayCB(method="ton_direct", type=t_type, ch_id=ch_key))
    if crypto_bot:
        kb.button(text="🪙 CryptoBot (Любая крипта)", callback_data=PayCB(method="crypto", type=t_type, ch_id=ch_key))
    kb.button(text=f"⭐ Telegram Stars ({stars} ⭐)", callback_data=PayCB(method="stars", type=t_type, ch_id=ch_key))
    kb.button(text="💳 Карта / СБП / Подарок Stars", callback_data=PayCB(method="admin", type=t_type, ch_id=ch_key))
    kb.button(text="🔙 К тарифам", callback_data=SelectChanCB(ch_id=ch_key))
    kb.adjust(1)

    try:
        await call.message.edit_text(
            f"💳 <b>Оплата: {ch_info['name']}</b>\n📦 Тариф: {t_name}\n💵 <b>Итого к оплате:</b> {price_rub} ₽ / {stars} ⭐\n\n👇 <b>Выберите удобный способ оплаты:</b>",
            reply_markup=kb.as_markup())
    except Exception:
        pass


# --- ОПЛАТА СБП / КАРТА / ЗВЕЗДЫ ---
@user_router.callback_query(PayCB.filter(F.method == "admin"))
async def pay_admin(call: CallbackQuery, callback_data: PayCB, db: AsyncDatabase, state: FSMContext):
    t_type = callback_data.type
    ch_key = callback_data.ch_id
    ch_info = CHANNELS.get(ch_key)

    data = await state.get_data()
    discount = data.get('discount', 0)
    promo_code = data.get('promo_code')

    stars = apply_discount(
        ch_info['prices']['monthly_stars'] if t_type == 'monthly' else ch_info['prices']['lifetime_stars'], discount)
    price = apply_discount(
        ch_info['prices']['monthly_rub'] if t_type == 'monthly' else ch_info['prices']['lifetime_rub'], discount)
    t_name = "Месячный доступ" if t_type == 'monthly' else "Доступ НАВСЕГДА"

    expires_at = get_expiration_date(t_type)
    payment_id = await db.create_payment(call.from_user.id, call.from_user.username, call.from_user.first_name, ch_key,
                                         t_type, stars, expires_at, 'admin', promo_code)

    kb = InlineKeyboardBuilder()
    kb.button(text="📨 Написать администратору", url=ADMIN_URL)
    kb.button(text="🔄 Проверить", callback_data=CheckPayCB(method="admin", id=str(payment_id)))
    kb.button(text="🔙 Назад", callback_data=TariffCB(type=t_type, ch_id=ch_key))
    kb.adjust(1)

    text = f"""👤 <b>Оплата через Администратора</b>

📢 <b>Канал:</b> {ch_info['name']}
💰 <b>Тариф:</b> {t_name}
💵 <b>Сумма:</b> {price} ₽ или {stars} ⭐
🆔 <b>ID платежа:</b> <code>{payment_id}</code>

📋 <b>Доступные способы:</b>
1️⃣ <b>Подарок за звезды</b> (Учитывайте комиссию Telegram)
2️⃣ <b>Оплата по СБП</b> (Без комиссии)
3️⃣ <b>Оплата картой</b> (Возможна комиссия банка)

📝 <b>Инструкция:</b>
1. Напишите администратору: {ADMIN_CONTACT}
2. Отправьте ему ID платежа: <code>{payment_id}</code>
3. Переведите средства удобным способом и пришлите чек.
4. После проверки вы будете добавлены в канал."""

    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        pass

    promo_text = f"\n🎟 <b>Промокод:</b> {promo_code}" if promo_code else ""
    admin_text = f"🔔 <b>НОВЫЙ ПЛАТЕЖ (КАРТА/СБП)</b>\n\n📢 <b>Канал:</b> {ch_info['name']}\n👤 <b>Пользователь:</b> {call.from_user.first_name} (@{call.from_user.username})\n💰 <b>Тариф:</b> {t_name}\n💵 <b>К оплате:</b> {price} ₽{promo_text}\n🆔 <b>ID платежа:</b> {payment_id}"
    admin_kb = InlineKeyboardBuilder()
    admin_kb.button(text="✅ Выдать", callback_data=GrantCB(user_id=call.from_user.id, payment_id=payment_id))
    admin_kb.button(text="❌ Отклонить", callback_data=RejectCB(payment_id=payment_id))

    for admin_id in ADMIN_IDS:
        try:
            await call.bot.send_message(admin_id, admin_text, reply_markup=admin_kb.as_markup())
        except Exception:
            pass


# --- АВТОМАТИЧЕСКАЯ ПРЯМАЯ ОПЛАТА TON ---
@user_router.callback_query(PayCB.filter(F.method == "ton_direct"))
async def pay_ton_direct(call: CallbackQuery, callback_data: PayCB, db: AsyncDatabase, state: FSMContext):
    t_type = callback_data.type
    ch_key = callback_data.ch_id
    ch_info = CHANNELS.get(ch_key)

    data = await state.get_data()
    discount = data.get('discount', 0)
    promo_code = data.get('promo_code')

    ton_price = apply_discount(
        ch_info['prices']['monthly_ton'] if t_type == 'monthly' else ch_info['prices']['lifetime_ton'], discount)
    t_name = "Месячный доступ" if t_type == 'monthly' else "Доступ НАВСЕГДА"

    expires_at = get_expiration_date(t_type)
    payment_id = await db.create_payment(call.from_user.id, call.from_user.username, call.from_user.first_name, ch_key,
                                         t_type, ton_price, expires_at, 'ton_direct', promo_code)

    memo = f"pay-{payment_id}"
    amount_nano = int(ton_price * 1_000_000_000)
    pay_url = f"ton://transfer/{TON_WALLET}?amount={amount_nano}&text={memo}"

    kb = InlineKeyboardBuilder()
    kb.button(text=f"💎 Оплатить {ton_price} TON", url=pay_url)
    kb.button(text="🔄 Проверить оплату", callback_data=CheckPayCB(method="ton_direct", id=str(payment_id)))
    kb.button(text="🔙 Отмена", callback_data=TariffCB(type=t_type, ch_id=ch_key))
    kb.adjust(1)

    text = f"""💎 <b>Прямая оплата TON (Автоматически)</b>

📢 <b>Канал:</b> {ch_info['name']}
💰 <b>Тариф:</b> {t_name}
💵 <b>Сумма к оплате:</b> <code>{ton_price}</code> TON

Нажмите кнопку <b>«Оплатить»</b> ниже. У вас откроется кошелек (Tonkeeper, Wallet и т.д.) с уже заполненным адресом, суммой и комментарием.

⚠️ <b>Если кнопка не работает (например, с ПК):</b>
Переведите ровно <code>{ton_price}</code> TON на кошелек:
<code>{TON_WALLET}</code>
Обязательно укажите комментарий (Memo): <code>{memo}</code>

<i>После оплаты нажмите «Проверить оплату», и бот сам найдет ваш перевод в блокчейне!</i>"""
    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        pass


@user_router.callback_query(CheckPayCB.filter(F.method == "ton_direct"))
async def check_ton_direct(call: CallbackQuery, callback_data: CheckPayCB, db: AsyncDatabase, ton_api: AsyncTonAPI,
                           state: FSMContext):
    payment_id = int(callback_data.id)
    payment = await db.get_payment_by_id(payment_id)

    if not payment:
        try:
            await call.answer("❌ Платеж не найден", show_alert=True)
        except Exception:
            pass
        return

    if payment['status'] == 'completed':
        try:
            await call.message.edit_text("✅ <b>Доступ уже выдан!</b>",
                                         reply_markup=get_main_kb(call.from_user.id in ADMIN_IDS))
        except Exception:
            pass
        return

    is_paid = await ton_api.check_payment(payment_id, payment['amount'])

    if is_paid:
        await process_successful_payment(call.bot, db, payment_id)
        await state.clear()
    else:
        try:
            await call.answer("⏳ Платеж пока не найден в блокчейне. Подождите пару минут и нажмите еще раз.",
                              show_alert=True)
        except Exception:
            pass


# --- ОПЛАТА STARS И CRYPTOBOT ---
@user_router.callback_query(PayCB.filter(F.method == "stars"))
async def pay_stars(call: CallbackQuery, callback_data: PayCB, db: AsyncDatabase, state: FSMContext):
    t_type = callback_data.type
    ch_key = callback_data.ch_id
    ch_info = CHANNELS.get(ch_key)

    data = await state.get_data()
    discount = data.get('discount', 0)
    promo_code = data.get('promo_code')

    stars = apply_discount(
        ch_info['prices']['monthly_stars'] if t_type == 'monthly' else ch_info['prices']['lifetime_stars'], discount)
    t_name = "Месячный доступ" if t_type == 'monthly' else "Доступ НАВСЕГДА"

    expires_at = get_expiration_date(t_type)
    payment_id = await db.create_payment(call.from_user.id, call.from_user.username, call.from_user.first_name, ch_key,
                                         t_type, stars, expires_at, 'stars', promo_code)

    try:
        await call.bot.send_invoice(chat_id=call.message.chat.id, title=f"{ch_info['name']} - {t_name}",
                                    description=f"Доступ к каналу '{ch_info['name']}'", payload=f"payment_{payment_id}",
                                    provider_token="", currency="XTR",
                                    prices=[LabeledPrice(label=t_name, amount=stars)])
        await call.message.delete()
    except Exception as e:
        logger.error(f"Stars invoice error: {e}")


@user_router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery, db: AsyncDatabase):
    if query.invoice_payload.startswith("payment_"):
        payment = await db.get_payment_by_id(int(query.invoice_payload.split("_")[1]))
        if payment and payment['status'] == 'pending' and query.total_amount == payment['amount']:
            try:
                return await query.answer(ok=True)
            except Exception:
                pass
    try:
        await query.answer(ok=False, error_message="Ошибка платежа. Обратитесь в поддержку.")
    except Exception:
        pass


@user_router.message(F.successful_payment)
async def successful_payment(message: Message, db: AsyncDatabase, state: FSMContext):
    payment_id = int(message.successful_payment.invoice_payload.split("_")[1])
    await db.update_payment(payment_id, status='pending',
                            telegram_payment_charge_id=message.successful_payment.telegram_payment_charge_id)
    await process_successful_payment(message.bot, db, payment_id)
    await state.clear()


@user_router.callback_query(PayCB.filter(F.method == "crypto"))
async def pay_crypto(call: CallbackQuery, callback_data: PayCB, db: AsyncDatabase, crypto_bot: AsyncCryptoBot,
                     state: FSMContext):
    t_type = callback_data.type
    ch_key = callback_data.ch_id
    ch_info = CHANNELS.get(ch_key)

    data = await state.get_data()
    discount = data.get('discount', 0)
    promo_code = data.get('promo_code')

    price = apply_discount(
        ch_info['prices']['monthly_rub'] if t_type == 'monthly' else ch_info['prices']['lifetime_rub'], discount)
    t_name = "Месячный доступ" if t_type == 'monthly' else "Доступ НАВСЕГДА"

    expires_at = get_expiration_date(t_type)
    payment_id = await db.create_payment(call.from_user.id, call.from_user.username, call.from_user.first_name, ch_key,
                                         t_type, price, expires_at, 'cryptobot', promo_code)
    bot_info = await call.bot.get_me()

    res = await crypto_bot.create_invoice(price, f"{t_name} в {ch_info['name']}", bot_info.username)
    if res['success']:
        await db.update_payment(payment_id, status='pending', invoice_id=str(res['invoice_id']))
        kb = InlineKeyboardBuilder()
        kb.button(text="🪙 Выбрать крипту и Оплатить", url=res['pay_url'])
        kb.button(text="🔄 Проверить", callback_data=CheckPayCB(method="crypto", id=str(res['invoice_id'])))
        kb.button(text="🔙 Назад", callback_data=TariffCB(type=t_type, ch_id=ch_key))
        kb.adjust(1)
        try:
            await call.message.edit_text(
                f"🪙 <b>Оплата CryptoBot</b>\n\n📢 <b>Канал:</b> {ch_info['name']}\n💰 <b>Тариф:</b> {t_name}\n💵 <b>К оплате:</b> {price} ₽\n\n<i>Перейдите по ссылке, чтобы выбрать удобную криптовалюту и оплатить по текущему курсу.</i>",
                reply_markup=kb.as_markup())
        except Exception:
            pass
    else:
        try:
            await call.answer("❌ Ошибка создания счета", show_alert=True)
        except Exception:
            pass


@user_router.callback_query(CheckPayCB.filter(F.method == "crypto"))
async def check_crypto(call: CallbackQuery, callback_data: CheckPayCB, db: AsyncDatabase, crypto_bot: AsyncCryptoBot,
                       state: FSMContext):
    res = await crypto_bot.check_invoice(callback_data.id)
    if res['success']:
        if res['status'] == 'paid':
            async with db.get_db() as conn:
                async with conn.execute('SELECT id FROM payments WHERE invoice_id = ?', (callback_data.id,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        await process_successful_payment(call.bot, db, row['id'])
                        await state.clear()
                        try:
                            await call.message.delete()
                        except Exception:
                            pass
        elif res['status'] == 'active':
            try:
                await call.answer("⏳ Счет еще не оплачен", show_alert=True)
            except Exception:
                pass
        else:
            try:
                await call.message.edit_text("❌ <b>Счет истек или отменен</b>",
                                             reply_markup=get_main_kb(call.from_user.id in ADMIN_IDS))
            except Exception:
                pass


@user_router.callback_query(CheckPayCB.filter(F.method == "admin"))
async def check_admin(call: CallbackQuery, callback_data: CheckPayCB, db: AsyncDatabase):
    payment = await db.get_payment_by_id(int(callback_data.id))
    if not payment:
        try:
            return await call.answer("❌ Платеж не найден")
        except Exception:
            return

    if payment['status'] == 'completed':
        try:
            await call.message.edit_text("✅ <b>Доступ уже выдан!</b>",
                                         reply_markup=get_main_kb(call.from_user.id in ADMIN_IDS))
        except Exception:
            pass
    elif payment['status'] == 'pending':
        try:
            await call.answer("⏳ Ожидание подтверждения", show_alert=True)
        except Exception:
            pass
    else:
        try:
            await call.message.edit_text("❌ <b>Платеж отклонен</b>",
                                         reply_markup=get_main_kb(call.from_user.id in ADMIN_IDS))
        except Exception:
            pass


# --- ЕДИНЫЙ ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ ---
@user_router.callback_query(MenuCB.filter(F.action == "my_access"))
@user_router.callback_query(F.data == "myaccess")
async def my_access(call: CallbackQuery, db: AsyncDatabase):
    accesses = await db.get_user_active_accesses(call.from_user.id)
    kb = InlineKeyboardBuilder()

    text = "👤 <b>Ваш профиль (Сводка)</b>\n\n"
    has_any = False

    for ch_key, ch_info in CHANNELS.items():
        acc = accesses.get(ch_key)
        text += f"📢 <b>{ch_info['name']}</b>\n"

        if acc:
            has_any = True
            exp_date = parse_dt(acc['expires_at'])
            if acc['tariff_type'] == 'lifetime':
                text += "✅ Активен (НАВСЕГДА)\n"
            else:
                text += f"✅ Активен до {exp_date.strftime('%d.%m.%Y')}\n"
                kb.button(text=f"🔄 Продлить: {ch_info['name']}", callback_data=TariffCB(type="monthly", ch_id=ch_key))
        else:
            text += "❌ Нет доступа\n"
        text += "\n"

    if not has_any:
        text += "<i>У вас пока нет активных подписок. Выберите канал в главном меню.</i>"

    kb.button(text="👨‍💻 Написать в поддержку", url=ADMIN_URL)
    kb.button(text="🔙 Назад", callback_data=MenuCB(action="main"))
    kb.adjust(1)
    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        pass


# --- АДМИН ПАНЕЛЬ ---
@admin_router.callback_query(AdminCB.filter(F.action == "panel"))
async def admin_panel(call: CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Статистика", callback_data=AdminCB(action="stats"))
    kb.button(text="⏳ Ожидающие", callback_data=AdminCB(action="pending"))
    kb.button(text="👥 Пользователи", callback_data=AdminCB(action="users_menu"))
    kb.button(text="🎟 Промокоды", callback_data=PromoCB(action="list"))
    kb.button(text="📢 Рассылка", callback_data=AdminCB(action="broadcast_menu"))
    kb.button(text="🔙 В меню", callback_data=MenuCB(action="main"))
    kb.adjust(2, 2, 1, 1)
    try:
        await call.message.edit_text("👑 <b>Админ панель</b>", reply_markup=kb.as_markup())
    except Exception:
        pass


@admin_router.callback_query(AdminCB.filter(F.action == "stats"))
async def admin_stats(call: CallbackQuery, db: AsyncDatabase):
    stats = await db.get_stats()
    text = f"""📊 <b>Общая статистика</b>

👥 <b>Люди в боте:</b> {stats['total_users']}
✅ <b>Активные подписки:</b> {stats['active_users']}
❌ <b>Неактивные / Истекшие:</b> {stats['inactive_users']}

💰 <b>Всего продаж:</b> {stats['total_payments']}
💎 <b>Общая выручка:</b> ~{stats['total_revenue']}

📈 <b>По методам:</b>
  ├ ⭐ Stars: {stats['rev_by_method'].get('stars', 0)}
  ├ 🪙 Crypto: {stats['rev_by_method'].get('cryptobot', 0)}
  ├ 💎 TON Direct: {stats['rev_by_method'].get('ton_direct', 0)}
  └ 👤 Админу: {stats['rev_by_method'].get('admin', 0)}"""

    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data=AdminCB(action="panel")).as_markup()
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass


@admin_router.callback_query(AdminCB.filter(F.action == "pending"))
async def admin_pending(call: CallbackQuery, db: AsyncDatabase):
    pending = await db.get_pending_payments()
    text = "⏳ <b>Ожидающие платежи:</b>\n\n"
    if pending:
        for p in pending[:5]:
            ch_name = CHANNELS.get(p['channel_key'], {}).get('name', 'Неизвестно')
            text += f"🆔 {p['id']} | 👤 {p['first_name']} | 📢 {ch_name} | 💰 {p['amount']}\n"
    else:
        text += "✅ Нет ожидающих."
    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data=AdminCB(action="panel")).as_markup()
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass


@admin_router.callback_query(GrantCB.filter())
async def grant_manual(call: CallbackQuery, callback_data: GrantCB, db: AsyncDatabase):
    await process_successful_payment(call.bot, db, callback_data.payment_id)
    text = f"✅ <b>Доступ выдан!</b>\n👤 <b>ID:</b> {callback_data.user_id}"
    try:
        if call.message.photo:
            await call.message.edit_caption(caption=text)
        else:
            await call.message.edit_text(text)
    except Exception:
        pass


@admin_router.callback_query(RejectCB.filter())
async def reject_manual(call: CallbackQuery, callback_data: RejectCB, db: AsyncDatabase):
    payment = await db.get_payment_by_id(callback_data.payment_id)
    await db.update_payment(callback_data.payment_id, status='rejected')

    old_text = call.message.html_text or ""
    new_text = old_text + "\n\n❌ <b>ОТКЛОНЕНО</b>"
    try:
        if call.message.photo:
            await call.message.edit_caption(caption=new_text, parse_mode="HTML")
        else:
            await call.message.edit_text(text=new_text, parse_mode="HTML")
    except Exception:
        pass

    if payment:
        user_id = payment['user_id']
        method = payment['method']
        reject_text = f"❌ <b>Ваш платеж отклонен!</b>\n\nАдминистратор не подтвердил вашу оплату. Доступ не выдан.\n\nЕсли произошла ошибка, напишите в поддержку."
        try:
            kb = InlineKeyboardBuilder().button(text="👨‍💻 Написать в поддержку", url=ADMIN_URL).as_markup()
            await call.bot.send_message(chat_id=user_id, text=reject_text, reply_markup=kb,
                                        disable_web_page_preview=True)
        except Exception:
            pass


# --- УПРАВЛЕНИЕ ПРОМОКОДАМИ ---
@admin_router.callback_query(PromoCB.filter(F.action == "list"))
async def admin_promo_list(call: CallbackQuery, db: AsyncDatabase):
    promos = await db.get_all_promos()
    bot_info = await call.bot.get_me()
    bot_username = bot_info.username
    kb = InlineKeyboardBuilder()

    text = "🎟 <b>Управление промокодами</b>\n\n"
    if promos:
        for p in promos:
            limit = "Безлимит" if p['max_uses'] == 0 else f"{p['uses']}/{p['max_uses']}"
            text += f"🏷 <b>{p['code']}</b> | -{p['discount_percent']}%\n"
            text += f"📊 Использовано: {limit}\n"
            text += f"🔗 Ссылка: <code>https://t.me/{bot_username}?start=promo_{p['code']}</code>\n\n"
            kb.button(text=f"❌ Удалить {p['code']}", callback_data=PromoCB(action="del", code=p['code']))
    else:
        text += "<i>Нет активных промокодов.</i>\n\n"

    kb.button(text="➕ Создать промокод", callback_data=PromoCB(action="add"))
    kb.button(text="🔙 В админку", callback_data=AdminCB(action="panel"))
    kb.adjust(1)
    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        pass


@admin_router.callback_query(PromoCB.filter(F.action == "del"))
async def admin_promo_del(call: CallbackQuery, callback_data: PromoCB, db: AsyncDatabase):
    await db.delete_promo(callback_data.code)
    try:
        await call.answer("✅ Промокод удален")
    except Exception:
        pass
    await admin_promo_list(call, db)


@admin_router.callback_query(PromoCB.filter(F.action == "add"))
async def admin_promo_add(call: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder().button(text="Отмена", callback_data=PromoCB(action="list")).as_markup()
    try:
        await call.message.edit_text("Отправьте название промокода (например, SALE50):", reply_markup=kb)
    except Exception:
        pass
    await state.set_state(AdminPromoState.waiting_for_code)


@admin_router.message(AdminPromoState.waiting_for_code)
async def admin_promo_code(message: Message, state: FSMContext):
    await state.update_data(code=message.text.strip().upper())
    try:
        await message.answer("Отправьте процент скидки (число от 1 до 99):")
    except Exception:
        pass
    await state.set_state(AdminPromoState.waiting_for_percent)


@admin_router.message(AdminPromoState.waiting_for_percent)
async def admin_promo_percent(message: Message, state: FSMContext):
    if not message.text.isdigit() or not (1 <= int(message.text) <= 99):
        try:
            return await message.answer("⚠️ Введите число от 1 до 99:")
        except Exception:
            return
    await state.update_data(percent=int(message.text))
    try:
        await message.answer("Отправьте максимальное количество использований (0 - безлимит):")
    except Exception:
        pass
    await state.set_state(AdminPromoState.waiting_for_uses)


@admin_router.message(AdminPromoState.waiting_for_uses)
async def admin_promo_uses(message: Message, state: FSMContext, db: AsyncDatabase):
    if not message.text.isdigit():
        try:
            return await message.answer("⚠️ Введите число (0 или больше):")
        except Exception:
            return

    data = await state.get_data()
    await db.add_promo(data['code'], data['percent'], int(message.text))
    await state.clear()

    kb = InlineKeyboardBuilder().button(text="🔙 К списку промокодов", callback_data=PromoCB(action="list")).as_markup()
    try:
        await message.answer(f"✅ Промокод <b>{data['code']}</b> на {data['percent']}% успешно создан!", reply_markup=kb)
    except Exception:
        pass


# --- УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ (АДМИН) ---
@admin_router.callback_query(AdminCB.filter(F.action == "users_menu"))
async def admin_users_menu(call: CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="👥 Все пользователи", callback_data=UsersPageCB(page=1, f_type="all"))
    kb.button(text="✅ С активной подпиской", callback_data=UsersPageCB(page=1, f_type="active"))
    kb.button(text="🔍 Найти по ID", callback_data=AdminCB(action="search_user"))
    kb.button(text="🔙 Назад", callback_data=AdminCB(action="panel"))
    kb.adjust(1)
    try:
        await call.message.edit_text("👥 <b>Управление пользователями</b>", reply_markup=kb.as_markup())
    except Exception:
        pass


@admin_router.callback_query(AdminCB.filter(F.action == "search_user"))
async def admin_search_user(call: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder().button(text="🔙 Отмена", callback_data=AdminCB(action="users_menu")).as_markup()
    try:
        await call.message.edit_text("Отправьте Telegram ID пользователя (только цифры):", reply_markup=kb)
    except Exception:
        pass
    await state.set_state(SearchUserState.waiting_for_id)


@admin_router.message(SearchUserState.waiting_for_id)
async def admin_process_search(message: Message, state: FSMContext, db: AsyncDatabase):
    if not message.text.isdigit():
        try:
            return await message.answer("⚠️ ID должен состоять только из цифр. Попробуйте снова.")
        except Exception:
            return
    await state.clear()
    call_mock = CallbackQuery(id="0", from_user=message.from_user, chat_instance="0", message=message)
    await admin_user_profile(call_mock, UserProfileCB(user_id=int(message.text)), db, edit=False)


@admin_router.callback_query(UsersPageCB.filter())
async def admin_users_list(call: CallbackQuery, callback_data: UsersPageCB, db: AsyncDatabase):
    page = callback_data.page
    f_type = callback_data.f_type
    limit = 10
    offset = (page - 1) * limit

    users = await db.get_users_page(limit, offset, f_type)
    total_users = await db.get_users_count(f_type)

    kb = InlineKeyboardBuilder()
    title = "С активной подпиской" if f_type == "active" else "Все пользователи"
    text = f"👥 <b>{title}</b> (Стр. {page})\nВсего: {total_users}\n\n"

    for u in users:
        name = u['first_name'] or "Без имени"
        t_info = ""
        if u.get('tariff_type'):
            ch_name = CHANNELS.get(u.get('channel_key', '1'), {}).get('name', 'Канал')[:10]
            t_name = "Мес." if u['tariff_type'] == 'monthly' else "Навс."
            t_info = f" [{ch_name}|{t_name}]"

        kb.button(text=f"👤 {name}{t_info}", callback_data=UserProfileCB(user_id=u['user_id']))

    kb.adjust(1)

    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardBuilder().button(text="⬅️", callback_data=UsersPageCB(page=page - 1,
                                                                                               f_type=f_type)).as_markup().inline_keyboard[
                               0][0])
    if offset + limit < total_users:
        nav_buttons.append(InlineKeyboardBuilder().button(text="➡️", callback_data=UsersPageCB(page=page + 1,
                                                                                               f_type=f_type)).as_markup().inline_keyboard[
                               0][0])

    if nav_buttons:
        kb.row(*nav_buttons)

    kb.row(InlineKeyboardBuilder().button(text="🔙 Назад",
                                          callback_data=AdminCB(action="users_menu")).as_markup().inline_keyboard[0][0])
    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        pass


@admin_router.callback_query(UserProfileCB.filter())
async def admin_user_profile(call: CallbackQuery, callback_data: UserProfileCB, db: AsyncDatabase, edit=True):
    user_id = callback_data.user_id
    accesses = await db.get_user_active_accesses(user_id)

    text = f"👤 <b>Профиль пользователя</b>\n🆔 <b>ID:</b> <code>{user_id}</code>\n\n"
    kb = InlineKeyboardBuilder()

    if accesses:
        for ch_key, acc in accesses.items():
            ch_name = CHANNELS.get(ch_key, {}).get('name', 'Неизвестный канал')
            exp_date = parse_dt(acc['expires_at'])

            text += f"📢 <b>{ch_name}</b>\n"
            if acc['tariff_type'] == 'lifetime':
                text += "💎 <b>Статус:</b> Активен (НАВСЕГДА)\n"
            else:
                days_left = (exp_date - datetime.now()).days
                text += f"💎 <b>Статус:</b> Активен\n⏳ <b>Осталось:</b> {days_left} дней\n📅 <b>До:</b> {exp_date.strftime('%d.%m.%Y %H:%M')}\n"

            kb.button(text=f"🎁 +30 дней ({ch_name[:10]})",
                      callback_data=UserActionCB(action="extend", user_id=user_id, ch_id=ch_key))
            kb.button(text=f"❌ Аннулировать ({ch_name[:10]})",
                      callback_data=UserActionCB(action="revoke", user_id=user_id, ch_id=ch_key))
            text += "\n"
    else:
        text += "❌ <b>Статус:</b> Нет активных подписок"

    kb.button(text="🔙 К списку", callback_data=AdminCB(action="users_menu"))
    kb.adjust(1)

    try:
        if edit:
            await call.message.edit_text(text, reply_markup=kb.as_markup())
        else:
            await call.message.answer(text, reply_markup=kb.as_markup())
    except Exception:
        pass


@admin_router.callback_query(UserActionCB.filter(F.action == "extend"))
async def admin_extend_user(call: CallbackQuery, callback_data: UserActionCB, db: AsyncDatabase):
    success = await db.extend_access(callback_data.user_id, callback_data.ch_id, 30)
    try:
        if success:
            await call.answer("✅ Добавлено 30 дней!", show_alert=True)
        else:
            await call.answer("❌ У юзера нет активной подписки на этот канал", show_alert=True)
    except Exception:
        pass
    await admin_user_profile(call, UserProfileCB(user_id=callback_data.user_id), db)


@admin_router.callback_query(UserActionCB.filter(F.action == "revoke"))
async def admin_revoke_user(call: CallbackQuery, callback_data: UserActionCB, db: AsyncDatabase):
    await db.revoke_access(callback_data.user_id, callback_data.ch_id)
    ch_info = CHANNELS.get(callback_data.ch_id)
    if ch_info and ch_info['id']:
        try:
            await call.bot.ban_chat_member(chat_id=ch_info['id'], user_id=callback_data.user_id)
            await call.bot.unban_chat_member(chat_id=ch_info['id'], user_id=callback_data.user_id)
        except Exception as e:
            logger.error(f"Не удалось кикнуть пользователя вручную: {e}")
    try:
        await call.answer("✅ Доступ аннулирован, пользователь исключен!", show_alert=True)
    except Exception:
        pass
    await admin_user_profile(call, UserProfileCB(user_id=callback_data.user_id), db)


@admin_router.callback_query(AdminCB.filter(F.action == "broadcast_menu"))
async def broadcast_menu(call: CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="📢 Всем (Общая)", callback_data=BroadcastCB(target="all"))
    kb.button(text="✅ Только АКТИВНЫМ", callback_data=BroadcastCB(target="active"))
    kb.button(text="❌ Только НЕАКТИВНЫМ", callback_data=BroadcastCB(target="inactive"))
    kb.button(text="🔙 Назад", callback_data=AdminCB(action="panel"))
    kb.adjust(1)
    try:
        await call.message.edit_text("🎯 <b>Кому делаем рассылку?</b>", reply_markup=kb.as_markup())
    except Exception:
        pass


@admin_router.callback_query(BroadcastCB.filter())
async def broadcast_start(call: CallbackQuery, callback_data: BroadcastCB, state: FSMContext):
    await state.update_data(target=callback_data.target)
    target_names = {"all": "📢 ВСЕМ", "active": "✅ АКТИВНЫМ", "inactive": "❌ НЕАКТИВНЫМ"}
    kb = InlineKeyboardBuilder().button(text="Отмена", callback_data=AdminCB(action="panel")).as_markup()
    try:
        await call.message.edit_text(
            f"Выбрано: <b>{target_names[callback_data.target]}</b>\n\nОтправьте сообщение (можно с фото/видео/премиум-эмодзи).",
            reply_markup=kb)
    except Exception:
        pass
    await state.set_state(BroadcastState.waiting_for_message)


@admin_router.message(BroadcastState.waiting_for_message)
async def broadcast_confirm(message: Message, state: FSMContext):
    await state.update_data(msg_id=message.message_id, chat_id=message.chat.id)
    data = await state.get_data()
    target_names = {"all": "ВСЕМ", "active": "АКТИВНЫМ", "inactive": "НЕАКТИВНЫМ"}

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Начать прямо сейчас", callback_data="start_broadcast")
    kb.button(text="📅 Запланировать", callback_data="schedule_broadcast")
    kb.button(text="❌ Отмена", callback_data=AdminCB(action="panel"))
    kb.adjust(1)

    try:
        await message.answer(
            f"👆 <b>Превью сообщения.</b>\nОно будет отправлено: <b>{target_names[data['target']]}</b>\n\nВыберите действие:",
            reply_markup=kb.as_markup())
    except Exception:
        pass
    await state.set_state(BroadcastState.confirm)


@admin_router.callback_query(BroadcastState.confirm, F.data == "schedule_broadcast")
async def broadcast_schedule_ask(call: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder().button(text="Отмена", callback_data=AdminCB(action="panel")).as_markup()
    now_str = datetime.now().strftime('%d.%m.%Y %H:%M')
    try:
        await call.message.edit_text(
            f"📅 <b>Отложенная рассылка</b>\n\n"
            f"Введите дату и время отправки в формате:\n<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n\n"
            f"<i>Пример: 31.12.2024 23:50</i>\n\n"
            f"🕒 <b>ВНИМАНИЕ! Текущее время на сервере:</b> <code>{now_str}</code>\n"
            f"<i>(Указывайте время, ориентируясь на время сервера!)</i>",
            reply_markup=kb
        )
    except Exception:
        pass
    await state.set_state(BroadcastState.waiting_for_date)


@admin_router.message(BroadcastState.waiting_for_date)
async def broadcast_schedule_save(message: Message, state: FSMContext, db: AsyncDatabase):
    try:
        send_at = datetime.strptime(message.text.strip(), "%d.%m.%Y %H:%M")
        if send_at <= datetime.now():
            try:
                return await message.answer("⚠️ Время должно быть в будущем! Попробуйте снова.")
            except Exception:
                return

        data = await state.get_data()
        await db.add_scheduled_broadcast(data['target'], data['chat_id'], data['msg_id'], send_at)
        await state.clear()

        kb = InlineKeyboardBuilder().button(text="В админку", callback_data=AdminCB(action="panel")).as_markup()
        try:
            await message.answer(
                f"✅ <b>Рассылка запланирована на {send_at.strftime('%d.%m.%Y %H:%M')}!</b>\n\n⚠️ <b>ВАЖНО:</b> Не удаляйте отправленное сообщение из этого чата, иначе бот не сможет его разослать!",
                reply_markup=kb)
        except Exception:
            pass
    except ValueError:
        try:
            await message.answer("⚠️ Неверный формат! Напишите дату как в примере: <code>31.12.2024 23:50</code>")
        except Exception:
            pass


@admin_router.callback_query(BroadcastState.confirm, F.data == "start_broadcast")
async def broadcast_send(call: CallbackQuery, state: FSMContext, db: AsyncDatabase):
    data = await state.get_data()
    target = data['target']
    try:
        await call.message.edit_text("⏳ <b>Рассылка началась...</b>")
    except Exception:
        pass
    await state.clear()

    if target == "active":
        users = await db.get_active_user_ids()
    elif target == "inactive":
        users = await db.get_inactive_user_ids()
    else:
        users = await db.get_all_user_ids()

    success, fail = 0, 0
    for u_id in users:
        try:
            await call.bot.copy_message(chat_id=u_id, from_chat_id=data['chat_id'], message_id=data['msg_id'])
            success += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)

    try:
        await call.message.edit_text(
            f"✅ <b>Рассылка завершена!</b>\n\nАудитория: {target}\nУспешно: {success}\nОшибок (заблокировали бота): {fail}",
            reply_markup=InlineKeyboardBuilder().button(text="В админку",
                                                        callback_data=AdminCB(action="panel")).as_markup())
    except Exception:
        pass


# --- ФОНОВЫЕ ЗАДАЧИ ---
async def background_tasks(bot: Bot, db: AsyncDatabase, crypto_bot: AsyncCryptoBot, ton_api: AsyncTonAPI):
    loop_count = 0
    while True:
        try:
            # 1. Отложенные рассылки
            pending_broadcasts = await db.get_pending_broadcasts()
            for b in pending_broadcasts:
                target = b['target']
                if target == "active":
                    users = await db.get_active_user_ids()
                elif target == "inactive":
                    users = await db.get_inactive_user_ids()
                else:
                    users = await db.get_all_user_ids()

                for u_id in users:
                    try:
                        await bot.copy_message(chat_id=u_id, from_chat_id=b['from_chat_id'], message_id=b['message_id'])
                    except Exception:
                        pass
                    await asyncio.sleep(0.05)
                await db.mark_broadcast_completed(b['id'])

            # 2. Быстрые проверки платежей
            pending_payments = await db.get_pending_payments()
            for p in pending_payments:
                if crypto_bot and p['method'] == 'cryptobot' and p['invoice_id']:
                    res = await crypto_bot.check_invoice(p['invoice_id'])
                    if res['success'] and res['status'] == 'paid':
                        await process_successful_payment(bot, db, p['id'])

                elif p['method'] == 'ton_direct':
                    is_paid = await ton_api.check_payment(p['id'], p['amount'])
                    if is_paid:
                        await process_successful_payment(bot, db, p['id'])

            # 3. Дожималка (Abandoned Cart)
            thirty_mins_ago = datetime.now() - timedelta(minutes=30)
            async with db.get_db() as conn:
                async with conn.execute(
                        "SELECT * FROM payments WHERE status = 'pending' AND abandoned_reminded = 0 AND created_at < ?",
                        (thirty_mins_ago,)) as cursor:
                    abandoned = [dict(row) for row in await cursor.fetchall()]

                for p in abandoned:
                    try:
                        kb = InlineKeyboardBuilder()
                        kb.button(text="👨‍💻 Написать в поддержку", url=ADMIN_URL)
                        kb.button(text="🔄 Попробовать снова", callback_data=MenuCB(action="buy"))
                        kb.adjust(1)
                        await bot.send_message(
                            p['user_id'],
                            f"👋 <b>Привет!</b>\n\nМы заметили, что вы хотели оплатить доступ к каналу, но транзакция не была завершена.\n\nЕсли у вас возникли сложности с оплатой или нужна помощь — смело жмите кнопку ниже, мы с радостью поможем!",
                            reply_markup=kb.as_markup()
                        )
                    except Exception:
                        pass
                    finally:
                        await conn.execute("UPDATE payments SET abandoned_reminded = 1 WHERE id = ?", (p['id'],))
                        await conn.commit()

            unnotified = await db.get_completed_unnotified_payments()
            for p in unnotified:
                await notify_admin_successful_payment(bot, p, p['method'], db)

            # 4. Медленные проверки (напоминания и кики раз в час)
            if loop_count == 0:
                active_subs = await db.get_active_expiring_subs()
                for sub in active_subs:
                    exp_date = parse_dt(sub['expires_at'])
                    time_left = exp_date - datetime.now()
                    days_left = time_left.days
                    hours_left = time_left.total_seconds() / 3600

                    ch_info = CHANNELS.get(sub['channel_key'], {})
                    ch_name = ch_info.get('name', 'Приватный канал')

                    rem_type = None
                    text = None

                    if 118 < hours_left <= 120:
                        rem_type = "5_days"
                        text = f"⚠️ <b>Внимание!</b>\n\nВаш доступ к каналу <b>{ch_name}</b> истекает через <b>5 дней</b>.\n\nПродлите подписку, чтобы не потерять доступ!"
                    elif 70 < hours_left <= 72:
                        rem_type = "3_days"
                        text = f"⚠️ <b>Осталось 3 дня!</b>\n\nВаш доступ к <b>{ch_name}</b> скоро закончится.\n\nПродлите подписку прямо сейчас."
                    elif 22 < hours_left <= 24:
                        rem_type = "1_day"
                        text = f"🚨 <b>Последний день!</b>\n\nВаша подписка на <b>{ch_name}</b> истекает менее чем через 24 часа!\n\nУспейте продлить доступ, чтобы вас не исключили из канала."
                    elif 0 < hours_left <= 1:
                        rem_type = "1_hour"
                        text = f"🔥 <b>ОСТАЛСЯ 1 ЧАС!</b>\n\nЧерез час бот автоматически исключит вас из закрытого канала <b>{ch_name}</b>.\n\nПродлите доступ прямо сейчас, чтобы остаться с нами!"

                    if rem_type and not await db.has_reminder(sub['id'], rem_type):
                        try:
                            kb = InlineKeyboardBuilder().button(text=f"🔄 Продлить: {ch_name}",
                                                                callback_data=TariffCB(type="monthly", ch_id=sub[
                                                                    'channel_key'])).as_markup()
                            await bot.send_message(sub['user_id'], text, reply_markup=kb)
                            await db.add_reminder(sub['id'], rem_type)
                        except Exception:
                            pass

                expired_subs = await db.get_expired_subscriptions()
                for sub in expired_subs:
                    user_id = sub['user_id']
                    ch_key = sub['channel_key']
                    ch_info = CHANNELS.get(ch_key, {})
                    ch_id = ch_info.get('id')
                    ch_name = ch_info.get('name', 'Приватный канал')

                    if ch_id and not await db.has_active_subscription(user_id, ch_key):
                        try:
                            await bot.ban_chat_member(chat_id=ch_id, user_id=user_id)
                            await bot.unban_chat_member(chat_id=ch_id, user_id=user_id)
                            try:
                                kb = InlineKeyboardBuilder().button(text="🔄 Вернуться в канал",
                                                                    callback_data=TariffCB(type="monthly",
                                                                                           ch_id=ch_key)).as_markup()
                                await bot.send_message(user_id,
                                                       f"💔 <b>Очень жаль расставаться!</b>\n\nВаша подписка на канал <b>{ch_name}</b> закончилась, и нам пришлось исключить вас.\n\nМы будем рады видеть вас снова, наши двери всегда открыты!",
                                                       reply_markup=kb)
                            except Exception:
                                pass
                        except Exception as e:
                            logger.error(f"Ошибка кика {user_id} из {ch_id}: {e}")
                    await db.mark_as_kicked(sub['id'])

        except Exception as e:
            logger.error(f"Фоновая задача упала: {e}")

        loop_count = (loop_count + 1) % 60
        await asyncio.sleep(60)


async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    db = AsyncDatabase()
    await db.init_db()

    crypto_bot = AsyncCryptoBot(
        CRYPTO_BOT_TOKEN) if CRYPTO_BOT_TOKEN and CRYPTO_BOT_TOKEN != 'your_cryptobot_token_here' else None
    ton_api = AsyncTonAPI(TON_WALLET, TONCENTER_API_KEY)

    user_router.message.middleware(RateLimitMiddleware())
    user_router.callback_query.middleware(RateLimitMiddleware())

    dp.include_router(user_router)
    dp.include_router(admin_router)
    dp.workflow_data.update({'db': db, 'crypto_bot': crypto_bot, 'ton_api': ton_api})

    asyncio.create_task(background_tasks(bot, db, crypto_bot, ton_api))
    logger.info("🚀 Бот запущен! Все баги устранены, синтаксис исправлен.")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())