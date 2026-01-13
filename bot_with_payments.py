import os
import asyncio
import httpx
import aiosqlite
import random
import json
from dotenv import load_dotenv

# Попытка импорта платёжной интеграции
try:
    from payment_integration import (
        init_paypal, init_yookassa,
        create_paypal_payment, create_yookassa_payment,
        verify_paypal_payment, verify_yookassa_payment,
        run_webhook_server, PAYPAL_AVAILABLE, YOOKASSA_AVAILABLE
    )
    PAYMENT_INTEGRATION = True
except ImportError:
    PAYMENT_INTEGRATION = False
    PAYPAL_AVAILABLE = False
    YOOKASSA_AVAILABLE = False
    
    # Заглушки для функций
    async def create_paypal_payment(*args, **kwargs): return None
    async def create_yookassa_payment(*args, **kwargs): return None
    async def verify_paypal_payment(*args, **kwargs): return False
    async def verify_yookassa_payment(*args, **kwargs): return False
    
    print("⚠️ payment_integration.py не найден, используем ручной режим")
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime

# 1. Загружаем ключи
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OLLAMA_URL = "http://localhost:11434/api/generate"
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1001234567890"))
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PAYPAL_EMAIL = os.getenv("PAYPAL_EMAIL", "Peter-1955@mail.ru")
WEBMONEY_WALLET = os.getenv("WEBMONEY_WALLET", "Z346673612158")

# Ограничения и кеширование для AI-ответов
DAILY_AI_REQUEST_LIMIT = 10
AI_RESPONSE_CACHE = {}

# Реферальная система
REFERRAL_COMMISSION = 0.15  # 15% комиссия

# Проверка критических переменных
if not TELEGRAM_TOKEN:
    print("❌ ОШИБКА: TELEGRAM_BOT_TOKEN не установлен!")
    print("\n📝 РЕШЕНИЕ:")
    print("1. Создайте файл .env в папке проекта")
    print("2. Добавьте строку: TELEGRAM_BOT_TOKEN=your_token_here")
    print("3. Получить токен можно у @BotFather в Telegram")
    print("4. Формат токена: 123456789:ABCDEFGHijklmnopqrstuvwxyz")
    exit(1)

if ADMIN_ID == 0:
    print("⚠️  ВНИМАНИЕ: ADMIN_ID не установлен!")
    print("Установите в .env: ADMIN_ID=your_telegram_id")

if CHANNEL_ID == -1001234567890:
    print("⚠️  ВНИМАНИЕ: CHANNEL_ID не установлен!")
    print("Установите в .env: CHANNEL_ID=your_channel_id")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# ===== ДАННЫЕ КУРСОВ (в памяти для быстрого доступа при инициализации) =====
COURSES_DATA = {
    "course_1": {
        "name": "Заработок на фрилансе", 
        "price": 100, 
        "description": "Полный гайд по фрилансу",
        "lessons": {
            "1": {
                "title": "Выбор ниши на фрилансе",
                "content": """📖 <b>УРОК 1: ВЫБОР НИШИ НА ФРИЛАНСЕ</b>

<b>Что такое ниша?</b>
Ниша - это специализированная область, где вы предлагаете услуги. Правильный выбор ниши - половина успеха фрилансера.

<b>Популярные ниши 2025:</b>
✅ Копирайтинг (500-2000₽)
✅ SMM (2000-5000₽)
✅ Веб-дизайн (1000-10000₽)
✅ Программирование (2000-20000₽)
"""
            },
            "2": {
                "title": "Создание портфолио",
                "content": """📖 <b>УРОК 2: СОЗДАНИЕ ПОРТФОЛИО</b>

<b>Почему портфолио важно?</b>
Портфолио - это ваше "лицо" на фрилансе. 80% заказчиков судят о вас по портфолио.

<b>Что включить:</b>
✅ 3-5 лучших работ
✅ Описание проекта (проблема → решение)
✅ Результаты и метрики
✅ Отзывы клиентов
"""
            }
        }
    },
    "course_2": {
        "name": "Инвестирование в крипто", 
        "price": 200, 
        "description": "Безопасное инвестирование",
        "lessons": {
            "1": {
                "title": "Что такое криптовалюта",
                "content": """📖 <b>УРОК 1: ЧТО ТАКОЕ КРИПТОВАЛЮТА</b>

<b>Определение:</b>
Криптовалюта - цифровые деньги, защищённые математикой.

<b>Преимущества:</b>
✅ Нет комиссий банков (2-5%)
✅ Быстрые переводы (минуты)
✅ Прозрачность и безопасность
"""
            }
        }
    },
    "course_3": {
        "name": "Создание SaaS проекта", 
        "price": 300, 
        "description": "Как запустить свой сервис",
        "lessons": {
            "1": {
                "title": "Как найти идею SaaS",
                "content": """📖 <b>УРОК 1: КАК НАЙТИ ИДЕЮ SaaS</b>

<b>Что такое SaaS?</b>
Software as a Service - программное обеспечение, за которое люди платят ежемесячно.

<b>Преимущества SaaS:</b>
✅ Рекуррентный доход
✅ Предсказуемая выручка
✅ Легко масштабировать
"""
            }
        }
    }
}

# ===== БАЗА ДАННЫХ =====
async def init_db():
    async with aiosqlite.connect("users.db") as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS courses (
                id TEXT PRIMARY KEY,
                name TEXT,
                price INTEGER,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                active BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS lessons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id TEXT,
                lesson_number INTEGER,
                title TEXT,
                content TEXT,
                FOREIGN KEY(course_id) REFERENCES courses(id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                course_id TEXT,
                amount INTEGER,
                status TEXT DEFAULT 'pending',
                payment_method TEXT,
                transaction_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(course_id) REFERENCES courses(id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS ai_requests (
                user_id INTEGER,
                request_date TEXT,
                request_count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, request_date)
            )
        ''')
        # Таблица аналитики событий
        await db.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                event_type TEXT,
                course_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT
            )
        ''')
        # Таблица реферралов
        await db.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                commission REAL DEFAULT 0,
                paid BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.commit()
        
        # Инициализируем курсы в БД
        await init_courses_in_db(db)

async def init_courses_in_db(db):
    """Инициализируем курсы в БД из COURSES_DATA"""
    
    # Проверяем, есть ли уже курсы
    cursor = await db.execute("SELECT COUNT(*) FROM courses")
    count = await cursor.fetchone()
    if count and count[0] > 0:
        return  # Курсы уже инициализированы
    
    # Добавляем курсы из COURSES_DATA
    for course_id, course_data in COURSES_DATA.items():
        await db.execute(
            "INSERT INTO courses (id, name, price, description) VALUES (?, ?, ?, ?)",
            (course_id, course_data['name'], course_data['price'], course_data['description'])
        )
        
        # Добавляем уроки
        if 'lessons' in course_data:
            for lesson_num, lesson_data in course_data['lessons'].items():
                await db.execute(
                    "INSERT INTO lessons (course_id, lesson_number, title, content) VALUES (?, ?, ?, ?)",
                    (course_id, int(lesson_num), lesson_data['title'], lesson_data['content'])
                )
    
    await db.commit()

# ===== ФУНКЦИИ РАБОТЫ С КУРСАМИ =====
async def get_course(course_id):
    """Получить информацию о курсе"""
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT id, name, price, description FROM courses WHERE id = ?", (course_id,))
        return await cursor.fetchone()

async def get_all_courses():
    """Получить все активные курсы"""
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT id, name, price, description FROM courses WHERE active = 1")
        return await cursor.fetchall()

async def get_course_lessons(course_id):
    """Получить все уроки курса"""
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT lesson_number, title, content FROM lessons WHERE course_id = ? ORDER BY lesson_number",
            (course_id,)
        )
        return await cursor.fetchall()

async def get_lesson(course_id, lesson_number):
    """Получить конкретный урок"""
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT title, content FROM lessons WHERE course_id = ? AND lesson_number = ?",
            (course_id, lesson_number)
        )
        return await cursor.fetchone()

async def add_user(user_id, username):
    async with aiosqlite.connect("users.db") as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (user_id, username))
        await db.commit()

async def get_user_purchases(user_id):
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT course_id, status FROM purchases WHERE user_id = ?", (user_id,))
        purchases = await cursor.fetchall()
    return purchases

async def has_access_to_course(user_id, course_id):
    purchases = await get_user_purchases(user_id)
    for course, status in purchases:
        if course == course_id and status == "completed":
            return True
    return False

async def has_any_active_course(user_id):
    """Проверяем, покупал ли пользователь хотя бы один курс."""
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT 1 FROM purchases WHERE user_id = ? AND status = 'completed' LIMIT 1",
            (user_id,)
        )
        return await cursor.fetchone() is not None

async def get_daily_requests(user_id):
    """Возвращаем, сколько AI-запросов сделал пользователь сегодня."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT request_count FROM ai_requests WHERE user_id = ? AND request_date = ?",
            (user_id, today)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

async def increment_daily_request(user_id):
    """Увеличиваем счётчик AI-запросов пользователя за сегодня."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    async with aiosqlite.connect("users.db") as db:
        await db.execute(
            """
            INSERT INTO ai_requests (user_id, request_date, request_count)
            VALUES (?, ?, 1)
            ON CONFLICT(user_id, request_date)
            DO UPDATE SET request_count = request_count + 1
            """,
            (user_id, today)
        )
        await db.commit()

# ===== АНАЛИТИКА И СОБЫТИЯ =====
async def track_event(user_id, event_type, course_id=None, metadata=None):
    """Записываем событие в таблицу аналитики."""
    async with aiosqlite.connect("users.db") as db:
        await db.execute(
            "INSERT INTO events (user_id, event_type, course_id, metadata) VALUES (?, ?, ?, ?)",
            (user_id, event_type, course_id, json.dumps(metadata) if metadata else None)
        )
        await db.commit()

async def get_funnel_stats():
    """Получаем статистику воронки продаж."""
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("""
            SELECT event_type, COUNT(*) as cnt 
            FROM events 
            GROUP BY event_type 
            ORDER BY cnt DESC
        """)
        return await cursor.fetchall()

async def get_popular_courses():
    """Какие курсы самые популярные (по кликам и покупкам)."""
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("""
            SELECT course_id, 
                   SUM(CASE WHEN event_type = 'click_course' THEN 1 ELSE 0 END) as clicks,
                   SUM(CASE WHEN event_type = 'purchase_completed' THEN 1 ELSE 0 END) as purchases
            FROM events 
            WHERE course_id IS NOT NULL
            GROUP BY course_id
            ORDER BY purchases DESC, clicks DESC
        """)
        return await cursor.fetchall()

# ===== РЕФЕРРАЛЬНАЯ СИСТЕМА =====
async def save_referrer(user_id, referrer_id):
    """Сохраняем связку: кто привёл кого."""
    async with aiosqlite.connect("users.db") as db:
        # Проверяем, что пользователь не привёл сам себя и нет дубля
        if user_id == referrer_id:
            return
        cursor = await db.execute(
            "SELECT 1 FROM referrals WHERE referred_id = ?", (user_id,)
        )
        if await cursor.fetchone():
            return  # Уже есть реферер
        await db.execute(
            "INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
            (referrer_id, user_id)
        )
        await db.commit()

async def process_referral_commission(user_id, amount):
    """Начисляем комиссию рефереру при покупке."""
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT referrer_id FROM referrals WHERE referred_id = ? AND paid = 0",
            (user_id,)
        )
        row = await cursor.fetchone()
        if row:
            referrer_id = row[0]
            commission = amount * REFERRAL_COMMISSION
            await db.execute(
                "UPDATE referrals SET commission = commission + ?, paid = 0 WHERE referred_id = ?",
                (commission, user_id)
            )
            await db.commit()
            return referrer_id, commission
    return None, 0

async def get_referral_stats(user_id):
    """Статистика рефералов пользователя."""
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT COUNT(*), SUM(commission) FROM referrals WHERE referrer_id = ?",
            (user_id,)
        )
        row = await cursor.fetchone()
        return row[0] or 0, row[1] or 0

async def create_payment_order(user_id, course_id, amount):
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "INSERT INTO purchases (user_id, course_id, amount, status) VALUES (?, ?, ?, ?)",
            (user_id, course_id, amount, "pending")
        )
        await db.commit()
        payment_id = cursor.lastrowid
    return payment_id

async def update_payment_status(payment_id, status, transaction_id):
    async with aiosqlite.connect("users.db") as db:
        await db.execute(
            "UPDATE purchases SET status = ?, transaction_id = ? WHERE id = ?",
            (status, transaction_id, payment_id)
        )
        await db.commit()

async def notify_channel(message_text):
    try:
        await bot.send_message(CHANNEL_ID, message_text, parse_mode="HTML")
    except Exception as e:
        print(f"Ошибка отправки в канал: {e}")

# ===== КЛАВИАТУРЫ =====
def main_menu():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📚 Каталог курсов", callback_data="catalog")],
        [InlineKeyboardButton(text="💰 Мои покупки", callback_data="my_purchases")],
        [InlineKeyboardButton(text="❓ Помощь", callback_data="help")],
    ])
    return keyboard

async def catalog_menu():
    """Динамически создаём меню из БД"""
    courses = await get_all_courses()
    buttons = []
    for course_id, name, price, description in courses:
        buttons.append([InlineKeyboardButton(
            text=f"{name} - {price}₽",
            callback_data=f"buy_{course_id}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def payment_menu(course_id, payment_id):
    buttons = []
    
    # Если PayPal API доступен - предлагаем автоматическую оплату
    if PAYMENT_INTEGRATION and PAYPAL_AVAILABLE:
        buttons.append([InlineKeyboardButton(text="💳 PayPal (автооплата)", callback_data=f"pay_paypal_{payment_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="💳 PayPal", callback_data=f"pay_paypal_{payment_id}")])
    
    # YooKassa для российских карт
    if PAYMENT_INTEGRATION and YOOKASSA_AVAILABLE:
        buttons.append([InlineKeyboardButton(text="💳 Карта РФ (ЮKassa)", callback_data=f"pay_yookassa_{payment_id}")])
    
    buttons.append([InlineKeyboardButton(text="💰 WebMoney", callback_data=f"pay_webmoney_{payment_id}")])
    buttons.append([InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_catalog")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ===== КОМАНДЫ =====
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Пользователь"
    await add_user(user_id, username)
    
    # Обработка реферальной ссылки: /start ref_123456
    if message.text and message.text.startswith("/start ref_"):
        try:
            referrer_id = int(message.text.replace("/start ref_", ""))
            await save_referrer(user_id, referrer_id)
            await track_event(user_id, "referral_join", metadata={"referrer": referrer_id})
        except ValueError:
            pass
    
    await track_event(user_id, "bot_start")
    
    text = """
🎓 Добро пожаловать на платформу заработка в интернете!

Здесь вы найдёте курсы по:
✅ Фрилансу
✅ Крипто-инвестициям
✅ Созданию SaaS

Выберите, что вас интересует:
"""
    await message.answer(text, reply_markup=main_menu())

@dp.message(Command("referral"))
async def cmd_referral(message: types.Message):
    """Показывает реферальную ссылку и статистику."""
    user_id = message.from_user.id
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
    
    count, total_commission = await get_referral_stats(user_id)
    
    text = f"""
🤝 <b>РЕФЕРАЛЬНАЯ ПРОГРАММА</b>

Приглашайте друзей и получайте {int(REFERRAL_COMMISSION * 100)}% с каждой покупки!

🔗 <b>Ваша ссылка:</b>
<code>{ref_link}</code>

📊 <b>Статистика:</b>
👥 Приглашено: {count}
💰 Заработано: {total_commission:.2f}₽
"""
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Показывает статистику для админа."""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.answer("⛔ Команда доступна только администратору")
        return
    
    funnel = await get_funnel_stats()
    popular = await get_popular_courses()
    
    text = "📊 <b>АНАЛИТИКА</b>\n\n<b>Воронка событий:</b>\n"
    for event_type, cnt in funnel:
        text += f"• {event_type}: {cnt}\n"
    
    text += "\n<b>Популярные курсы:</b>\n"
    for course_id, clicks, purchases in popular:
        text += f"• {course_id}: {clicks} кликов, {purchases} покупок\n"
    
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("mycourse"))
async def cmd_mycourse(message: types.Message):
    user_id = message.from_user.id
    purchases = await get_user_purchases(user_id)
    
    if not purchases:
        await message.answer("❌ Вы ещё не купили ни одного курса.\n\nНапишите /start чтобы купить курс!")
        return
    
    completed_purchases = [(course_id, status) for course_id, status in purchases if status == "completed"]
    
    if not completed_purchases:
        await message.answer("❌ У вас нет активных курсов. Подождите подтверждения платежа.")
        return
    
    text = "📚 <b>ВАШИ КУПЛЕННЫЕ КУРСЫ:</b>\n\n"
    buttons = []
    
    for course_id, _ in completed_purchases:
        course = await get_course(course_id)
        if course:
            text += f"✅ {course[1]}\n"
            buttons.append([InlineKeyboardButton(
                text=f"📖 Открыть: {course[1]}", 
                callback_data=f"view_course_{course_id}"
            )])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У вас нет доступа к админ-панели")
        return
    
    admin_menu = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
    ])
    await message.answer("⚙️ <b>АДМИН-ПАНЕЛЬ</b>", reply_markup=admin_menu, parse_mode="HTML")

# ===== CALLBACK HANDLERS =====
@dp.callback_query(lambda c: c.data == "catalog")
async def show_catalog(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await track_event(user_id, "view_catalog")
    
    courses = await get_all_courses()
    text = "📚 <b>КАТАЛОГ КУРСОВ</b>\n\nВыберите интересующий вас курс:\n"
    for course_id, name, price, description in courses:
        text += f"\n💡 {name}\n   Описание: {description}\n   💰 Цена: {price}₽\n"
    
    keyboard = await catalog_menu()
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("buy_"))
async def buy_course(callback: types.CallbackQuery):
    course_id = callback.data.replace("buy_", "")
    course = await get_course(course_id)
    
    if not course:
        await callback.answer("❌ Курс не найден", show_alert=True)
        return
    
    user_id = callback.from_user.id
    await track_event(user_id, "click_course", course_id)
    
    if await has_access_to_course(user_id, course_id):
        await callback.answer("✅ Вы уже имеете доступ к этому курсу!", show_alert=True)
        return
    
    await track_event(user_id, "start_payment", course_id, {"price": course[2]})
    payment_id = await create_payment_order(user_id, course_id, course[2])  # course[2] = price
    
    text = f"""
🛒 <b>ПОКУПКА КУРСА</b>

📚 {course[1]}
💬 {course[3]}
💰 Цена: {course[2]}₽

Выберите способ оплаты:
"""
    await callback.message.edit_text(text, reply_markup=payment_menu(course_id, payment_id), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("pay_paypal_"))
async def pay_paypal(callback: types.CallbackQuery):
    payment_id = callback.data.replace("pay_paypal_", "")
    user_id = callback.from_user.id
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT course_id, amount FROM purchases WHERE id = ?", (int(payment_id),))
        purchase = await cursor.fetchone()
    
    if not purchase:
        await callback.answer("❌ Платёж не найден", show_alert=True)
        return
    
    course_id, amount = purchase
    
    # Если доступна интеграция с PayPal API - создаём настоящий платёж
    if PAYMENT_INTEGRATION and PAYPAL_AVAILABLE:
        await callback.answer("⏳ Создаю платёж...")
        payment_url = await create_paypal_payment(float(amount), course_id, int(payment_id))
        
        if payment_url:
            text = f"""
💳 <b>ОПЛАТА ЧЕРЕЗ PayPal</b>

💰 Сумма: {amount}₽

Нажмите кнопку ниже для перехода на страницу оплаты PayPal.
После оплаты вы будете перенаправлены обратно.
"""
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить через PayPal", url=payment_url)],
                [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_payment_{payment_id}")],
                [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_catalog")],
            ])
        else:
            text = "❌ Ошибка создания платежа. Попробуйте позже или выберите другой способ оплаты."
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_catalog")],
            ])
    else:
        # Ручной режим - инструкция для перевода
        text = f"""
💳 <b>ОПЛАТА ЧЕРЕЗ PayPal</b>

💰 Сумма: {amount}₽
📧 На адрес: {PAYPAL_EMAIL}

📌 Инструкция:
1. Откройте PayPal
2. Отправьте платёж на {PAYPAL_EMAIL}
3. Укажите сумму: {amount}₽
4. В комментарии укажите: #{payment_id}
5. Вернитесь и нажмите "✅ Подтвердить платёж"
"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Платёж выполнен", callback_data=f"confirm_paypal_{payment_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_catalog")],
        ])
    
    await track_event(user_id, "payment_method_selected", course_id, {"method": "paypal"})
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("pay_yookassa_"))
async def pay_yookassa(callback: types.CallbackQuery):
    """Оплата через ЮKassa (карты РФ)"""
    payment_id = callback.data.replace("pay_yookassa_", "")
    user_id = callback.from_user.id
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT course_id, amount FROM purchases WHERE id = ?", (int(payment_id),))
        purchase = await cursor.fetchone()
    
    if not purchase:
        await callback.answer("❌ Платёж не найден", show_alert=True)
        return
    
    course_id, amount = purchase
    course = await get_course(course_id)
    
    await callback.answer("⏳ Создаю платёж...")
    
    payment_url = await create_yookassa_payment(
        float(amount), 
        course_id, 
        int(payment_id),
        f"Курс: {course[1]}"
    )
    
    if payment_url:
        text = f"""
💳 <b>ОПЛАТА КАРТОЙ (ЮKassa)</b>

💰 Сумма: {amount}₽
📚 Курс: {course[1]}

Нажмите кнопку ниже для перехода на страницу оплаты.
Принимаются карты Visa, MasterCard, МИР.
"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить картой", url=payment_url)],
            [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_yookassa_{payment_id}")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_catalog")],
        ])
    else:
        text = "❌ Ошибка создания платежа. Попробуйте позже или выберите другой способ оплаты."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_catalog")],
        ])
    
    await track_event(user_id, "payment_method_selected", course_id, {"method": "yookassa"})
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query(lambda c: c.data.startswith("check_yookassa_"))
async def check_yookassa_status(callback: types.CallbackQuery):
    """Проверка статуса платежа YooKassa"""
    payment_id = callback.data.replace("check_yookassa_", "")
    user_id = callback.from_user.id
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT course_id, amount, status, transaction_id FROM purchases WHERE id = ?", 
            (int(payment_id),)
        )
        purchase = await cursor.fetchone()
    
    if not purchase:
        await callback.answer("❌ Платёж не найден", show_alert=True)
        return
    
    course_id, amount, status, transaction_id = purchase
    
    if status == "completed":
        await callback.answer("✅ Платёж уже подтверждён!", show_alert=True)
        return
    
    if transaction_id:
        is_paid = await verify_yookassa_payment(transaction_id)
        if is_paid:
            await update_payment_status(int(payment_id), "completed", transaction_id)
            
            course = await get_course(course_id)
            await track_event(user_id, "purchase_completed", course_id, {"amount": amount, "method": "yookassa"})
            
            # Комиссия рефереру
            referrer_id, commission = await process_referral_commission(user_id, amount)
            if referrer_id:
                try:
                    await bot.send_message(referrer_id, f"🎉 Ваш реферал купил курс!\n💰 Комиссия: {commission:.2f}₽")
                except Exception:
                    pass
            
            await callback.answer("✅ Платёж подтверждён!", show_alert=True)
            
            username = callback.from_user.username or "Аноним"
            await notify_channel(f"🎉 <b>НОВАЯ ПОКУПКА!</b>\n\n👤 @{username}\n📚 {course[1]}\n💰 {amount}₽")
            
            text = f"""
✅ <b>ПЛАТЁЖ ПРИНЯТ!</b>

Спасибо за покупку курса "{course[1]}"!

📚 Ваш курс активирован.
🎓 Напишите /mycourse чтобы начать обучение!
"""
            await callback.message.edit_text(text, reply_markup=main_menu(), parse_mode="HTML")
        else:
            await callback.answer("⏳ Платёж ещё обрабатывается. Попробуйте через минуту.", show_alert=True)
    else:
        await callback.answer("⏳ Ожидаем данные о платеже...", show_alert=True)


@dp.callback_query(lambda c: c.data.startswith("pay_webmoney_"))
async def pay_webmoney(callback: types.CallbackQuery):
    payment_id = callback.data.replace("pay_webmoney_", "")
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT course_id, amount FROM purchases WHERE id = ?", (int(payment_id),))
        purchase = await cursor.fetchone()
    
    if not purchase:
        await callback.answer("❌ Платёж не найден", show_alert=True)
        return
    
    course_id, amount = purchase
    
    text = f"""
💰 <b>ОПЛАТА ЧЕРЕЗ WebMoney</b>

💵 Сумма: {amount}₽

📌 Инструкция:
1. Откройте WebMoney
2. Отправьте перевод на кошелёк: {WEBMONEY_WALLET}
3. В комментарии укажите номер платежа: {payment_id}
4. Вернитесь и нажмите "✅ Платёж выполнен"
"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Платёж выполнен", callback_data=f"confirm_webmoney_{payment_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_catalog")],
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("confirm_"))
async def confirm_payment(callback: types.CallbackQuery):
    """
    Обработчик ручного подтверждения платежа.
    ВАЖНО: Ручные платежи требуют подтверждения администратора!
    """
    parts = callback.data.split("_")
    payment_method = parts[1]  # paypal, webmoney
    payment_id = parts[-1]
    user_id = callback.from_user.id
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT course_id, amount, status FROM purchases WHERE id = ?", 
            (int(payment_id),)
        )
        purchase = await cursor.fetchone()
    
    if not purchase:
        await callback.answer("❌ Платёж не найден", show_alert=True)
        return
    
    course_id, amount, current_status = purchase
    
    # Если уже подтверждён
    if current_status == "completed":
        await callback.answer("✅ Этот платёж уже подтверждён!", show_alert=True)
        return
    
    # Если уже ожидает проверки
    if current_status == "pending_admin":
        await callback.answer("⏳ Платёж уже отправлен на проверку администратору", show_alert=True)
        return
    
    # Устанавливаем статус "ожидает проверки админом"
    async with aiosqlite.connect("users.db") as db:
        await db.execute(
            "UPDATE purchases SET status = 'pending_admin' WHERE id = ?",
            (int(payment_id),)
        )
        await db.commit()
    
    course = await get_course(course_id)
    username = callback.from_user.username or f"user_{user_id}"
    
    # Отправляем запрос администратору
    admin_text = f"""
🔔 <b>ЗАПРОС НА ПОДТВЕРЖДЕНИЕ ПЛАТЕЖА</b>

👤 Пользователь: @{username} (ID: {user_id})
📚 Курс: {course[1]}
💰 Сумма: {amount}₽
💳 Способ: {payment_method.upper()}
🆔 ID платежа: #{payment_id}

⚠️ <b>Проверьте поступление средств перед подтверждением!</b>
"""
    
    admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_approve_{payment_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_{payment_id}")
        ]
    ])
    
    try:
        await bot.send_message(ADMIN_ID, admin_text, reply_markup=admin_keyboard, parse_mode="HTML")
    except Exception as e:
        print(f"Error notifying admin: {e}")
    
    # Сообщаем пользователю
    text = f"""
⏳ <b>ПЛАТЁЖ ОТПРАВЛЕН НА ПРОВЕРКУ</b>

📚 Курс: {course[1]}
💰 Сумма: {amount}₽
🆔 Номер платежа: #{payment_id}

Администратор проверит поступление средств и подтвердит доступ.
Обычно это занимает до 24 часов.

📱 Вы получите уведомление после подтверждения.
"""
    
    await track_event(user_id, "payment_pending_admin", course_id, {"amount": amount, "method": payment_method})
    await callback.message.edit_text(text, reply_markup=main_menu(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("admin_approve_"))
async def admin_approve_payment(callback: types.CallbackQuery):
    """Администратор подтверждает платёж"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Только администратор может подтверждать платежи", show_alert=True)
        return
    
    payment_id = callback.data.replace("admin_approve_", "")
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT user_id, course_id, amount FROM purchases WHERE id = ?", 
            (int(payment_id),)
        )
        purchase = await cursor.fetchone()
    
    if not purchase:
        await callback.answer("❌ Платёж не найден", show_alert=True)
        return
    
    buyer_id, course_id, amount = purchase
    
    # Подтверждаем платёж
    await update_payment_status(int(payment_id), "completed", f"admin_approved_{payment_id}")
    
    course = await get_course(course_id)
    
    # Трекинг покупки
    await track_event(buyer_id, "purchase_completed", course_id, {"amount": amount, "approved_by": "admin"})
    
    # Комиссия рефереру
    referrer_id, commission = await process_referral_commission(buyer_id, amount)
    if referrer_id:
        try:
            await bot.send_message(referrer_id, f"🎉 Ваш реферал купил курс!\n💰 Комиссия: {commission:.2f}₽")
        except Exception:
            pass
    
    # Уведомляем покупателя
    buyer_text = f"""
✅ <b>ПЛАТЁЖ ПОДТВЕРЖДЁН!</b>

Спасибо за покупку курса "{course[1]}"!

📚 Ваш курс активирован.
🔗 Доступ навечно.

🎓 Напишите /mycourse чтобы начать обучение!
"""
    try:
        await bot.send_message(buyer_id, buyer_text, parse_mode="HTML")
    except Exception:
        pass
    
    # Уведомление в канал
    await notify_channel(f"🎉 <b>НОВАЯ ПОКУПКА!</b>\n\n📚 {course[1]}\n💰 {amount}₽")
    
    # Обновляем сообщение админа
    await callback.message.edit_text(
        callback.message.text + "\n\n✅ <b>ПОДТВЕРЖДЕНО</b>",
        parse_mode="HTML"
    )
    await callback.answer("✅ Платёж подтверждён!")


@dp.callback_query(lambda c: c.data.startswith("admin_reject_"))
async def admin_reject_payment(callback: types.CallbackQuery):
    """Администратор отклоняет платёж"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Только администратор может отклонять платежи", show_alert=True)
        return
    
    payment_id = callback.data.replace("admin_reject_", "")
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT user_id, course_id, amount FROM purchases WHERE id = ?", 
            (int(payment_id),)
        )
        purchase = await cursor.fetchone()
    
    if not purchase:
        await callback.answer("❌ Платёж не найден", show_alert=True)
        return
    
    buyer_id, course_id, amount = purchase
    
    # Отклоняем платёж
    await update_payment_status(int(payment_id), "rejected", "admin_rejected")
    
    course = await get_course(course_id)
    
    # Уведомляем покупателя
    buyer_text = f"""
❌ <b>ПЛАТЁЖ ОТКЛОНЁН</b>

Ваш платёж за курс "{course[1]}" не был подтверждён.

Возможные причины:
• Средства не поступили
• Неверная сумма
• Не указан номер платежа

📩 Свяжитесь с поддержкой если считаете это ошибкой.
"""
    try:
        await bot.send_message(buyer_id, buyer_text, parse_mode="HTML")
    except Exception:
        pass
    
    # Обновляем сообщение админа
    await callback.message.edit_text(
        callback.message.text + "\n\n❌ <b>ОТКЛОНЕНО</b>",
        parse_mode="HTML"
    )
    await callback.answer("❌ Платёж отклонён")

@dp.callback_query(lambda c: c.data.startswith("check_payment_"))
async def check_payment_status(callback: types.CallbackQuery):
    """Проверка статуса платежа через API"""
    payment_id = callback.data.replace("check_payment_", "")
    user_id = callback.from_user.id
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT course_id, amount, status, transaction_id FROM purchases WHERE id = ?", 
            (int(payment_id),)
        )
        purchase = await cursor.fetchone()
    
    if not purchase:
        await callback.answer("❌ Платёж не найден", show_alert=True)
        return
    
    course_id, amount, status, transaction_id = purchase
    
    if status == "completed":
        course = await get_course(course_id)
        await callback.answer("✅ Платёж уже подтверждён!", show_alert=True)
        text = f"""
✅ <b>ПЛАТЁЖ ПОДТВЕРЖДЁН!</b>

📚 Курс "{course[1]}" уже активирован.
🎓 Напишите /mycourse чтобы начать обучение!
"""
        await callback.message.edit_text(text, reply_markup=main_menu(), parse_mode="HTML")
        return
    
    # Проверяем статус через PayPal API
    if PAYMENT_INTEGRATION and PAYPAL_AVAILABLE and transaction_id:
        is_paid = await verify_paypal_payment(transaction_id)
        if is_paid:
            await update_payment_status(int(payment_id), "completed", transaction_id)
            
            course = await get_course(course_id)
            await track_event(user_id, "purchase_completed", course_id, {"amount": amount, "method": "paypal"})
            
            # Комиссия рефереру
            referrer_id, commission = await process_referral_commission(user_id, amount)
            if referrer_id:
                try:
                    await bot.send_message(referrer_id, f"🎉 Ваш реферал купил курс!\n💰 Комиссия: {commission:.2f}₽")
                except Exception:
                    pass
            
            await callback.answer("✅ Платёж подтверждён!", show_alert=True)
            
            username = callback.from_user.username or "Аноним"
            await notify_channel(f"🎉 <b>НОВАЯ ПОКУПКА!</b>\n\n👤 @{username}\n📚 {course[1]}\n💰 {amount}₽")
            
            text = f"""
✅ <b>ПЛАТЁЖ ПРИНЯТ!</b>

Спасибо за покупку курса "{course[1]}"!

📚 Ваш курс активирован.
🎓 Напишите /mycourse чтобы начать обучение!
"""
            await callback.message.edit_text(text, reply_markup=main_menu(), parse_mode="HTML")
        else:
            await callback.answer("⏳ Платёж ещё не получен. Попробуйте позже.", show_alert=True)
    else:
        await callback.answer("⏳ Ожидаем подтверждение платежа...", show_alert=True)


@dp.callback_query(lambda c: c.data.startswith("view_course_"))
async def view_course(callback: types.CallbackQuery):
    course_id = callback.data.replace("view_course_", "")
    course = await get_course(course_id)
    
    if not course:
        await callback.answer("❌ Курс не найден", show_alert=True)
        return
    
    if not await has_access_to_course(callback.from_user.id, course_id):
        await callback.answer("❌ У вас нет доступа к этому курсу", show_alert=True)
        return
    
    back_btn = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад к курсам", callback_data="back_to_courses")]
    ])
    
    # Получаем уроки из БД
    lessons = await get_course_lessons(course_id)
    
    if lessons:
        text = f"📚 <b>{course[1]}</b>\n\n<b>Выберите урок:</b>\n\n"
        buttons = []
        for lesson_number, title, content in lessons:
            text += f"📖 {title}\n"
            buttons.append([InlineKeyboardButton(
                text=f"▶️ {title}",
                callback_data=f"lesson_{course_id}_{lesson_number}"
            )])
        buttons.append([InlineKeyboardButton(text="◀️ Назад к курсам", callback_data="back_to_courses")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    else:
        await callback.message.answer(f"{course[1]}", parse_mode="HTML", reply_markup=back_btn)
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_courses")
async def back_to_courses(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    purchases = await get_user_purchases(user_id)
    completed_purchases = [(course_id, status) for course_id, status in purchases if status == "completed"]
    
    text = "📚 <b>ВАШИ КУПЛЕННЫЕ КУРСЫ:</b>\n\n"
    buttons = []
    
    for course_id, _ in completed_purchases:
        course = await get_course(course_id)
        if course:
            text += f"✅ {course[1]}\n"
            buttons.append([InlineKeyboardButton(
                text=f"📖 Открыть: {course[1]}", 
                callback_data=f"view_course_{course_id}"
            )])
    
    buttons.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="back_to_menu")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "my_purchases")
async def show_purchases(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    purchases = await get_user_purchases(user_id)
    
    if not purchases:
        text = "❌ Вы ещё ничего не купили.\n\nПосетите каталог курсов!"
    else:
        text = "✅ <b>ВАШИ КУРСЫ:</b>\n\n"
        for course_id, status in purchases:
            course = await get_course(course_id)
            if course:
                status_text = "✅ Доступен" if status == "completed" else "⏳ На рассмотрении"
                text += f"• {course[1]} - {status_text}\n"
    
    buttons = []
    
    if purchases:
        pending_purchases = [p for p in purchases if p[1] == "pending"]
        if pending_purchases:
            buttons.append([InlineKeyboardButton(text="❌ Отменить платёж", callback_data="cancel_payment")])
    
    buttons.append([InlineKeyboardButton(text="❓ Условия возврата", callback_data="refund_policy")])
    buttons.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="back_to_menu")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data in ["back_to_menu", "back_to_catalog"])
async def back_to_menu(callback: types.CallbackQuery):
    if callback.data == "back_to_menu":
        text = "Главное меню:"
        keyboard = main_menu()
    else:
        text = "Каталог курсов:"
        keyboard = await catalog_menu()
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "help")
async def show_help(callback: types.CallbackQuery):
    text = """
❓ <b>ПОМОЩЬ</b>

1️⃣ <b>Как купить курс?</b>
   • Нажмите "Каталог курсов"
   • Выберите интересующий курс
   • Выберите способ оплаты
   • Следуйте инструкциям платежа

2️⃣ <b>Когда я получу доступ?</b>
   • После подтверждения платежа - сразу!

3️⃣ <b>Вопросы?</b>
   • Свяжитесь с поддержкой
"""
    await callback.message.edit_text(text, reply_markup=main_menu(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        user_count = await cursor.fetchone()
        user_count = user_count[0] if user_count else 0
        
        cursor = await db.execute("SELECT COUNT(*) FROM purchases WHERE status = 'completed'")
        sale_count = await cursor.fetchone()
        sale_count = sale_count[0] if sale_count else 0
        
        cursor = await db.execute("SELECT SUM(amount) FROM purchases WHERE status = 'completed'")
        total_money = await cursor.fetchone()
        total_money = total_money[0] if total_money and total_money[0] else 0
    
    text = f"""
📊 <b>СТАТИСТИКА</b>

👥 Пользователей: {user_count}
💰 Продаж: {sale_count}
💵 Заработок: {total_money}₽

Отличные результаты! 🚀
"""
    
    back_btn = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_btn, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_payment")
async def cancel_payment(callback: types.CallbackQuery):
    """Отмена платежа, если он ещё не подтвердил админ"""
    user_id = callback.from_user.id
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT id, course_id, amount FROM purchases WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC",
            (user_id,)
        )
        pending = await cursor.fetchall()
    
    if not pending:
        await callback.answer("❌ Нет платежей для отмены", show_alert=True)
        return
    
    if len(pending) == 1:
        payment_id, course_id, amount = pending[0]
        course = await get_course(course_id)
        
        async with aiosqlite.connect("users.db") as db:
            await db.execute("DELETE FROM purchases WHERE id = ?", (payment_id,))
            await db.commit()
        
        text = f"""
✅ <b>ПЛАТЁЖ ОТМЕНЁН</b>

Курс: {course[1]}
Сумма: {amount}₽

Платёж был отменён до подтверждения.

Если вы уже отправили деньги - обратитесь в поддержку.
"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📞 Связаться с поддержкой", callback_data="contact_support")],
            [InlineKeyboardButton(text="◀️ Главное меню", callback_data="back_to_menu")]
        ])
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    else:
        text = "❌ <b>ОТМЕНА ПЛАТЕЖА</b>\n\nВыберите платёж для отмены:\n\n"
        buttons = []
        
        for payment_id, course_id, amount in pending:
            course = await get_course(course_id)
            text += f"• {course[1]} - {amount}₽\n"
            buttons.append([InlineKeyboardButton(
                text=f"❌ Отменить: {course[1]}",
                callback_data=f"cancel_payment_confirm_{payment_id}"
            )])
        
        buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="my_purchases")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("cancel_payment_confirm_"))
async def cancel_payment_confirm(callback: types.CallbackQuery):
    """Подтверждение отмены платежа"""
    payment_id = int(callback.data.replace("cancel_payment_confirm_", ""))
    
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT course_id, amount FROM purchases WHERE id = ?", (payment_id,))
        purchase = await cursor.fetchone()
    
    if not purchase:
        await callback.answer("❌ Платёж не найден", show_alert=True)
        return
    
    course_id, amount = purchase
    course = await get_course(course_id)
    
    async with aiosqlite.connect("users.db") as db:
        await db.execute("DELETE FROM purchases WHERE id = ?", (payment_id,))
        await db.commit()
    
    text = f"""
✅ <b>ПЛАТЁЖ ОТМЕНЁН</b>

Курс: {course[1]}
Сумма: {amount}₽

Платёж отменён. Доступ к курсу закрыт.

Если вы уже отправили деньги - свяжитесь с поддержкой для возврата.
"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Поддержка", callback_data="contact_support")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="back_to_menu")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "refund_policy")
async def refund_policy(callback: types.CallbackQuery):
    """Показать политику возврата"""
    text = """
💰 <b>УСЛОВИЯ ВОЗВРАТА</b>

<b>📍 ЕСЛИ ПЛАТЁЖ ЕЩЁ НЕ ПОДТВЕРДИЛ АДМИН:</b>
✅ Отмените платёж через меню 'Мои покупки'
✅ Платёж будет удалён из системы
✅ Доступ к курсу закрыт

<b>📍 ЕСЛИ ПЛАТЁЖ УЖЕ ПОДТВЕРДЁН:</b>
⏰ Возврат в течение 7 дней
💬 Свяжитесь с поддержкой
📧 Предоставьте ID платежа
✅ Возврат денег в течение 3-5 дней

<b>📍 ГАРАНТИЯ КАЧЕСТВА:</b>
🎓 Если материалы курса вам не подошли - вернём деньги
📚 Если курс не соответствует описанию - вернём деньги
⏳ Гарантия 14 дней с момента покупки

<b>📞 СВЯЗЬ С ПОДДЕРЖКОЙ:</b>
Напишите админу для запроса возврата
"""
    
    buttons = [
        [InlineKeyboardButton(text="📞 Связаться с поддержкой", callback_data="contact_support")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="my_purchases")]
    ]
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "contact_support")
async def contact_support(callback: types.CallbackQuery):
    """Контактная информация поддержки"""
    text = f"""
📞 <b>КОНТАКТЫ ПОДДЕРЖКИ</b>

<b>Администратор:</b>
👤 ID: {ADMIN_ID}
💬 Напишите администратору для запроса возврата

<b>Email поддержки:</b>
📧 {PAYPAL_EMAIL}

<b>ВАЖНО:</b>
При обращении укажите:
• ID платежа
• Название курса
• Сумму платежа
• Причину возврата

<b>Время обработки:</b>
⏰ 1-3 дня
💰 Возврат денег: 3-5 рабочих дней
"""
    
    buttons = [
        [InlineKeyboardButton(text="◀️ Назад", callback_data="my_purchases")]
    ]
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("lesson_"))
async def view_lesson(callback: types.CallbackQuery):
    try:
        data_without_prefix = callback.data.replace("lesson_", "")
        parts = data_without_prefix.rsplit("_", 1)
        
        if len(parts) != 2:
            await callback.answer("❌ Неверный формат урока", show_alert=True)
            return
        
        course_id = parts[0]
        lesson_number = int(parts[1])
        
        course = await get_course(course_id)
        if not course:
            await callback.answer("❌ Курс не найден", show_alert=True)
            return
        
        lesson = await get_lesson(course_id, lesson_number)
        if not lesson:
            await callback.answer("❌ Урок не найден", show_alert=True)
            return
        
        back_btn = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад к урокам", callback_data=f"view_course_{course_id}")]
        ])
        
        title, content = lesson
        full_content = f"<b>{title}</b>\n\n{content}"
        await callback.message.answer(full_content, parse_mode="HTML", reply_markup=back_btn)
        await callback.answer()
    except Exception as e:
        print(f"❌ Ошибка при открытии урока: {e}")
        import traceback
        traceback.print_exc()
        await callback.answer("❌ Ошибка при открытии урока", show_alert=True)

@dp.message()
async def handle_message(message: types.Message):
    if message.text and message.text.startswith('/'):
        return
    
    if not message.text:
        return

    user_id = message.from_user.id

    if not await has_any_active_course(user_id):
        await message.answer("AI-помощник доступен только покупателям курсов. Сначала купите любой курс.")
        return

    request_count = await get_daily_requests(user_id)
    if request_count >= DAILY_AI_REQUEST_LIMIT:
        await message.answer("Исчерпаны запросы на сегодня. Попробуйте завтра.")
        return

    prompt = message.text.strip()
    cached_response = AI_RESPONSE_CACHE.get(prompt)
    if cached_response:
        await message.answer(f"{cached_response}\n\n♻️ Ответ из кеша")
        return

    status_msg = await message.answer("🤖 Секунду, обращаюсь к Gemma...")

    try:
        payload = {
            "model": "gemma2:9b",
            "prompt": prompt,
            "stream": False
        }
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(OLLAMA_URL, json=payload)
            response.raise_for_status()
            ai_text = response.json().get("response", "Ошибка ответа.")

        AI_RESPONSE_CACHE[prompt] = ai_text
        await increment_daily_request(user_id)
        await status_msg.edit_text(ai_text)
        
    except Exception as e:
        print(f"Ошибка: {e}")
        await status_msg.edit_text("❌ Произошла ошибка при связи с локальной моделью.")

async def main():
    await init_db()
    scheduler.start()
    
    print("🚀 Бот запущен!")
    print("📚 Курсы хранятся в БД (courses.db)")
    print("✅ Структура правильная: курсы → уроки в БД, не в коде!")
    print(f"CHANNEL_ID: {CHANNEL_ID}")
    print(f"ADMIN_ID: {ADMIN_ID}")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
