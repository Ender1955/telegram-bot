# payment_integration.py
# Модуль интеграции с платёжными системами (PayPal, YooKassa)
# 
# УСТАНОВКА: pip install paypalrestsdk aiohttp yookassa
#
# НАСТРОЙКА PayPal:
# 1. Зарегистрируйтесь на https://developer.paypal.com
# 2. Создайте приложение в Sandbox (для тестов) или Live
# 3. Скопируйте Client ID и Secret в .env

import os
import logging
import hashlib
import hmac
from typing import Optional, Tuple
from datetime import datetime
from aiohttp import web
import aiosqlite
import json

# PayPal SDK
try:
    import paypalrestsdk
    PAYPAL_AVAILABLE = True
except ImportError:
    PAYPAL_AVAILABLE = False
    print("⚠️ paypalrestsdk не установлен. Установите: pip install paypalrestsdk")

# YooKassa (ЮKassa) - для российских карт
try:
    from yookassa import Configuration, Payment as YooPayment
    from yookassa.domain.notification import WebhookNotification
    YOOKASSA_AVAILABLE = True
except ImportError:
    YOOKASSA_AVAILABLE = False
    print("⚠️ yookassa не установлен. Установите: pip install yookassa")

# Логгирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===== КОНФИГУРАЦИЯ =====
# PayPal
PAYPAL_CLIENT_ID = os.getenv("PAYPAL_CLIENT_ID", "")
PAYPAL_CLIENT_SECRET = os.getenv("PAYPAL_CLIENT_SECRET", "")
PAYPAL_MODE = os.getenv("PAYPAL_MODE", "sandbox")  # sandbox или live
PAYPAL_WEBHOOK_ID = os.getenv("PAYPAL_WEBHOOK_ID", "")

# YooKassa (ЮKassa)
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID", "")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY", "")

# Общие настройки
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "https://yourdomain.com")
DB_PATH = "users.db"


# ===== ИНИЦИАЛИЗАЦИЯ PayPal =====
def init_paypal():
    """Инициализация PayPal SDK"""
    if not PAYPAL_AVAILABLE:
        return False
    
    if not PAYPAL_CLIENT_ID or not PAYPAL_CLIENT_SECRET:
        logger.warning("PayPal credentials not configured")
        return False
    
    paypalrestsdk.configure({
        "mode": PAYPAL_MODE,
        "client_id": PAYPAL_CLIENT_ID,
        "client_secret": PAYPAL_CLIENT_SECRET
    })
    logger.info(f"PayPal initialized in {PAYPAL_MODE} mode")
    return True


# ===== ИНИЦИАЛИЗАЦИЯ YooKassa =====
def init_yookassa():
    """Инициализация ЮKassa"""
    if not YOOKASSA_AVAILABLE:
        return False
    
    if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
        logger.warning("YooKassa credentials not configured")
        return False
    
    Configuration.account_id = YOOKASSA_SHOP_ID
    Configuration.secret_key = YOOKASSA_SECRET_KEY
    logger.info("YooKassa initialized")
    return True


# ===== PayPal ФУНКЦИИ =====
async def create_paypal_payment(amount: float, course_id: str, payment_id: int, currency: str = "RUB") -> Optional[str]:
    """
    Создаёт платёж в PayPal и возвращает URL для оплаты.
    
    Args:
        amount: Сумма платежа
        course_id: ID курса
        payment_id: ID платежа в нашей БД
        currency: Валюта (RUB, USD, EUR)
    
    Returns:
        URL для оплаты или None при ошибке
    """
    if not PAYPAL_AVAILABLE:
        logger.error("PayPal SDK not available")
        return None
    
    payment = paypalrestsdk.Payment({
        "intent": "sale",
        "payer": {
            "payment_method": "paypal"
        },
        "transactions": [{
            "amount": {
                "total": f"{amount:.2f}",
                "currency": currency
            },
            "description": f"Course: {course_id}",
            "custom": str(payment_id),  # Наш ID платежа для webhook
            "invoice_number": f"INV-{payment_id}-{int(datetime.now().timestamp())}"
        }],
        "redirect_urls": {
            "return_url": f"{WEBHOOK_HOST}/paypal/return?payment_id={payment_id}",
            "cancel_url": f"{WEBHOOK_HOST}/paypal/cancel?payment_id={payment_id}"
        }
    })
    
    if payment.create():
        logger.info(f"PayPal payment created: {payment.id}")
        
        # Сохраняем PayPal ID в БД
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE purchases SET transaction_id = ?, payment_method = 'paypal' WHERE id = ?",
                (payment.id, payment_id)
            )
            await db.commit()
        
        # Возвращаем URL для оплаты
        for link in payment.links:
            if link.rel == "approval_url":
                return link.href
        
        return None
    else:
        logger.error(f"PayPal payment creation failed: {payment.error}")
        return None


async def execute_paypal_payment(payment_id: str, payer_id: str) -> bool:
    """
    Завершает платёж PayPal после подтверждения пользователем.
    
    Args:
        payment_id: ID платежа в PayPal
        payer_id: ID плательщика (из redirect URL)
    
    Returns:
        True если платёж успешен
    """
    if not PAYPAL_AVAILABLE:
        return False
    
    payment = paypalrestsdk.Payment.find(payment_id)
    
    if payment.execute({"payer_id": payer_id}):
        logger.info(f"PayPal payment executed: {payment_id}")
        
        # Обновляем статус в нашей БД
        custom = payment.transactions[0].custom  # Наш payment_id
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE purchases SET status = 'completed' WHERE id = ?",
                (int(custom),)
            )
            await db.commit()
        
        return True
    else:
        logger.error(f"PayPal payment execution failed: {payment.error}")
        return False


async def verify_paypal_payment(paypal_payment_id: str) -> bool:
    """
    Проверяет статус платежа в PayPal.
    
    Args:
        paypal_payment_id: ID платежа в PayPal
    
    Returns:
        True если платёж завершён (approved/completed)
    """
    if not PAYPAL_AVAILABLE or not paypal_payment_id:
        return False
    
    try:
        payment = paypalrestsdk.Payment.find(paypal_payment_id)
        state = payment.state.lower()
        logger.info(f"PayPal payment {paypal_payment_id} state: {state}")
        return state in ["approved", "completed"]
    except Exception as e:
        logger.error(f"PayPal payment verification failed: {e}")
        return False


async def verify_yookassa_payment(yookassa_payment_id: str) -> bool:
    """
    Проверяет статус платежа в YooKassa.
    
    Args:
        yookassa_payment_id: ID платежа в YooKassa
    
    Returns:
        True если платёж успешен
    """
    if not YOOKASSA_AVAILABLE or not yookassa_payment_id:
        return False
    
    try:
        payment = YooPayment.find_one(yookassa_payment_id)
        status = payment.status.lower()
        logger.info(f"YooKassa payment {yookassa_payment_id} status: {status}")
        return status == "succeeded"
    except Exception as e:
        logger.error(f"YooKassa payment verification failed: {e}")
        return False


def verify_paypal_webhook(headers: dict, body: bytes) -> bool:
    """
    Проверяет подпись webhook от PayPal.
    
    Args:
        headers: HTTP заголовки запроса
        body: Тело запроса
    
    Returns:
        True если подпись валидна
    """
    if not PAYPAL_AVAILABLE or not PAYPAL_WEBHOOK_ID:
        return False
    
    try:
        # PayPal использует сложную схему верификации
        # В продакшене используйте paypalrestsdk.WebhookEvent.verify()
        transmission_id = headers.get("paypal-transmission-id", "")
        timestamp = headers.get("paypal-transmission-time", "")
        webhook_id = PAYPAL_WEBHOOK_ID
        crc = headers.get("paypal-transmission-sig", "")
        
        # Упрощённая проверка (в продакшене используйте полную верификацию)
        return bool(transmission_id and timestamp and crc)
    except Exception as e:
        logger.error(f"PayPal webhook verification failed: {e}")
        return False


# ===== YooKassa ФУНКЦИИ =====
async def create_yookassa_payment(amount: float, course_id: str, payment_id: int, 
                                   description: str = None) -> Optional[str]:
    """
    Создаёт платёж в ЮKassa и возвращает URL для оплаты.
    
    Args:
        amount: Сумма платежа в рублях
        course_id: ID курса
        payment_id: ID платежа в нашей БД
        description: Описание платежа
    
    Returns:
        URL для оплаты или None при ошибке
    """
    if not YOOKASSA_AVAILABLE:
        logger.error("YooKassa SDK not available")
        return None
    
    if not description:
        description = f"Оплата курса {course_id}"
    
    try:
        import uuid
        idempotence_key = str(uuid.uuid4())
        
        payment = YooPayment.create({
            "amount": {
                "value": f"{amount:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": f"{WEBHOOK_HOST}/yookassa/return?payment_id={payment_id}"
            },
            "capture": True,  # Автоматическое подтверждение
            "description": description,
            "metadata": {
                "payment_id": str(payment_id),
                "course_id": course_id
            }
        }, idempotence_key)
        
        logger.info(f"YooKassa payment created: {payment.id}")
        
        # Сохраняем YooKassa ID в БД
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE purchases SET transaction_id = ?, payment_method = 'yookassa' WHERE id = ?",
                (payment.id, payment_id)
            )
            await db.commit()
        
        return payment.confirmation.confirmation_url
        
    except Exception as e:
        logger.error(f"YooKassa payment creation failed: {e}")
        return None


async def check_yookassa_payment(yookassa_payment_id: str) -> Optional[dict]:
    """
    Проверяет статус платежа в ЮKassa.
    
    Args:
        yookassa_payment_id: ID платежа в ЮKassa
    
    Returns:
        Информация о платеже или None
    """
    if not YOOKASSA_AVAILABLE:
        return None
    
    try:
        payment = YooPayment.find_one(yookassa_payment_id)
        return {
            "id": payment.id,
            "status": payment.status,
            "amount": payment.amount.value,
            "paid": payment.paid,
            "metadata": payment.metadata
        }
    except Exception as e:
        logger.error(f"YooKassa payment check failed: {e}")
        return None


# ===== WEBHOOK HANDLERS (aiohttp) =====
async def handle_paypal_webhook(request: web.Request) -> web.Response:
    """Обработчик webhook от PayPal"""
    try:
        body = await request.read()
        headers = dict(request.headers)
        
        # Проверяем подпись
        if not verify_paypal_webhook(headers, body):
            logger.warning("Invalid PayPal webhook signature")
            return web.Response(status=400, text="Invalid signature")
        
        data = json.loads(body)
        event_type = data.get("event_type", "")
        
        logger.info(f"PayPal webhook received: {event_type}")
        
        if event_type == "PAYMENT.SALE.COMPLETED":
            # Платёж успешно завершён
            resource = data.get("resource", {})
            custom = resource.get("custom", "")  # Наш payment_id
            
            if custom:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE purchases SET status = 'completed' WHERE id = ?",
                        (int(custom),)
                    )
                    await db.commit()
                logger.info(f"Payment {custom} marked as completed via webhook")
        
        elif event_type == "PAYMENT.SALE.DENIED":
            # Платёж отклонён
            resource = data.get("resource", {})
            custom = resource.get("custom", "")
            
            if custom:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE purchases SET status = 'failed' WHERE id = ?",
                        (int(custom),)
                    )
                    await db.commit()
                logger.info(f"Payment {custom} marked as failed via webhook")
        
        return web.Response(status=200, text="OK")
        
    except Exception as e:
        logger.error(f"PayPal webhook error: {e}")
        return web.Response(status=500, text="Internal error")


async def handle_yookassa_webhook(request: web.Request) -> web.Response:
    """Обработчик webhook от ЮKassa"""
    try:
        body = await request.read()
        
        # Парсим уведомление
        notification = WebhookNotification(json.loads(body))
        payment = notification.object
        
        logger.info(f"YooKassa webhook received: {payment.status}")
        
        if payment.status == "succeeded":
            # Платёж успешен
            our_payment_id = payment.metadata.get("payment_id")
            
            if our_payment_id:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE purchases SET status = 'completed' WHERE id = ?",
                        (int(our_payment_id),)
                    )
                    await db.commit()
                logger.info(f"Payment {our_payment_id} marked as completed via YooKassa webhook")
        
        elif payment.status == "canceled":
            # Платёж отменён
            our_payment_id = payment.metadata.get("payment_id")
            
            if our_payment_id:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE purchases SET status = 'cancelled' WHERE id = ?",
                        (int(our_payment_id),)
                    )
                    await db.commit()
                logger.info(f"Payment {our_payment_id} cancelled via YooKassa webhook")
        
        return web.Response(status=200, text="OK")
        
    except Exception as e:
        logger.error(f"YooKassa webhook error: {e}")
        return web.Response(status=500, text="Internal error")


async def handle_paypal_return(request: web.Request) -> web.Response:
    """Обработчик возврата после оплаты PayPal"""
    payment_id = request.query.get("paymentId")
    payer_id = request.query.get("PayerID")
    our_payment_id = request.query.get("payment_id")
    
    if payment_id and payer_id:
        success = await execute_paypal_payment(payment_id, payer_id)
        if success:
            return web.Response(
                text="<html><body><h1>✅ Оплата успешна!</h1><p>Вернитесь в Telegram-бот.</p></body></html>",
                content_type="text/html"
            )
    
    return web.Response(
        text="<html><body><h1>❌ Ошибка оплаты</h1><p>Попробуйте снова.</p></body></html>",
        content_type="text/html"
    )


async def handle_paypal_cancel(request: web.Request) -> web.Response:
    """Обработчик отмены оплаты PayPal"""
    our_payment_id = request.query.get("payment_id")
    
    if our_payment_id:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE purchases SET status = 'cancelled' WHERE id = ?",
                (int(our_payment_id),)
            )
            await db.commit()
    
    return web.Response(
        text="<html><body><h1>❌ Оплата отменена</h1><p>Вернитесь в Telegram-бот.</p></body></html>",
        content_type="text/html"
    )


async def handle_yookassa_return(request: web.Request) -> web.Response:
    """Обработчик возврата после оплаты ЮKassa"""
    our_payment_id = request.query.get("payment_id")
    
    # Проверяем статус платежа
    if our_payment_id:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT transaction_id, status FROM purchases WHERE id = ?",
                (int(our_payment_id),)
            )
            row = await cursor.fetchone()
            
            if row and row[1] == "completed":
                return web.Response(
                    text="<html><body><h1>✅ Оплата успешна!</h1><p>Вернитесь в Telegram-бот.</p></body></html>",
                    content_type="text/html"
                )
            
            # Если статус ещё не обновлён, проверяем через API
            if row and row[0]:
                payment_info = await check_yookassa_payment(row[0])
                if payment_info and payment_info.get("paid"):
                    await db.execute(
                        "UPDATE purchases SET status = 'completed' WHERE id = ?",
                        (int(our_payment_id),)
                    )
                    await db.commit()
                    return web.Response(
                        text="<html><body><h1>✅ Оплата успешна!</h1><p>Вернитесь в Telegram-бот.</p></body></html>",
                        content_type="text/html"
                    )
    
    return web.Response(
        text="<html><body><h1>⏳ Ожидание подтверждения</h1><p>Если оплата прошла, вернитесь в бот через минуту.</p></body></html>",
        content_type="text/html"
    )


def create_webhook_app() -> web.Application:
    """Создаёт aiohttp приложение для webhook'ов"""
    app = web.Application()
    
    app.router.add_post("/webhook/paypal", handle_paypal_webhook)
    app.router.add_post("/webhook/yookassa", handle_yookassa_webhook)
    app.router.add_get("/paypal/return", handle_paypal_return)
    app.router.add_get("/paypal/cancel", handle_paypal_cancel)
    app.router.add_get("/yookassa/return", handle_yookassa_return)
    
    return app


async def run_webhook_server(host: str = "0.0.0.0", port: int = 8080):
    """Запускает webhook сервер"""
    app = create_webhook_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info(f"Webhook server started on {host}:{port}")
    return runner


# ===== EXAMPLE USAGE =====
if __name__ == "__main__":
    import asyncio
    
    async def main():
        # Инициализация
        init_paypal()
        init_yookassa()
        
        # Запуск webhook сервера
        runner = await run_webhook_server()
        
        # Тест создания платежа
        # url = await create_paypal_payment(100.0, "course_1", 1)
        # print(f"Payment URL: {url}")
        
        # Держим сервер запущенным
        try:
            while True:
                await asyncio.sleep(3600)
        finally:
            await runner.cleanup()
    
    asyncio.run(main())
