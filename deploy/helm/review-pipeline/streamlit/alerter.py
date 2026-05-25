"""
Модуль оповещения для дашборда отзывов.

Анализирует поток WindowAgg-сообщений и отправляет уведомления
в Telegram при обнаружении аномалий:
  • Средний рейтинг товара упал ниже 3.0
  • Доля негативных отзывов (rating < 3) > 30% за последние 10 минут

Запускается в отдельном фоновом потоке.
"""
import json
import logging
import queue
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("alerter")


# ─────────────────────────────────────────────────────────────────────
# Конфигурация по умолчанию
# ─────────────────────────────────────────────────────────────────────
DEFAULT_CHECK_INTERVAL = 60          # проверка каждые 60 сек
ALERT_WINDOW_SECONDS = 600           # анализируем последние 10 минут
RATING_THRESHOLD = 3.0               # средний рейтинг ниже этого → alert
NEGATIVE_RATIO_THRESHOLD = 0.30      # доля негатива выше этого → alert
ALERT_COOLDOWN_SECONDS = 300         # не дублировать alert чаще 5 мин


# ─────────────────────────────────────────────────────────────────────
# AlertEngine — проверка условий и отправка Telegram
# ─────────────────────────────────────────────────────────────────────

class AlertEngine:
    """Проверяет окна на аномалии и отправляет уведомления."""

    def __init__(
        self,
        bot_token: str = "",
        chat_id: str = "",
        check_interval: int = DEFAULT_CHECK_INTERVAL,
        window_seconds: int = ALERT_WINDOW_SECONDS,
        rating_threshold: float = RATING_THRESHOLD,
        negative_ratio: float = NEGATIVE_RATIO_THRESHOLD,
    ):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.check_interval = check_interval
        self.window_seconds = window_seconds
        self.rating_threshold = rating_threshold
        self.negative_ratio = negative_ratio

        # Для подавления дублирующих алертов: {product_id: timestamp}
        self._last_alert: dict[str, float] = {}
        self._alert_cooldown = ALERT_COOLDOWN_SECONDS

    # ── Проверка условий ────────────────────────────────────────────

    def check(self, windows: list[dict]) -> list[str]:
        """
        Анализирует список окон, возвращает список alert-сообщений.
        Каждое сообщение — строка (текст для Telegram).
        """
        if not windows:
            return []

        now_ts = time.time()
        cutoff_ts = (datetime.now(timezone.utc).timestamp()
                     - self.window_seconds)

        # Отфильтровываем окна за последние N секунд
        recent = []
        for w in windows:
            ws = w.get("window_start")
            if ws is None:
                continue
            # window_start может быть ISO-строкой
            if isinstance(ws, str):
                try:
                    dt = datetime.fromisoformat(ws.replace("Z", "+00:00"))
                    ts = dt.timestamp()
                except (ValueError, TypeError):
                    continue
            elif isinstance(ws, (int, float)):
                ts = ws / 1_000_000_000  # наносекунды → секунды
            else:
                continue
            if ts >= cutoff_ts:
                recent.append(w)

        if not recent:
            return []

        # Группируем по product_id
        by_product: dict[str, list[dict]] = defaultdict(list)
        for w in recent:
            pid = w.get("product_id")
            if pid:
                by_product[pid].append(w)

        alerts: list[str] = []

        for pid, group in by_product.items():
            ratings = [w.get("avg_rating", 0) for w in group]
            avg = sum(ratings) / len(ratings) if ratings else 0

            negative_count = sum(1 for r in ratings if r < 3.0)
            negative_ratio = negative_count / len(ratings) if ratings else 0

            # Проверяем cooldown
            last_time = self._last_alert.get(pid, 0)
            if now_ts - last_time < self._alert_cooldown:
                continue

            triggered = False
            messages: list[str] = []

            # Условие 1: средний рейтинг ниже порога
            if avg < self.rating_threshold:
                messages.append(
                    f"⚠️ Средний рейтинг товара *{pid}* упал до "
                    f"`{avg:.2f}` (порог: {self.rating_threshold})"
                )
                triggered = True

            # Условие 2: доля негативных отзывов превышает порог
            if negative_ratio > self.negative_ratio:
                messages.append(
                    f"🚨 Доля негативных отзывов (rating<3) по товару "
                    f"*{pid}*: `{negative_ratio:.0%}` "
                    f"(порог: {self.negative_ratio:.0%}, "
                    f"{negative_count}/{len(ratings)} отзывов)"
                )
                triggered = True

            if triggered:
                self._last_alert[pid] = now_ts
                alerts.extend(messages)

        return alerts

    # ── Отправка Telegram ───────────────────────────────────────────

    def send_telegram(self, message: str) -> bool:
        """Отправляет сообщение через Telegram Bot API. Возвращает успех."""
        if not self.bot_token or not self.chat_id:
            logger.warning(
                "Telegram не настроен: bot_token='%s...', chat_id='%s'",
                self.bot_token[:8] if self.bot_token else "",
                self.chat_id,
            )
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }

        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("Telegram alert sent: %s", message[:80])
            return True
        except requests.RequestException as e:
            logger.error("Telegram send failed: %s", e)
            return False


# ─────────────────────────────────────────────────────────────────────
# Фоновая задача: цикл проверки + отправка
# ─────────────────────────────────────────────────────────────────────

def run_alerter_loop(
    alert_queue: queue.Queue,
    bot_token: str,
    chat_id: str,
    stop_event: threading.Event = None,
) -> None:
    """
    Запускает фоновый цикл алертера.
    Читает окна из alert_queue, периодически проверяет условия,
    отправляет Telegram.

    Args:
        alert_queue: очередь с WindowAgg-сообщениями
        bot_token: Telegram Bot API токен
        chat_id: Telegram Chat ID
        stop_event: threading.Event для graceful shutdown
    """
    engine = AlertEngine(bot_token=bot_token, chat_id=chat_id)
    buffer: list[dict] = []

    while stop_event is None or not stop_event.is_set():
        # Вычитываем все накопленные сообщения
        drained = 0
        while not alert_queue.empty():
            try:
                msg = alert_queue.get_nowait()
                buffer.append(msg)
                drained += 1
            except queue.Empty:
                break

        if drained:
            logger.debug("Alerter drained %d messages (buffer: %d)",
                         drained, len(buffer))

        # Проверяем условия
        alerts = engine.check(buffer)

        # Отправляем каждое предупреждение
        for alert_text in alerts:
            engine.send_telegram(alert_text)

        # Ждём до следующей проверки
        if stop_event is not None:
            stop_event.wait(engine.check_interval)
        else:
            time.sleep(engine.check_interval)

    logger.info("Alerter stopped.")


# ─────────────────────────────────────────────────────────────────────
# Удобный запуск из потока
# ─────────────────────────────────────────────────────────────────────

def start_alerter_thread(
    alert_queue: queue.Queue,
    bot_token: str = "",
    chat_id: str = "",
) -> tuple[threading.Thread, threading.Event]:
    """
    Создаёт и запускает фоновый поток алертера.

    Returns:
        (thread, stop_event)
    """
    stop_event = threading.Event()
    t = threading.Thread(
        target=run_alerter_loop,
        args=(alert_queue, bot_token, chat_id, stop_event),
        daemon=True,
        name="alerter",
    )
    t.start()
    logger.info("Alerter thread started (check every %d sec)",
                DEFAULT_CHECK_INTERVAL)
    return t, stop_event
