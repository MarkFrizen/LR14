"""
Streamlit-дашборд, подключающийся к NATS JetStream и читающий
агрегированные окна из темы "reviews.windowed" в реальном времени.

Использует asyncio + nats-py в фоновом потоке.
Таблица обновляется при каждом новом сообщении.
"""
import asyncio
import json
import queue
import threading
import time
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

# ─────────────────────────────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────────────────────────────
NATS_URL = "nats://nats:4222"           # по умолчанию (переопределяется в sidebar)
STREAM_NAME = "reviews"
WINDOWED_SUBJECT = "reviews.windowed"
CONSUMER_NAME = "streamlit-dashboard"

# Очередь для передачи сообщений из фонового потока -> Streamlit
_msg_queue: queue.Queue = queue.Queue()


# ─────────────────────────────────────────────────────────────────────
# Фоновый asyncio-задача: слушает NATS JetStream
# ─────────────────────────────────────────────────────────────────────
async def nats_listener(nats_url: str):
    """
    Подключается к NATS, создаёт pull-consumer на стриме reviews,
    читает сообщения и кладёт их в _msg_queue.
    """
    from nats import connect
    from nats.js import JetStreamContext

    nc = await connect(nats_url)
    js: JetStreamContext = nc.jetstream()

    # Создаём durable consumer (если уже есть — ок)
    try:
        await js.add_consumer(
            STREAM_NAME,
            config={
                "durable_name": CONSUMER_NAME,
                "ack_policy": "explicit",
                "filter_subject": WINDOWED_SUBJECT,
                "max_deliver": 3,
                "replay_policy": "instant",
                "deliver_policy": "last",  # читаем с последнего
            },
        )
    except Exception:
        pass  # уже существует

    sub = await js.pull_subscribe(WINDOWED_SUBJECT, durable=CONSUMER_NAME)

    while True:
        try:
            msgs = await sub.fetch(50, timeout=5)
            for msg in msgs:
                try:
                    data = json.loads(msg.data)
                    _msg_queue.put_nowait(data)
                except json.JSONDecodeError:
                    pass
                await msg.ack()
        except asyncio.TimeoutError:
            continue
        except Exception:
            await asyncio.sleep(1)


def _start_nats_background(nats_url: str):
    """Запускает NATS listener в фоновом потоке с собственным event loop."""
    asyncio.run(nats_listener(nats_url))


# ─────────────────────────────────────────────────────────────────────
# Streamlit-интерфейс
# ─────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="NATS Dashboard — Анализ отзывов",
        page_icon="📊",
        layout="wide",
    )

    # ── Инициализация сессионного буфера ─────────────────────────────
    if "windows" not in st.session_state:
        st.session_state.windows = []
    if "listener_started" not in st.session_state:
        st.session_state.listener_started = False

    # ── Sidebar ──────────────────────────────────────────────────────
    nats_url = st.sidebar.text_input("NATS URL", value=NATS_URL)
    st.sidebar.markdown("---")
    stats_placeholder = st.sidebar.empty()

    # Кнопка сброса
    if st.sidebar.button("🔄 Очистить данные"):
        st.session_state.windows = []

    # ── Запуск фонового NATS слушателя (один раз) ────────────────────
    if not st.session_state.listener_started:
        t = threading.Thread(
            target=_start_nats_background,
            args=(nats_url,),
            daemon=True,
        )
        t.start()
        st.session_state.listener_started = True

    # ── Вычитываем свежие сообщения из очереди ───────────────────────
    new_count = 0
    while not _msg_queue.empty():
        try:
            msg = _msg_queue.get_nowait()
            st.session_state.windows.append(msg)
            new_count += 1
        except queue.Empty:
            break

    # ── Заголовок ────────────────────────────────────────────────────
    st.title("📊 Дашборд отзывов маркетплейса")
    st.markdown(
        f"Подключён к **NATS** `{nats_url}` | "
        f"Тема **{WINDOWED_SUBJECT}**"
    )

    windows = st.session_state.windows
    total = len(windows)

    # ── Метрики (KPI) ────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Получено окон", f"{total:,}")
    with col2:
        if windows:
            uniq = len(set(w.get("product_id", "") for w in windows))
            st.metric("Уникальных товаров", f"{uniq}")
        else:
            st.metric("Уникальных товаров", "0")

    with col3:
        if windows:
            avg_r = sum(w.get("avg_rating", 0) for w in windows) / total
            st.metric("Средний рейтинг", f"{avg_r:.2f}")
        else:
            st.metric("Средний рейтинг", "—")

    with col4:
        if windows:
            total_l = sum(w.get("total_likes", 0) for w in windows)
            st.metric("Всего лайков", f"{total_l:,}")
        else:
            st.metric("Всего лайков", "0")

    # ── Статус в sidebar ─────────────────────────────────────────────
    if new_count > 0:
        stats_placeholder.info(f"📨 +{new_count} новых окон")
    else:
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        stats_placeholder.caption(f"Последняя проверка: {now} | Всего: {total}")

    # ── Если данных нет ──────────────────────────────────────────────
    if not windows:
        st.warning(
            "⏳ Ожидание данных из NATS...\n\n"
            "Убедитесь, что:\n"
            "1. Go-сборщик запущен и публикует в NATS\n"
            "2. NATS доступен по указанному URL\n"
            "3. Consumer 'streamlit-dashboard' создан на стриме 'reviews'"
        )
        time.sleep(2)
        st.rerun()
        return

    # ── Преобразуем в DataFrame для удобства ─────────────────────────
    df = pd.DataFrame(windows)

    # Нормализуем window_start: ISO строка -> datetime
    if "window_start" in df.columns:
        df["window_start_dt"] = pd.to_datetime(df["window_start"], utc=True)

    # ── Графики ──────────────────────────────────────────────────────
    g1, g2 = st.columns(2)

    with g1:
        st.subheader("🏆 Топ-20 товаров по рейтингу")
        top = (
            df.groupby("product_id")["avg_rating"]
            .mean()
            .sort_values(ascending=False)
            .head(20)
            .reset_index()
        )
        fig = px.bar(
            top,
            x="product_id",
            y="avg_rating",
            color="avg_rating",
            color_continuous_scale="RdYlGn",
            range_color=[1, 5],
        )
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with g2:
        st.subheader("📈 Динамика окон")
        if "window_start_dt" in df.columns:
            timeline = (
                df.sort_values("window_start_dt")
                .groupby("window_start_dt", as_index=False)["review_count"]
                .sum()
            )
            fig = px.line(
                timeline,
                x="window_start_dt",
                y="review_count",
                markers=True,
            )
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)

    # ── Основная таблица ─────────────────────────────────────────────
    st.subheader("📋 Таблица окон (обновляется в реальном времени)")

    # Формируем отображаемые колонки
    display_cols = {
        "product_id": "Товар",
        "avg_rating": "Рейтинг",
        "total_likes": "Лайки",
        "review_count": "Отзывов",
        "window_start": "Время окна",
    }

    display = pd.DataFrame()
    for col, label in display_cols.items():
        if col in df.columns:
            display[label] = df[col]

    # Форматирование
    if "Время окна" in display.columns:
        try:
            display["Время окна"] = pd.to_datetime(
                display["Время окна"], utc=True
            ).dt.strftime("%H:%M:%S")
        except Exception:
            pass

    display.index = range(1, len(display) + 1)

    st.dataframe(
        display.sort_index(ascending=False),
        use_container_width=True,
        height=500,
    )

    # ── Последнее окно (детали) ──────────────────────────────────────
    with st.expander("🔍 Последнее полученное окно"):
        last = windows[-1]
        st.json(last)

    # ── Автообновление ───────────────────────────────────────────────
    time.sleep(1)
    st.rerun()


if __name__ == "__main__":
    main()
