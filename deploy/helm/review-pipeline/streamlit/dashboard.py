"""
Streamlit-дашборд, подключающийся к NATS JetStream и читающий
агрегированные окна из темы "reviews.windowed" в реальном времени.

Использует asyncio + nats-py в фоновом потоке.
Интерактивные графики Plotly с выпадающим списком товаров.
Автообновление каждые 5 секунд.
"""
import asyncio
import json
import logging
import queue
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from alerter import start_alerter_thread

# ─────────────────────────────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────────────────────────────
NATS_URL = "nats://nats:4222"
STREAM_NAME = "reviews"
WINDOWED_SUBJECT = "reviews.windowed"
CONSUMER_NAME = "streamlit-dashboard"
REFRESH_SECONDS = 5

# ── Очереди: dashboard + alerter ──────────────────────────────────────
_msg_queue: queue.Queue = queue.Queue()
_alert_queue: queue.Queue = queue.Queue()

# Логгер для алертов (отображается в sidebar)
if "alert_log" not in st.session_state:
    st.session_state.alert_log = []


# ─────────────────────────────────────────────────────────────────────
# Фоновый asyncio-задача: слушает NATS JetStream
# ─────────────────────────────────────────────────────────────────────
async def nats_listener(nats_url: str):
    from nats import connect
    from nats.js import JetStreamContext

    nc = await connect(nats_url)
    js: JetStreamContext = nc.jetstream()

    try:
        await js.add_consumer(
            STREAM_NAME,
            config={
                "durable_name": CONSUMER_NAME,
                "ack_policy": "explicit",
                "filter_subject": WINDOWED_SUBJECT,
                "max_deliver": 3,
                "replay_policy": "instant",
                "deliver_policy": "last",
            },
        )
    except Exception:
        pass

    sub = await js.pull_subscribe(WINDOWED_SUBJECT, durable=CONSUMER_NAME)

    while True:
        try:
            msgs = await sub.fetch(50, timeout=5)
            for msg in msgs:
                try:
                    data = json.loads(msg.data)
                    _msg_queue.put_nowait(data)
                    _alert_queue.put_nowait(data)
                except json.JSONDecodeError:
                    pass
                await msg.ack()
        except asyncio.TimeoutError:
            continue
        except Exception:
            await asyncio.sleep(1)


def _start_nats_background(nats_url: str):
    asyncio.run(nats_listener(nats_url))


# ─────────────────────────────────────────────────────────────────────
# Вспомогательные функции для графиков
# ─────────────────────────────────────────────────────────────────────

def _prepare_df(windows: list) -> pd.DataFrame:
    """Преобразует список словарей в pd.DataFrame с парсингом window_start."""
    df = pd.DataFrame(windows)
    if df.empty:
        return df
    if "window_start" in df.columns:
        df["window_start_dt"] = pd.to_datetime(df["window_start"], utc=True)
    return df


def line_chart_rating(df: pd.DataFrame, product_id: str) -> go.Figure:
    """
    Линейный график изменения среднего рейтинга по времени
    для выбранного product_id.
    """
    sub = df[df["product_id"] == product_id].copy()

    if sub.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="Нет данных для выбранного товара",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="gray"),
        )
        fig.update_layout(height=350)
        return fig

    sub = sub.sort_values("window_start_dt")

    # Определяем цвет: зелёный (≥4), жёлтый (3–4), красный (<3)
    colors = ["#2ecc71" if r >= 4 else "#f1c40f" if r >= 3 else "#e74c3c"
              for r in sub["avg_rating"]]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=sub["window_start_dt"],
        y=sub["avg_rating"],
        mode="lines+markers",
        name=product_id,
        marker=dict(size=8, color=colors, line=dict(width=1, color="black")),
        line=dict(color="#3498db", width=2),
        hovertemplate="<b>%{x|%H:%M:%S}</b><br>Рейтинг: %{y:.2f}<extra></extra>",
    ))

    fig.update_layout(
        title=f"📈 Средний рейтинг: {product_id}",
        xaxis_title="Время",
        yaxis_title="Средний рейтинг",
        yaxis=dict(range=[0.5, 5.5], dtick=0.5),
        height=350,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=40, b=20),
    )
    fig.add_hline(y=4, line_dash="dash", line_color="green", opacity=0.3)
    fig.add_hline(y=3, line_dash="dash", line_color="orange", opacity=0.3)
    return fig


def histogram_review_count(df: pd.DataFrame) -> go.Figure:
    """
    Гистограмма распределения количества отзывов по товарам.
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="Нет данных",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="gray"),
        )
        fig.update_layout(height=350)
        return fig

    hist_data = (
        df.groupby("product_id", as_index=False)["review_count"]
        .sum()
        .sort_values("review_count", ascending=False)
    )

    fig = px.bar(
        hist_data,
        x="product_id",
        y="review_count",
        color="review_count",
        color_continuous_scale="Blues",
        labels={"product_id": "Товар", "review_count": "Всего отзывов"},
    )
    fig.update_layout(
        title="📊 Распределение отзывов по товарам",
        height=350,
        xaxis_tickangle=-45,
        hovermode="x",
        margin=dict(l=20, r=20, t=40, b=80),
    )
    fig.update_traces(
        hovertemplate="<b>%{x}</b><br>Отзывов: %{y}<extra></extra>",
    )
    return fig


# ─────────────────────────────────────────────────────────────────────
# Анализ аномалий (для отображения в sidebar)
# ─────────────────────────────────────────────────────────────────────

ALERT_WINDOW_SECONDS = 600   # 10 минут
RATING_THRESHOLD = 3.0
NEGATIVE_RATIO_THRESHOLD = 0.30


def compute_anomalies(windows: list[dict]) -> list[str]:
    """
    Проверяет окна на аномалии за последние 10 минут.
    Возвращает список коротких сообщений (без отправки Telegram).
    """
    if not windows:
        return []

    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - ALERT_WINDOW_SECONDS

    # Фильтр: только последние 10 минут
    recent = []
    for w in windows:
        ws = w.get("window_start")
        if ws is None:
            continue
        if isinstance(ws, str):
            try:
                dt = datetime.fromisoformat(ws.replace("Z", "+00:00"))
                ts = dt.timestamp()
            except (ValueError, TypeError):
                continue
        elif isinstance(ws, (int, float)):
            ts = ws / 1_000_000_000
        else:
            continue
        if ts >= cutoff:
            recent.append(w)

    if not recent:
        return []

    from collections import defaultdict
    by_product: dict[str, list[dict]] = defaultdict(list)
    for w in recent:
        pid = w.get("product_id")
        if pid:
            by_product[pid].append(w)

    result: list[str] = []
    for pid, group in by_product.items():
        ratings = [w.get("avg_rating", 0) for w in group]
        avg = sum(ratings) / len(ratings) if ratings else 0
        negative = sum(1 for r in ratings if r < 3.0)
        neg_ratio = negative / len(ratings) if ratings else 0

        if avg < RATING_THRESHOLD:
            result.append(
                f"{pid}: ср.рейтинг {avg:.2f} (<{RATING_THRESHOLD})"
            )
        if neg_ratio > NEGATIVE_RATIO_THRESHOLD:
            result.append(
                f"{pid}: негатив {neg_ratio:.0%} "
                f"({negative}/{len(ratings)})"
            )

    return result


# ─────────────────────────────────────────────────────────────────────
# Streamlit-интерфейс
# ─────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="NATS Dashboard — Анализ отзывов",
        page_icon="📊",
        layout="wide",
    )

    # ── Инициализация сессионного состояния ──────────────────────────
    if "windows" not in st.session_state:
        st.session_state.windows = []
    if "listener_started" not in st.session_state:
        st.session_state.listener_started = False
    if "selected_product" not in st.session_state:
        st.session_state.selected_product = None
    if "alerter_started" not in st.session_state:
        st.session_state.alerter_started = False

    # ── Sidebar ──────────────────────────────────────────────────────
    with st.sidebar:
        st.header("🔗 Подключение")
        nats_url = st.text_input("NATS URL", value=NATS_URL)
        st.caption(f"Тема: `{WINDOWED_SUBJECT}`")

        st.divider()
        st.header("🔔 Оповещения Telegram")

        bot_token = st.text_input(
            "Bot Token",
            value=st.session_state.get("bot_token", ""),
            type="password",
            key="bot_token_input",
            help="Получить у t.me/BotFather",
        )
        chat_id = st.text_input(
            "Chat ID",
            value=st.session_state.get("chat_id", ""),
            key="chat_id_input",
            help="ID чата/группы для уведомлений",
        )

        # Сохраняем введённые значения в session_state
        st.session_state.bot_token = bot_token
        st.session_state.chat_id = chat_id

        # Статус алертера
        alert_configured = bool(bot_token and chat_id)
        if alert_configured:
            st.success("✅ Telegram настроен")
        else:
            st.warning("⚠️ Укажите Bot Token и Chat ID для алертов")

        st.divider()

        if st.button("🔄 Очистить данные", use_container_width=True):
            st.session_state.windows = []
            st.rerun()

        st.divider()

        # Статус подключения
        stats_placeholder = st.empty()
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        stats_placeholder.caption(
            f"🟢 Слушатель активен\n"
            f"Последняя проверка: {now}\n"
            f"Всего окон: {len(st.session_state.windows)}"
        )

        # ── Журнал алертов ──────────────────────────────────────────
        if st.session_state.alert_log:
            st.divider()
            st.caption("📋 Последние оповещения")
            for entry in st.session_state.alert_log[-5:]:
                st.caption(f"🔔 {entry}")

        # ── Текущие аномалии (preview без Telegram) ──────────────────
        if has_data and total > 0:
            st.divider()
            st.caption("⚠️ Текущие аномалии")
            anomalies = compute_anomalies(windows)
            if anomalies:
                for a in anomalies:
                    st.caption(f"🚨 {a}")
            else:
                st.caption("✅ Всё в норме")

    # ── Запуск фонового NATS слушателя (один раз) ────────────────────
    if not st.session_state.listener_started:
        t = threading.Thread(
            target=_start_nats_background,
            args=(nats_url,),
            daemon=True,
        )
        t.start()
        st.session_state.listener_started = True

    # ── Запуск алертера (один раз) ───────────────────────────────────
    bot_token = st.session_state.get("bot_token", "")
    chat_id = st.session_state.get("chat_id", "")
    if not st.session_state.alerter_started and bot_token and chat_id:
        _alert_thread, _alert_stop = start_alerter_thread(
            _alert_queue,
            bot_token=bot_token,
            chat_id=chat_id,
        )
        st.session_state.alerter_started = True
        st.session_state.alert_stop = _alert_stop

    # ── Вычитываем свежие сообщения из очереди ───────────────────────
    new_count = 0
    while not _msg_queue.empty():
        try:
            msg = _msg_queue.get_nowait()
            st.session_state.windows.append(msg)
            new_count += 1
        except queue.Empty:
            break

    windows = st.session_state.windows
    total = len(windows)

    # ── Обновляем статус если были новые сообщения ───────────────────
    if new_count > 0:
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        stats_placeholder.caption(
            f"🟢 Слушатель активен\n"
            f"📨 +{new_count} новых окон\n"
            f"Последняя проверка: {now}\n"
            f"Всего окон: {total}"
        )

    # ── Заголовок ────────────────────────────────────────────────────
    st.title("📊 Дашборд отзывов маркетплейса")
    st.markdown(
        f"Подключён к **NATS** `{nats_url}` | "
        f"Тема **{WINDOWED_SUBJECT}** | "
        f"Автообновление каждые **{REFRESH_SECONDS} с**"
    )

    # ── Преобразуем в DataFrame ─────────────────────────────────────
    df = _prepare_df(windows)
    has_data = not df.empty

    # ── Метрики (KPI) ────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Получено окон", f"{total:,}")
    with col2:
        uniq = df["product_id"].nunique() if has_data else 0
        st.metric("Уникальных товаров", f"{uniq}")
    with col3:
        avg_r = df["avg_rating"].mean() if has_data else None
        st.metric("Средний рейтинг", f"{avg_r:.2f}" if avg_r is not None else "—")
    with col4:
        total_l = int(df["total_likes"].sum()) if has_data else 0
        st.metric("Всего лайков", f"{total_l:,}")

    # ── Если данных нет ──────────────────────────────────────────────
    if not has_data:
        st.warning(
            "⏳ Ожидание данных из NATS...\n\n"
            "Убедитесь, что:\n"
            "1. Go-сборщик запущен и публикует в NATS\n"
            "2. NATS доступен по указанному URL\n"
            "3. Consumer 'streamlit-dashboard' создан на стриме 'reviews'"
        )
        time.sleep(REFRESH_SECONDS)
        st.rerun()
        return

    # ═════════════════════════════════════════════════════════════════
    # Выпадающий список для выбора товара
    # ═════════════════════════════════════════════════════════════════
    products = sorted(df["product_id"].unique())

    # Сохраняем выбор в session_state, чтобы он не сбрасывался при rerun
    selected_idx = 0
    if st.session_state.selected_product in products:
        selected_idx = products.index(st.session_state.selected_product)

    selected_product = st.selectbox(
        "🔍 Выберите товар для детального анализа:",
        options=products,
        index=selected_idx,
        key="product_selector",
    )
    st.session_state.selected_product = selected_product

    # ═════════════════════════════════════════════════════════════════
    # 1-й ряд: два интерактивных графика
    # ═════════════════════════════════════════════════════════════════
    col_left, col_right = st.columns(2)

    with col_left:
        fig_line = line_chart_rating(df, selected_product)
        st.plotly_chart(fig_line, use_container_width=True, key="line_chart")

    with col_right:
        fig_hist = histogram_review_count(df)
        st.plotly_chart(fig_hist, use_container_width=True, key="hist_chart")

    # ═════════════════════════════════════════════════════════════════
    # 2-й ряд: два дополнительных графика (для полноты картины)
    # ═════════════════════════════════════════════════════════════════
    col_left2, col_right2 = st.columns(2)

    with col_left2:
        # Топ-20 товаров по среднему рейтингу
        st.subheader("🏆 Топ-20 товаров по рейтингу")
        top_df = (
            df.groupby("product_id")["avg_rating"]
            .mean()
            .sort_values(ascending=False)
            .head(20)
            .reset_index()
        )
        fig_top = px.bar(
            top_df,
            x="product_id",
            y="avg_rating",
            color="avg_rating",
            color_continuous_scale="RdYlGn",
            range_color=[1, 5],
            labels={"product_id": "Товар", "avg_rating": "Средний рейтинг"},
        )
        fig_top.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
        st.plotly_chart(fig_top, use_container_width=True, key="top20_bar")

    with col_right2:
        # Динамика окон по времени (все товары)
        st.subheader("📈 Суммарные отзывы по времени")
        if "window_start_dt" in df.columns:
            timeline = (
                df.sort_values("window_start_dt")
                .groupby("window_start_dt", as_index=False)["review_count"]
                .sum()
            )
            fig_timeline = px.line(
                timeline,
                x="window_start_dt",
                y="review_count",
                markers=True,
                labels={
                    "window_start_dt": "Время",
                    "review_count": "Всего отзывов",
                },
            )
            fig_timeline.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=20, b=20),
            )
            st.plotly_chart(fig_timeline, use_container_width=True, key="timeline")

    # ═════════════════════════════════════════════════════════════════
    # Таблица окон (реального времени)
    # ═════════════════════════════════════════════════════════════════
    st.subheader("📋 Таблица окон (обновляется в реальном времени)")

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
        height=400,
    )

    # ── Последнее окно (JSON) ─────────────────────────────────────
    with st.expander("🔍 Последнее полученное окно (JSON)"):
        st.json(windows[-1])

    # ── Автообновление ───────────────────────────────────────────────
    time.sleep(REFRESH_SECONDS)
    st.rerun()


if __name__ == "__main__":
    main()
