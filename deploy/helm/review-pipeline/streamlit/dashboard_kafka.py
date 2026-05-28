#!/usr/bin/env python3
"""
Streamlit-дашборд для визуализации агрегированных отзывов из Kafka.

Читает агрегаты из топика 'reviews.aggregated' (публикуемые Python-анализатором),
отображает графики в реальном времени.

Запуск:
  .venv/bin/pip install streamlit pandas plotly kafka-python
  .venv/bin/streamlit run deploy/helm/review-pipeline/streamlit/dashboard_kafka.py \
      -- --bootstrap-servers localhost:9092

Особенности:
  - Polling Kafka в фоновом потоке (synchronous KafkaConsumer)
  - Plotly-графики: avg_rating, negative_share, review_count, latency
  - Метрика задержки end-to-end (collection → dashboard)
  - Фильтр по товару
  - Автообновление каждые REFRESH_SECONDS
"""

import json
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from kafka import KafkaConsumer

# ══════════════════════════════════════════════════════════════════════
#  Настройки
# ══════════════════════════════════════════════════════════════════════

AGGREGATED_TOPIC = "reviews.aggregated"
CONSUMER_GROUP = "streamlit-dashboard"
REFRESH_SECONDS = 5          # auto-refresh интервал
MAX_AGGREGATES = 2000        # максимум записей в памяти

# Для latency: показываем последние N минут на графике
LATENCY_CHART_MINUTES = 30

# ── Очередь для передачи сообщений из фонового потока в Streamlit ──
_msg_queue: queue.Queue = queue.Queue(maxsize=5000)
_listener_started = False


# ══════════════════════════════════════════════════════════════════════
#  Фоновый Kafka listener (synchronous consumer, отдельный поток)
# ══════════════════════════════════════════════════════════════════════

def kafka_listener(bootstrap_servers: str, stop_event: threading.Event):
    """Фоновый поток: читает Kafka и кладёт сообщения в очередь.

    Работает с синхронным KafkaConsumer, ставит в очередь
    словари-агрегаты для основного потока Streamlit.
    """
    consumer = KafkaConsumer(
        AGGREGATED_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode() if k else None,
        value_deserializer=lambda v: json.loads(v.decode()) if v else None,
        session_timeout_ms=30000,
        heartbeat_interval_ms=5000,
        max_poll_records=500,
    )

    print(f"[KAFKA-LISTENER] Started, subscribed to '{AGGREGATED_TOPIC}'")

    while not stop_event.is_set():
        try:
            # poll каждые 2 секунды
            msgs = consumer.poll(timeout_ms=2000, max_records=500)
            if not msgs:
                continue

            for _tp, records in msgs.items():
                for msg in records:
                    if msg.value is None:
                        continue
                    try:
                        _msg_queue.put_nowait(msg.value)
                    except queue.Full:
                        # очередь переполнена — удаляем старые записи
                        try:
                            while _msg_queue.qsize() >= 5000:
                                _msg_queue.get_nowait()
                            _msg_queue.put_nowait(msg.value)
                        except queue.Empty:
                            pass
        except Exception as e:
            print(f"[KAFKA-LISTENER] Poll error: {e}")
            time.sleep(1)

    consumer.close()
    print("[KAFKA-LISTENER] Stopped")


# ══════════════════════════════════════════════════════════════════════
#  Streamlit UI
# ══════════════════════════════════════════════════════════════════════

def init_session_state():
    """Инициализация session_state."""
    if "aggregates" not in st.session_state:
        st.session_state.aggregates = []     # list[dict]
    if "last_update" not in st.session_state:
        st.session_state.last_update = time.time()
    if "selected_product" not in st.session_state:
        st.session_state.selected_product = "All"
    if "listener_started" not in st.session_state:
        st.session_state.listener_started = False


def start_listener(bootstrap_servers: str):
    """Запускает фоновый Kafka listener (однократно)."""
    if st.session_state.listener_started:
        return

    stop_event = threading.Event()
    thread = threading.Thread(
        target=kafka_listener,
        args=(bootstrap_servers, stop_event),
        daemon=True,
    )
    thread.start()
    st.session_state.listener_started = True
    st.session_state._stop_event = stop_event
    st.session_state._listener_thread = thread
    print(f"[DASHBOARD] Kafka listener thread started")


def drain_queue():
    """Вычитывает все сообщения из очереди в session_state."""
    count = 0
    while not _msg_queue.empty():
        try:
            agg = _msg_queue.get_nowait()
            st.session_state.aggregates.append(agg)
            count += 1
        except queue.Empty:
            break

    # Ограничиваем размер
    if len(st.session_state.aggregates) > MAX_AGGREGATES:
        st.session_state.aggregates = \
            st.session_state.aggregates[-MAX_AGGREGATES:]

    if count > 0:
        st.session_state.last_update = time.time()


def get_df() -> pd.DataFrame:
    """Возвращает DataFrame со всеми агрегатами."""
    if not st.session_state.aggregates:
        return pd.DataFrame()
    df = pd.DataFrame(st.session_state.aggregates)

    # Парсинг времени
    for col in ("window_start", "window_end", "computed_at", "max_review_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


# ══════════════════════════════════════════════════════════════════════
#  Графики
# ══════════════════════════════════════════════════════════════════════

def plot_avg_rating(df: pd.DataFrame):
    """Линейный график среднего рейтинга по времени (один product_id или все)."""
    if df.empty:
        st.info("Нет данных для графика среднего рейтинга")
        return

    fig = go.Figure()
    products = df["product_id"].unique()

    for pid in products:
        pdf = df[df["product_id"] == pid].sort_values("computed_at")
        fig.add_trace(go.Scatter(
            x=pdf["computed_at"],
            y=pdf["avg_rating"],
            mode="lines+markers",
            name=pid,
            line=dict(width=2),
            marker=dict(size=5),
        ))

    fig.update_layout(
        title="Средний рейтинг по товарам (скользящее окно 5 мин)",
        xaxis_title="Время",
        yaxis_title="Средний рейтинг",
        yaxis=dict(range=[1, 5]),
        hovermode="x unified",
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
        ),
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_negative_share(df: pd.DataFrame):
    """Доля негативных отзывов (rating < 3) по товарам."""
    if df.empty:
        st.info("Нет данных для графика негативных отзывов")
        return

    fig = go.Figure()
    products = df["product_id"].unique()

    for pid in products:
        pdf = df[df["product_id"] == pid].sort_values("computed_at")
        fig.add_trace(go.Scatter(
            x=pdf["computed_at"],
            y=pdf["negative_share"] * 100,
            mode="lines+markers",
            name=pid,
            line=dict(width=2),
            marker=dict(size=5),
        ))

    fig.update_layout(
        title="Доля негативных отзывов (rating < 3)",
        xaxis_title="Время",
        yaxis_title="Негативных (%)",
        yaxis=dict(ticksuffix="%"),
        hovermode="x unified",
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
        ),
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_review_count(df: pd.DataFrame):
    """Количество отзывов в окне по товарам (stacked bar)."""
    if df.empty:
        st.info("Нет данных для графика количества отзывов")
        return

    # Берём только последнюю точку каждого товара
    latest = df.sort_values("computed_at").groupby("product_id").last().reset_index()
    top = latest.nlargest(20, "review_count")

    fig = px.bar(
        top,
        x="product_id",
        y="review_count",
        color="avg_rating",
        color_continuous_scale="RdYlGn",
        range_color=[1, 5],
        title="Товары по количеству отзывов в текущем окне (топ-20)",
        labels={"product_id": "Товар", "review_count": "Отзывов", "avg_rating": "Ср. рейтинг"},
        height=400,
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)


def plot_latency(df: pd.DataFrame):
    """График задержки end-to-end (collection → dashboard).

    latency_sec = computed_at - max_review_date (в секундах).
    """
    if df.empty or "latency_sec" not in df.columns:
        st.info("Нет данных для графика задержки")
        return

    # Усредняем latency по всем товарам на каждый момент computed_at
    latency_df = (
        df.groupby("computed_at")["latency_sec"]
        .mean()
        .reset_index()
        .sort_values("computed_at")
    )

    # Ограничиваем последними N минутами
    if len(latency_df) > 0:
        cutoff = latency_df["computed_at"].max() - pd.Timedelta(
            minutes=LATENCY_CHART_MINUTES)
        latency_df = latency_df[latency_df["computed_at"] >= cutoff]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=latency_df["computed_at"],
        y=latency_df["latency_sec"],
        mode="lines+markers",
        name="Avg latency",
        line=dict(color="red", width=2),
        marker=dict(size=6, color="red"),
        fill="tozeroy",
        fillcolor="rgba(255,0,0,0.1)",
    ))

    # Текущая latency (последнее значение)
    if not latency_df.empty:
        current = latency_df["latency_sec"].iloc[-1]
        fig.add_hline(
            y=current,
            line_dash="dash",
            line_color="orange",
            annotation_text=f"Текущая: {current:.0f}с",
            annotation_position="bottom right",
        )

    fig.update_layout(
        title=f"Задержка end-to-end (последние {LATENCY_CHART_MINUTES} мин)",
        xaxis_title="Время",
        yaxis_title="Задержка (сек)",
        hovermode="x unified",
        height=350,
    )
    st.plotly_chart(fig, use_container_width=True)


def display_kpi(df: pd.DataFrame):
    """KPI-метрики в верхней части дашборда."""
    col1, col2, col3, col4, col5 = st.columns(5)

    total = len(st.session_state.aggregates)
    col1.metric("Всего агрегатов", f"{total}")

    if not df.empty:
        latest = df.sort_values("computed_at").groupby("product_id").last()
        avg_rating_all = latest["avg_rating"].mean()
        col2.metric("Средний рейтинг", f"{avg_rating_all:.2f}")

        total_reviews = latest["review_count"].sum()
        col3.metric("Отзывов в окне", f"{int(total_reviews)}")

        avg_neg = latest["negative_share"].mean() * 100
        col4.metric("Негативных среднее", f"{avg_neg:.1f}%")

        # Последняя latency
        if "latency_sec" in df.columns:
            last_latency = df["latency_sec"].iloc[-1] if len(df) > 0 else 0
            col5.metric("Задержка (с)", f"{last_latency:.1f}",
                        delta_color="inverse")
    else:
        col2.metric("Средний рейтинг", "—")
        col3.metric("Отзывов в окне", "—")
        col4.metric("Негативных среднее", "—")
        col5.metric("Задержка (с)", "—")


def display_table(df: pd.DataFrame):
    """Таблица последних агрегатов."""
    if df.empty:
        st.info("Нет данных для таблицы")
        return

    # Последние 50 записей
    table = df.sort_values("computed_at", ascending=False).head(50)

    # Форматирование
    display_cols = [
        "product_id", "review_count", "avg_rating",
        "negative_share", "latency_sec", "computed_at",
    ]
    available = [c for c in display_cols if c in table.columns]

    display_df = table[available].copy()
    if "negative_share" in display_df.columns:
        display_df["negative_share"] = display_df["negative_share"].apply(
            lambda x: f"{x:.1%}")
    if "avg_rating" in display_df.columns:
        display_df["avg_rating"] = display_df["avg_rating"].apply(
            lambda x: f"{x:.2f}")
    if "latency_sec" in display_df.columns:
        display_df["latency_sec"] = display_df["latency_sec"].apply(
            lambda x: f"{x:.1f}s")
    if "computed_at" in display_df.columns:
        display_df["computed_at"] = display_df["computed_at"].dt.strftime(
            "%H:%M:%S")

    display_df = display_df.rename(columns={
        "product_id": "Товар",
        "review_count": "Отзывов",
        "avg_rating": "Рейтинг",
        "negative_share": "Негатив",
        "latency_sec": "Задержка",
        "computed_at": "Время",
    })

    st.subheader("Последние агрегаты")
    st.dataframe(display_df, use_container_width=True, height=400)


# ══════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(
        page_title="Review Pipeline Dashboard (Kafka)",
        page_icon="📊",
        layout="wide",
    )

    st.title("📊 Review Pipeline — Kafka")
    st.caption(
        "Полный конвейер: Go-сборщик → Kafka → Python (окно + агрегация) "
        "→ Kafka → Streamlit дашборд"
    )

    # Sidebar
    bootstrap_servers = st.sidebar.text_input(
        "Kafka bootstrap servers",
        value="localhost:9092",
    )

    init_session_state()

    # Запуск listener
    start_listener(bootstrap_servers)

    # Вычитываем очередь
    drain_queue()

    df = get_df()

    # ── Фильтр по товару ────────────────────────────────────────
    if not df.empty:
        products = ["All"] + sorted(df["product_id"].unique().tolist())
        selected = st.sidebar.selectbox(
            "Фильтр по товару",
            options=products,
            index=products.index(st.session_state.selected_product)
            if st.session_state.selected_product in products
            else 0,
        )
        st.session_state.selected_product = selected

        if selected != "All":
            df = df[df["product_id"] == selected]

    st.sidebar.divider()
    st.sidebar.caption(
        f"Агрегатов в памяти: {len(st.session_state.aggregates)}"
    )
    last_upd = datetime.fromtimestamp(
        st.session_state.last_update).strftime("%H:%M:%S")
    st.sidebar.caption(f"Последнее обновление: {last_upd}")

    if st.sidebar.button("🔄 Очистить данные"):
        st.session_state.aggregates = []
        st.rerun()

    # ── KPI ──────────────────────────────────────────────────────
    display_kpi(df)

    # ── Графики ──────────────────────────────────────────────────
    if df.empty:
        st.info(
            "⏳ Ожидание агрегатов из Kafka… "
            "Убедитесь, что конвейер запущен:\n\n"
            "1. Go-сборщик → Kafka ('reviews.raw')\n"
            "2. Python-анализатор → Kafka ('reviews.aggregated')\n"
            "3. Данный дашборд"
        )
    else:
        col_left, col_right = st.columns(2)
        with col_left:
            plot_avg_rating(df)
        with col_right:
            plot_negative_share(df)

        st.divider()
        col_left2, col_right2 = st.columns(2)
        with col_left2:
            plot_review_count(df)
        with col_right2:
            plot_latency(df)

        st.divider()
        display_table(df)

    # ── Auto-refresh ─────────────────────────────────────────────
    st.sidebar.divider()
    auto_refresh = st.sidebar.checkbox("Автообновление", value=True)
    if auto_refresh:
        time.sleep(REFRESH_SECONDS)
        st.rerun()


if __name__ == "__main__":
    main()
