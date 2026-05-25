import streamlit as st
import polars as pl
import plotly.express as px
import os
import time

st.set_page_config(page_title="Review Pipeline Dashboard", layout="wide")

st.title("📊 Review Pipeline Dashboard")
st.markdown("Мониторинг ETL-конвейера анализа отзывов маркетплейсов")

# ── Автообновление ────────────────────────────────────────────────────
auto_refresh = st.sidebar.checkbox("Автообновление", value=True)
refresh_interval = st.sidebar.slider("Интервал (сек)", 5, 60, 10)

placeholder = st.empty()

PARQUET_PATH = "/data/aggregated_windows.parquet"


def load_data() -> pl.DataFrame | None:
    if not os.path.exists(PARQUET_PATH):
        return None
    try:
        return pl.read_parquet(PARQUET_PATH)
    except Exception:
        return None


def dashboard():
    df = load_data()

    if df is None or df.is_empty():
        placeholder.warning("⏳ Данные ещё не поступили. Ожидание...")
        return

    # ── KPI ────────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Всего окон", f"{len(df):,}")
    with col2:
        st.metric("Уникальных товаров", f"{df['product_id'].n_unique():,}")
    with col3:
        st.metric("Средний рейтинг", f"{df['avg_rating'].mean():.2f}")
    with col4:
        st.metric("Всего лайков", f"{df['total_likes'].sum():,}")

    # ── Графики ────────────────────────────────────────────────────────
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Рейтинг по товарам (топ-20)")
        top_products = (
            df.group_by("product_id")
            .agg(pl.mean("avg_rating").alias("avg_rating"))
            .sort("avg_rating", descending=True)
            .head(20)
        )
        fig = px.bar(
            top_products.to_pandas(),
            x="product_id",
            y="avg_rating",
            color="avg_rating",
            color_continuous_scale="RdYlGn",
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("Динамика окон")
        if "window_start" in df.columns:
            timeline = (
                df.sort("window_start")
                .with_columns(pl.col("window_start").cast(pl.Datetime).alias("ts"))
            )
            fig = px.line(
                timeline.to_pandas(),
                x="ts",
                y="review_count",
                title="Количество отзывов по времени",
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── Таблица ────────────────────────────────────────────────────────
    st.subheader("Последние 50 окон")
    st.dataframe(
        df.sort("window_start", descending=True).head(50).to_pandas(),
        use_container_width=True,
        hide_index=True,
    )


while True:
    with placeholder.container():
        dashboard()
    if not auto_refresh:
        break
    time.sleep(refresh_interval)
