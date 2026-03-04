"""Streamlit dashboard for ANA hydrology exploration and filtering."""

from __future__ import annotations

import sqlite3
import re
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


@st.cache_data(show_spinner=False)
def load_data(db_path: Path, db_cache_key: tuple[int, int]) -> pd.DataFrame:
    """Load measurement data from SQLite for dashboard usage.

    Args:
        db_path: Path to SQLite database.
        db_cache_key: Cache invalidation key built from file metadata.

    Returns:
        pd.DataFrame: Raw measurement dataframe loaded from ``ana_medicoes``.
    """
    # db_cache_key is intentionally unused; it invalidates cache when file changes.
    _ = db_cache_key
    con = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(
            """
            SELECT
                record_id,
                reservatorio_id,
                reservatorio,
                uf,
                subsistema,
                data_medicao,
                cota_m,
                afluencia_m3s,
                defluencia_m3s,
                vazao_vertida_m3s,
                vazao_turbinada_m3s,
                vazao_natural_m3s,
                vazao_incremental_m3s,
                volume_util_pct,
                balanco_vazao_m3s,
                situacao_hidrologica
            FROM ana_medicoes
            ORDER BY data_medicao ASC
            """,
            con,
        )
    finally:
        con.close()


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize dataframe types and fill display defaults.

    Args:
        df: Input dataframe from SQLite.

    Returns:
        pd.DataFrame: Sanitized dataframe ready for filtering/plotting.
    """
    output = df.copy()
    output["data_medicao"] = pd.to_datetime(output["data_medicao"], errors="coerce")
    output = output.dropna(subset=["data_medicao"])
    output["uf"] = output["uf"].fillna("NA")
    output["subsistema"] = output["subsistema"].fillna("NA")
    output["situacao_hidrologica"] = output["situacao_hidrologica"].fillna("indefinido")
    return output


def filter_data(
    df: pd.DataFrame,
    *,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    subsistema: str | None,
    uf: str | None,
    reservatorio: str | None,
) -> pd.DataFrame:
    """Apply date and optional categorical filters to measurements.

    Args:
        df: Preprocessed measurements dataframe.
        date_start: Initial date inclusive.
        date_end: Final date inclusive.
        subsistema: Optional subsystem filter, or ``Todos``.
        uf: Optional UF filter, or ``Todos``.
        reservatorio: Optional reservoir name filter, or ``Todos``.

    Returns:
        pd.DataFrame: Filtered dataframe.
    """
    filtered = df[(df["data_medicao"] >= date_start) & (df["data_medicao"] <= date_end)]
    if subsistema and subsistema != "Todos":
        filtered = filtered[filtered["subsistema"] == subsistema]
    if uf and uf != "Todos":
        filtered = filtered[filtered["uf"] == uf]
    if reservatorio and reservatorio != "Todos":
        filtered = filtered[filtered["reservatorio"] == reservatorio]
    return filtered


def granularity_freq(granularity: str) -> str:
    """Translate UI granularity labels to pandas frequency aliases.

    Args:
        granularity: One of ``Diario``, ``Semanal``, or ``Mensal``.

    Returns:
        str: Pandas resample frequency code.
    """
    mapping = {
        "Diario": "D",
        "Semanal": "W-MON",
        "Mensal": "MS",
    }
    return mapping[granularity]


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Convert dataframe into UTF-8 CSV bytes for Streamlit download."""
    return df.to_csv(index=False).encode("utf-8")


def safe_file_token(value: str) -> str:
    """Return a filesystem-safe token for file names."""
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower())
    normalized = normalized.strip("_")
    return normalized or "filtro"


def _mode_or_default(series: pd.Series) -> str:
    clean = series.dropna()
    if clean.empty:
        return "indefinido"
    mode = clean.mode()
    if mode.empty:
        return "indefinido"
    return str(mode.iloc[0])


def series_by_granularity(df: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """Aggregate reservoir series by selected granularity.

    Args:
        df: Reservoir-specific dataframe.
        granularity: UI granularity label.

    Returns:
        pd.DataFrame: Aggregated time series with mean numeric metrics and modal status.
    """
    if df.empty:
        return df

    freq = granularity_freq(granularity)
    numeric = (
        df.set_index("data_medicao")
        .resample(freq)[
            ["volume_util_pct", "afluencia_m3s", "defluencia_m3s", "balanco_vazao_m3s"]
        ]
        .mean()
    )
    status = (
        df.set_index("data_medicao")["situacao_hidrologica"]
        .resample(freq)
        .agg(_mode_or_default)
        .rename("situacao_hidrologica")
    )
    agg = numeric.join(status).dropna(how="all")
    return agg.reset_index()


def subsystem_daily_mean(df: pd.DataFrame) -> pd.DataFrame:
    """Build daily subsystem series by averaging all reservoirs each day."""
    if df.empty:
        return df

    metrics = ["volume_util_pct", "afluencia_m3s", "defluencia_m3s", "balanco_vazao_m3s"]
    daily = (
        df.groupby("data_medicao", as_index=False)[metrics]
        .mean()
        .sort_values("data_medicao")
    )
    daily["situacao_hidrologica"] = "agregado"
    return daily


def reservoir_period_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-reservoir mean metrics for the selected period."""
    if df.empty:
        return df

    summary = (
        df.groupby(["reservatorio", "uf"], dropna=False)[
            ["volume_util_pct", "afluencia_m3s", "defluencia_m3s", "balanco_vazao_m3s"]
        ]
        .mean()
        .reset_index()
        .rename(
            columns={
                "volume_util_pct": "volume_util_medio_pct",
                "afluencia_m3s": "afluencia_media_m3s",
                "defluencia_m3s": "defluencia_media_m3s",
                "balanco_vazao_m3s": "balanco_medio_m3s",
            }
        )
        .sort_values("volume_util_medio_pct", ascending=False)
    )
    return summary


def _apply_plot_style(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="#F7F8FA",
        plot_bgcolor="#F7F8FA",
        margin=dict(l=20, r=20, t=40, b=20),
        legend_title_text="",
    )
    fig.update_xaxes(gridcolor="#E4E7EC", zeroline=False)
    fig.update_yaxes(gridcolor="#E4E7EC", zeroline=False)
    return fig


def _volume_x_bounds(plot: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    if plot.empty:
        return None
    x0 = pd.to_datetime(plot["data_medicao"]).min()
    x1 = pd.to_datetime(plot["data_medicao"]).max()
    if x0 == x1:
        delta = pd.Timedelta(hours=12)
        return (x0 - delta, x1 + delta)
    return (x0, x1)


def _hydrology_figure(serie: pd.DataFrame) -> go.Figure:
    """Build the dashboard hydrology figure for the selected view.

    Args:
        serie: Aggregated series from ``series_by_granularity``.

    Returns:
        go.Figure: Multi-panel chart with volume, flows, and balance.
    """
    plot = serie.dropna(subset=["volume_util_pct"]).copy()
    plot = plot.sort_values("data_medicao").reset_index(drop=True)

    flow_plot = serie.dropna(
        subset=["afluencia_m3s", "defluencia_m3s"], how="all"
    ).copy()
    bal_plot = serie.dropna(subset=["balanco_vazao_m3s"]).copy()

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=(
            "Volume util por situacao hidrologica",
            "Afluencia x Defluencia",
            "Balanco de vazao (verde=positivo, vermelho=negativo)",
        ),
    )

    volume_bounds = _volume_x_bounds(plot)
    if volume_bounds is not None:
        x0, x1 = volume_bounds
        bands = [
            (0, 30, "rgba(199,161,161,0.30)"),
            (30, 50, "rgba(213,198,168,0.30)"),
            (50, 80, "rgba(182,197,178,0.30)"),
            (80, 100, "rgba(164,191,183,0.30)"),
        ]
        for y0, y1, color in bands:
            fig.add_trace(
                go.Scatter(
                    x=[x0, x1, x1, x0],
                    y=[y0, y0, y1, y1],
                    mode="lines",
                    line=dict(width=0),
                    fill="toself",
                    fillcolor=color,
                    showlegend=False,
                    hoverinfo="skip",
                ),
                row=1,
                col=1,
            )

    # Row 1: volume
    fig.add_trace(
        go.Scatter(
            x=plot["data_medicao"],
            y=plot["volume_util_pct"],
            mode="lines",
            line=dict(color="#99A3AE", width=2),
            name="Volume util (%)",
            hovertemplate="Data=%{x|%Y-%m-%d}<br>Volume=%{y:.2f}%<extra></extra>",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=plot["data_medicao"],
            y=plot["volume_util_pct"],
            mode="markers",
            marker=dict(color="#7F8A96", size=7, line=dict(color="#ECEFF3", width=1)),
            text=plot["situacao_hidrologica"],
            name="Pontos",
            hovertemplate="Data=%{x|%Y-%m-%d}<br>Volume=%{y:.2f}<br>Situacao=%{text}<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Row 2: afluencia vs defluencia
    fig.add_trace(
        go.Scatter(
            x=flow_plot["data_medicao"],
            y=flow_plot["afluencia_m3s"],
            mode="lines",
            line=dict(color="#8FA9C2", width=2),
            name="Afluencia",
            hovertemplate="Data=%{x|%Y-%m-%d}<br>Afluencia=%{y:.2f}<extra></extra>",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=flow_plot["data_medicao"],
            y=flow_plot["defluencia_m3s"],
            mode="lines",
            line=dict(color="#C9A8A8", width=2),
            name="Defluencia",
            hovertemplate="Data=%{x|%Y-%m-%d}<br>Defluencia=%{y:.2f}<extra></extra>",
        ),
        row=2,
        col=1,
    )

    # Row 3: balance
    bal_colors = [
        "#93B49D" if value >= 0 else "#CB9C9C"
        for value in bal_plot["balanco_vazao_m3s"]
    ]
    fig.add_trace(
        go.Bar(
            x=bal_plot["data_medicao"],
            y=bal_plot["balanco_vazao_m3s"],
            marker_color=bal_colors,
            name="Balanco",
            hovertemplate="Data=%{x|%Y-%m-%d}<br>Balanco=%{y:.2f}<extra></extra>",
        ),
        row=3,
        col=1,
    )

    fig.add_hline(y=0, line_color="#8F99A4", line_width=1, row=3, col=1)
    fig.update_yaxes(range=[0, 100], row=1, col=1)
    fig.update_yaxes(title_text="Volume util (%)", row=1, col=1)
    fig.update_yaxes(title_text="Vazao (m3/s)", row=2, col=1)
    fig.update_yaxes(title_text="Balanco (m3/s)", row=3, col=1)
    fig.update_xaxes(title_text="Data", row=3, col=1)
    fig.update_layout(height=850, dragmode="zoom")
    return _apply_plot_style(fig)


def main() -> None:
    """Render dashboard app and handle user interaction widgets."""
    st.set_page_config(page_title="ANA Hydrology Dashboard", layout="wide")
    st.title("ANA Pipeline - Hidrologia")

    default_db = Path("data") / "out" / "ana.db"
    db_input = st.sidebar.text_input("SQLite path", value=str(default_db))
    db_path = Path(db_input)

    if not db_path.exists():
        st.warning(f"Database not found: {db_path}")
        st.info("Run the extraction job first to create the SQLite database.")
        return

    try:
        stat = db_path.stat()
        db_cache_key = (int(stat.st_mtime_ns), int(stat.st_size))
        base_df = load_data(db_path, db_cache_key)
    except Exception as exc:
        st.error(f"Failed to read database: {exc}")
        return

    if base_df.empty:
        st.info("Database is empty.")
        return

    df = preprocess(base_df)

    st.sidebar.subheader("Filtros")
    view_mode = st.sidebar.radio(
        "Visualizacao",
        options=["Por reservatorios", "Por subsistema"],
        index=0,
    )

    min_date = df["data_medicao"].min().date()
    max_date = df["data_medicao"].max().date()
    date_range = st.sidebar.date_input(
        "Periodo",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        date_start = pd.Timestamp(date_range[0])
        date_end = pd.Timestamp(date_range[1])
    else:
        date_start = pd.Timestamp(min_date)
        date_end = pd.Timestamp(max_date)

    period_df = df[
        (df["data_medicao"] >= date_start) & (df["data_medicao"] <= date_end)
    ]
    granularity = st.sidebar.selectbox(
        "Granularidade", options=["Diario", "Semanal", "Mensal"], index=2
    )

    if view_mode == "Por reservatorios":
        subs_options = sorted(period_df["subsistema"].dropna().unique().tolist())
        selected_sub = st.sidebar.selectbox(
            "Subsistema", options=["Todos", *subs_options], index=0
        )

        uf_base = (
            period_df
            if selected_sub == "Todos"
            else period_df[period_df["subsistema"] == selected_sub]
        )
        uf_options = sorted(uf_base["uf"].dropna().unique().tolist())
        selected_uf = st.sidebar.selectbox(
            "UF", options=["Todos", *uf_options], index=0
        )

        res_base = (
            uf_base if selected_uf == "Todos" else uf_base[uf_base["uf"] == selected_uf]
        )
        res_options = sorted(res_base["reservatorio"].dropna().unique().tolist())
        selected_reservatorio = st.sidebar.selectbox(
            "Reservatorio", options=["Todos", *res_options], index=0
        )

        filtered = filter_data(
            df,
            date_start=date_start,
            date_end=date_end,
            subsistema=selected_sub,
            uf=selected_uf,
            reservatorio=selected_reservatorio,
        )
        if filtered.empty:
            st.warning("Nenhum dado para os filtros selecionados.")
            return

        st.download_button(
            label="Baixar CSV filtrado",
            data=to_csv_bytes(filtered),
            file_name="ana_medicoes_filtrado.csv",
            mime="text/csv",
            use_container_width=True,
        )

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Registros", int(len(filtered)))
        c2.metric("Reservatorios", int(filtered["reservatorio_id"].nunique()))
        c3.metric("Data inicial", str(filtered["data_medicao"].min().date()))
        c4.metric("Data final", str(filtered["data_medicao"].max().date()))

        st.subheader("Hidrologia")
        focus_options = sorted(filtered["reservatorio"].dropna().unique().tolist())
        if not focus_options:
            st.info("Sem reservatorios no filtro atual.")
            return

        if selected_reservatorio == "Todos":
            selected_focus = st.selectbox(
                "Reservatorio foco", options=focus_options, index=0
            )
        else:
            selected_focus = selected_reservatorio
            st.caption(f"Reservatorio foco: {selected_focus}")

        focus_df = filtered[filtered["reservatorio"] == selected_focus].copy()
        serie = series_by_granularity(focus_df, granularity)
        if serie.empty:
            st.info("Sem dados para o reservatorio selecionado.")
            return

        st.caption(f"Granularidade: {granularity}")
        st.plotly_chart(_hydrology_figure(serie), use_container_width=True)

        st.subheader("Comparativo de volume util medio")
        comp = (
            filtered.groupby(["reservatorio", "subsistema"], dropna=False)[
                "volume_util_pct"
            ]
            .mean()
            .reset_index(name="volume_util_medio_pct")
            .sort_values("volume_util_medio_pct", ascending=False)
        )
        st.dataframe(comp, use_container_width=True, hide_index=True)
        return

    sub_options = sorted(period_df["subsistema"].dropna().unique().tolist())
    if not sub_options:
        st.warning("Nao ha dados no periodo selecionado.")
        return
    selected_subsystem = st.sidebar.selectbox(
        "Subsistema",
        options=sub_options,
        index=0,
    )

    filtered = filter_data(
        df,
        date_start=date_start,
        date_end=date_end,
        subsistema=selected_subsystem,
        uf="Todos",
        reservatorio="Todos",
    )
    if filtered.empty:
        st.warning("Nenhum dado para os filtros selecionados.")
        return

    st.download_button(
        label="Baixar CSV filtrado",
        data=to_csv_bytes(filtered),
        file_name=f"ana_medicoes_{safe_file_token(selected_subsystem)}_filtrado.csv",
        mime="text/csv",
        use_container_width=True,
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Registros", int(len(filtered)))
    c2.metric("Reservatorios", int(filtered["reservatorio_id"].nunique()))
    c3.metric("Data inicial", str(filtered["data_medicao"].min().date()))
    c4.metric("Data final", str(filtered["data_medicao"].max().date()))

    st.subheader(f"Hidrologia por subsistema: {selected_subsystem}")
    subsystem_daily = subsystem_daily_mean(filtered)
    serie = series_by_granularity(subsystem_daily, granularity)
    if serie.empty:
        st.info("Sem dados agregados para o subsistema selecionado.")
        return
    st.caption(
        f"Granularidade: {granularity}. No modo Diario, cada ponto e a media dos reservatorios do subsistema no dia."
    )
    st.plotly_chart(_hydrology_figure(serie), use_container_width=True)

    st.subheader("Medias por reservatorio no periodo")
    summary = reservoir_period_summary(filtered)
    st.dataframe(summary, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
