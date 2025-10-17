import polars as pl
import mapping.columns as colm
import plotly.express as px


def show_expenses_by_category(df: pl.DataFrame, **kwargs):
    df_by_category = df.group_by(colm.CATEGORY).agg(pl.col(colm.AMOUNT).sum())

    return px.bar(
        df_by_category.to_pandas(),
        x=colm.CATEGORY,
        y=colm.AMOUNT,
        title="Expenses by Category",
        **kwargs,
    )
