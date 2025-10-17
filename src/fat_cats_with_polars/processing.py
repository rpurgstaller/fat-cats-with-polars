import argparse
import polars as pl
import mapping.columns as colm
import mapping.categories as catm
from mapping.rules import RULES, Rule


def process_data(df: pl.DataFrame, rules: list[Rule]) -> pl.DataFrame:
    df = df.rename(colm.MAPPING)
    df = df.with_columns(
        pl.col(colm.AMOUNT)
        .str.replace(".", "", literal=True)
        .str.replace(",", ".", literal=True)
        .cast(pl.Float32)
        .abs()
    )

    expr = pl.when(pl.col(colm.PARTNER_NAME).is_null()).then(pl.lit(catm.UNDEFINED))
    for rule in rules:
        expr = rule.get_expr(expr)

    df = df.with_columns(expr.otherwise(pl.lit(catm.UNDEFINED)).alias(colm.CATEGORY))

    df = df.with_columns(
        (pl.col(colm.CATEGORY) != catm.UNDEFINED).alias(colm.PROCESSED)
    )

    return df
