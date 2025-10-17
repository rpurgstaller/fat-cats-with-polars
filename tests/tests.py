import polars as pl

from mapping.rules import RuleContainsAny, Rule
from processing import process_data
from stats import show_expenses_by_category
import mapping.columns as colm
import mapping.categories as catm


def test_process_data():
    df_in = pl.DataFrame(
        {
            "Buchungsdatum": ["2025-01-01"] * 10,
            "Partnername": [f"partner {i}" for i in range(10)],
            "Partner IBAN": ["x"] * 10,
            "Betrag": [f"{i}.000,00" for i in range(10)],
            "Buchungs-Details": [f"detail {i}" for i in range(10)],
        }
    )

    rules: list[Rule] = [
        RuleContainsAny.create_for_partner_name(
            target_category=catm.GROCERIES,
            patterns=[
                "partner 0",
                "partner 1",
            ],
        ),
        RuleContainsAny.create_for_partner_name(
            target_category=catm.LIVING,
            patterns=[
                "partner 2",
                "partner 3",
            ],
        ),
    ]

    df: pl.DataFrame = process_data(df_in, rules=rules)
    df_expected = pl.DataFrame(
        {
            colm.CATEGORY: [catm.GROCERIES] * 2
            + [catm.LIVING] * 2
            + [catm.UNDEFINED] * 6,
            colm.PROCESSED: [True] * 4 + [False] * 6,
        }
    )

    assert df.select([colm.CATEGORY, colm.PROCESSED]).equals(df_expected)


def test_show_expenses_by_category():
    df = pl.DataFrame(
        {
            colm.AMOUNT: [1, 2, 2, 3, 3, 3, 4, 4, 4, 4],
            colm.CATEGORY: ["A", "A", "A", "B", "B", "B", "A", "B", "A", "B"],
        }
    )

    fig = show_expenses_by_category(df)
    fig.show()


def test_process_and_show():
    df_in = pl.DataFrame(
        {
            "Buchungsdatum": ["2025-01-01"] * 6,
            "Partnername": [
                "supermarkets inc.",
                "rent",
                "internet provider",
                "train",
                "music inc.",
                "cats and stuff inc.",
            ],
            "Partner IBAN": ["x"] * 6,
            "Betrag": ["500,00", "1.000,00", "20,00", "55,00", "12,00", "77,00"],
            "Buchungs-Details": [f"detail {i}" for i in range(6)],
        }
    )

    rules: list[Rule] = [
        RuleContainsAny.create_for_partner_name(
            target_category=catm.GROCERIES,
            patterns=["supermarkets"],
        ),
        RuleContainsAny.create_for_partner_name(
            target_category=catm.LIVING,
            patterns=["rent", "internet provider"],
        ),
        RuleContainsAny.create_for_partner_name(
            target_category=catm.MOBILITY,
            patterns=["train"],
        ),
        RuleContainsAny.create_for_partner_name(
            target_category=catm.LEISURE,
            patterns=["music"],
        ),
        RuleContainsAny.create_for_partner_name(
            target_category=catm.CATS,
            patterns=["cats and stuff"],
        ),
    ]

    colors = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A']

    df_processed = process_data(df_in, rules=rules)
    fig = show_expenses_by_category(df_processed, color=colors)
    fig.update_layout(showlegend=False)
    fig.show()