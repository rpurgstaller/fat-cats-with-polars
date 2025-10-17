import polars as pl
import mapping.columns as colm
import mapping.categories as catm


class Rule:
    def get_expr(self, expr):
        pass


class RuleContainsAny(Rule):
    def __init__(self):
        self.source_col = None
        self.target_category = None
        self.patterns = None

    @classmethod
    def create(cls, source_col: str, target_category: str, patterns: list[str]):
        r = cls()
        r.source_col = source_col
        r.target_category = target_category
        r.patterns = patterns
        return r

    @classmethod
    def create_for_partner_name(cls, target_category: str, patterns: list[str]):
        return cls.create(colm.PARTNER_NAME, target_category, patterns)

    def get_expr(self, expr):
        return expr.when(pl.col(self.source_col).str.contains_any(self.patterns)).then(
            pl.lit(self.target_category)
        )


RULES = [
    RuleContainsAny.create_for_partner_name(
        target_category=catm.GROCERIES,
        patterns=[
            "supermarkets",
            "bakeries",
            "Deliciousness GmbH",
        ],
    ),
    RuleContainsAny.create_for_partner_name(
        target_category=catm.LIVING,
        patterns=[
            "my internet provider",
            "my mobile provider",
            "...",
        ],
    ),
    RuleContainsAny.create_for_partner_name(
        target_category=catm.SALARY,
        patterns=["fat cats ltd."],
    ),
    RuleContainsAny.create_for_partner_name(
        target_category=catm.GASTRONOMY,
        patterns=["my favourite ramen place", "my second favourite ramen place", "..."],
    ),
    RuleContainsAny.create_for_partner_name(
        target_category=catm.CATS, patterns=["VET", "catfood supplier"]
    ),
    RuleContainsAny.create_for_partner_name(
        target_category=catm.INSURANCE, patterns=["Broken Leg Insurance Co."]
    ),
    RuleContainsAny.create_for_partner_name(
        target_category=catm.MOBILITY, patterns=["trains and stuff"]
    ),
    RuleContainsAny.create_for_partner_name(
        target_category=catm.LEISURE,
        patterns=[
            "my bookstore",
            "my climbing gym",
            "my music provider",
        ],
    ),
]
