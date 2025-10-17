import argparse
import polars as pl

from mapping.rules import RULES
from processing import process_data
from stats import show_expenses_by_category

parser = argparse.ArgumentParser("fat-cats-with-polars")
parser.add_argument(
    "path_in", help="The raw input file, containing all transactions", type=str
)
args = parser.parse_args()

path_in = args.path_in

df = pl.read_csv(path_in, infer_schema_length=0, truncate_ragged_lines=True)
df = process_data(df, rules=RULES)

show_expenses_by_category(df)
