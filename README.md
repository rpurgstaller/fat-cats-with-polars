# Fat Cats With Polars

A simple expense analysis and visualization tool using Polars.

![csv-to-barplot](docs/img/csv_to_barplot.png)

## Motivation

I wanted a simple script to analyze my monthly expenses to better understand my spending habits. 
Using a basic rule engine, the script categorizes transactions into groups such as groceries, living, leisure, etc.
Currently, it generates a bar plot of my expenses per category based on the rules I've defined.

## Usage

Initialize the project:

```shell
make init
```

Run the project:

```shell
python src/fat_cats_with_polars/run.py file_in.csv
```

## Requirements

- Python 3.13+
- Polars
- uv
- Plotly
