from pathlib import Path
import pandera.polars as pa
import polars as pl


# examples from https://khuyentran1401.github.io/reproducible-data-science/testing_data/pandera.html
# and https://pandera.readthedocs.io/en/stable/polars.html
# and https://pandera.readthedocs.io/en/stable/lazy_validation.html
# and https://pandera.readthedocs.io/en/stable/checks.html
# and https://pandera.readthedocs.io/en/latest/dataframe_schemas.html


schema = pa.DataFrameSchema(
    columns={
        "name": pa.Column(str),
        "gender": pa.Column(str),
        "phone": pa.Column(str),
        "cell": pa.Column(str, pa.Check.str_matches(r"^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}$")),
        "email": pa.Column(str, pa.Check.str_matches(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")),
        "city": pa.Column(str),
        "state": pa.Column(str),
        "country": pa.Column(str),
    },
    strict=True
)


ndjson_filename = Path(__file__).parents[1] / "test_luigi_examples" / "data" / "flat-randomusers.txt"
# schema validation and checks work with a polars DataFrame; checks don't seem to work with a polars LazyFrame
df = pl.read_ndjson(ndjson_filename)
# make 2 copies of the same data to see that we get multiple validation fails
df = pl.concat([df, df], rechunk=True)

try:
    # results will contain the validated dataframe with any additional cleaning that panderas applies
    results = schema.validate(df, lazy=True)
except pa.errors.SchemaErrors as exc:
    print("Schema errors and failure cases:")
    print(exc.failure_cases)
    print("\nDataFrame object that failed validation:")
    print(exc.data.collect())
