from pathlib import Path
import pandera.polars as pa
import polars as pl


schema = pa.DataFrameSchema(
    columns={
        "name": pa.Column(str),
        "gender": pa.Column(str),
        "phone": pa.Column(str),
        "cell": pa.Column(str),
        "email": pa.Column(str),
        "city": pa.Column(str),
        "state": pa.Column(str),
        "country": pa.Column(str),
    },
    strict=True
)


ndjson_filename = Path(__file__).parents[1] / "test_luigi_examples" / "data" / "flat-randomusers.txt"
# read as a LazyFrame, since want to test pandera against a LazyFrame rather than a DataFrame
lf = pl.LazyFrame(pl.read_ndjson(ndjson_filename))

try:
    results = schema.validate(lf, lazy=True).collect()
except pa.errors.SchemaErrors as exc:
    print("Schema errors and failure cases:")
    print(exc.failure_cases)
    print("\nDataFrame object that failed validation:")
    print(exc.data)

print(results)
