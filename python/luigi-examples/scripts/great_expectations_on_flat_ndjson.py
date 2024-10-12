from pathlib import Path
import great_expectations as gx


context = gx.get_context()
# the data source specifies the engine used for sourcing the data
data_source = context.data_sources.add_pandas("pandas")
# can do a help(data_source) to see all the assets that can be added;
# these are typically some data store/file that the above data source engine can use
ndjson_filename = Path(__file__).parents[1] / "test_luigi_examples" / "data" / "flat-randomusers.txt"
data_asset = data_source.add_json_asset(name="ndjson asset on pandas", path_or_buf=ndjson_filename, lines=True)
# Batches are designed to be "MECE" -- mutually exclusive and collectively exhaustive partitions of Data Assets
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch()

expectation = gx.expectations.ExpectColumnValuesToMatchRegex(
    column="email", regex=r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
)

validation_result = batch.validate(expectation)
print(validation_result)
