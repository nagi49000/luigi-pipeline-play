from pathlib import Path
import great_expectations as gx

# based on this GX datasource and batch - https://discourse.greatexpectations.io/t/configure-datasource-for-json-files/121
context = gx.get_context()
# the data source specifies the engine used for sourcing the data
data_source = context.data_sources.add_pandas("pandas")
# can do a help(data_source) to see all the assets that can be added;
# these are typically some data store/file that the above data source engine can use
ndjson_filename = Path(__file__).parents[1] / "test_luigi_examples" / "data" / "flat-randomusers.txt"
data_asset = data_source.add_json_asset(name="ndjson asset on pandas", path_or_buf=ndjson_filename, lines=True)

# define the expectations in code as a suite in the context
suite = gx.ExpectationSuite(name="flat ndjson suite")
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="email", regex=r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
    )
)
suite.add_expectation(
    expectation=gx.expectations.ExpectColumnValuesToMatchRegex(
        column="phone", regex=r"^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}$"
    )
)
suite = context.suites.add(suite)

# Batches are designed to be "MECE" -- mutually exclusive and collectively exhaustive partitions of Data Assets
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch()

# Add Validation Definition to the Data Context
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name="flat json validation"
)
validation_definition = context.validation_definitions.add(validation_definition)

# Add Checkpoint to the Data Context
checkpoint = gx.Checkpoint(
    name="flat json checkpoint",
    validation_definitions=[validation_definition],
    # actions=action_list, # some set of actions, like sending to slack
    result_format={"result_format": "COMPLETE"},
)
context.checkpoints.add(checkpoint)

results = checkpoint.run()

# the expectation suite can be saved as a json configuration
# context.get_validator(batch=batch, expectation_suite=suite).save_expectation_suite("expectation_suite.json")

print(results)
