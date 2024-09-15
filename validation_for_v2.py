import pandas as pd
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import Checkpoint

# Load your online CSV data
csv_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/country_data/United%20States.csv"
df = pd.read_csv(csv_url)

# Initialize the Great Expectations context
context = ge.data_context.DataContext()

# Create a RuntimeBatchRequest for the CSV data
batch_request = RuntimeBatchRequest(
    datasource_name="vaccination_data",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="vaccination_data",
    runtime_parameters={"batch_data": df},
    batch_identifiers={"default_identifier_name": "default_identifier"}
)

# Define a checkpoint with a proper validation configuration
checkpoint = Checkpoint(
    name="vaccination_checkpoint",
    data_context=context,
    validations=[{
        "batch_request": batch_request,
        "expectation_suite_name": "vaccination_expectations"
    }]
)

# Run the checkpoint validation
checkpoint_result = checkpoint.run()

# Output results
print(checkpoint_result)
