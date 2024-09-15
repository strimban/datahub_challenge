# DataHub Challenge

This project integrates Great Expectations with DataHub to validate and ingest vaccination data from a CSV file into DataHub. It uses a custom source and integrates validation results into DataHub’s metadata.

## Features
- Ingests CSV data into DataHub.
- Runs data validation using Great Expectations.
- Sends validation results to DataHub.

## Installation

Clone the repository:

```bash
git clone https://github.com/strimban/datahub_challenge.git
cd datahub_challenge


pip install -r requirements.txt

datahub docker quickstart

pip install -U .

datahub ingest -c import_data.yaml