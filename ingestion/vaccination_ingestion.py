from typing import Iterable
from pydantic.dataclasses import dataclass
import pandas as pd
from typing import Optional
from datetime import date, datetime
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import MetadataChangeEventClass, DatasetSnapshotClass
from datahub.metadata.schema_classes import DatasetPropertiesClass
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest


# Define the Pydantic dataclass to map the CSV rows
@dataclass
class VaccinationData:
    location: str
    date: date
    vaccine: str
    source_url: str
    total_vaccinations: Optional[int]
    people_vaccinated: Optional[int]
    people_fully_vaccinated: Optional[int]
    total_boosters: Optional[int]


# Define the custom source config
class VaccinationSourceConfig(ConfigModel):
    env: str = "PROD"
    csv_file_url: str  # Add the CSV file path to the configuration


# Custom source class
class VaccinationSource(Source):
    source_config: VaccinationSourceConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: VaccinationSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = VaccinationSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Read the CSV file
        df = pd.read_csv(self.source_config.csv_file_url)

        # Initialize the Great Expectations context
        context = ge.data_context.DataContext()

        # Create a RuntimeBatchRequest for the CSV data
        batch_request = RuntimeBatchRequest(
            datasource_name="vaccination_data",
            data_connector_name="default_runtime_data_connector_name",
            # Use this connector
            data_asset_name="vaccination_data",
            runtime_parameters={"batch_data": df},  # Pass the Pandas dataframe
            batch_identifiers={"default_identifier_name": "default_identifier"}
        )

        # Get a validator object for the batch
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="vaccination_expectations"
        )

        # Run validation
        results = validator.validate()

        # Process each row and yield MetadataWorkUnit (as in previous code)
        for _, row in df.iterrows():
            vaccination_data = VaccinationData(
                location=row['location'],
                date=datetime.strptime(row['date'], "%Y-%m-%d").date(),
                vaccine=row['vaccine'],
                source_url=row['source_url'],
                total_vaccinations=int(row['total_vaccinations']) if pd.notna(
                    row['total_vaccinations']) else None,
                people_vaccinated=int(row['people_vaccinated']) if pd.notna(
                    row['people_vaccinated']) else None,
                people_fully_vaccinated=int(row['people_fully_vaccinated']) if pd.notna(
                    row['people_fully_vaccinated']) else None,
                total_boosters=int(row['total_boosters']) if pd.notna(
                    row['total_boosters']) else None
            )

            # Create the aspect
            aspects = [
                DatasetPropertiesClass(
                    description=f"Vaccination data for {vaccination_data.location} on {vaccination_data.date}",
                    customProperties={
                        "vaccine": vaccination_data.vaccine,
                        "total_vaccinations": str(vaccination_data.total_vaccinations),
                        "people_vaccinated": str(vaccination_data.people_vaccinated),
                        "people_fully_vaccinated": str(
                            vaccination_data.people_fully_vaccinated),
                        "total_boosters": str(vaccination_data.total_boosters),
                        "source_url": vaccination_data.source_url,
                    }
                )
            ]

            # Create the snapshot
            snapshot = DatasetSnapshotClass(
                urn=f"urn:li:dataset:(urn:li:dataPlatform:vaccination_data,{vaccination_data.location.replace(' ', '_')},PROD)",
                aspects=aspects
            )

            # Create the MetadataChangeEvent
            mce = MetadataChangeEventClass(proposedSnapshot=snapshot)

            # Create the work unit
            wu = MetadataWorkUnit(vaccination_data.date, mce=mce)
            self.report.report_workunit(wu)
            yield wu

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass
