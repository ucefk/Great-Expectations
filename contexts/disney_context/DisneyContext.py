from checkpoints.disney_checkpoints.DisneyTableCP import DisnetTableCP
from contexts.BaseContext import BaseContext
from data_sources.disney_data_sources.DisneyFileDataSource import DisneyFileDataSource
from expectations.disney_expectation_config.DisneyExpectationConfigurer import (
    DisneyExpectationConfigurer,
)


class DisneyContext(BaseContext):
    """Définition du context 'disney'."""

    def __init__(self, working_dir_path: str = None, context_name: str = "disney") -> None:
        super().__init__(working_dir_path, context_name)

        self.disney_data_source = DisneyFileDataSource(self)
        self.disney_expectation_config = DisneyExpectationConfigurer(self)
        self.disney_checkpoints = DisnetTableCP(self)

        # self.initialize_disney_context()

    def initialize_disney_context(self) -> None:
        self.initialize_data_sources()
        self.initialize_expectations()
        self.initialize_checkpoints()

    def initialize_data_sources(self) -> None:
        self.disney_data_source.add_file_data_source(
            datasource_config={"datasource_name": "disney_csv"},
        )
        self.disney_data_source.add_file_data_asset(
            asset_config={"datasource_name": "disney_csv", "asset_name": "disney_asset1"},
        )
        # batch_request = self.disney_data_source.get_batch_request(
        #     request_config={"datasource_name": "disney_csv", "asset_name": "disney_asset1"},
        # )

    def initialize_expectations(self) -> None:
        self.disney_expectation_config.populate_expectation_suite("disney_nullity_verification")

    def initialize_checkpoints(self):
        self.disney_checkpoints.set_checkpoints()
