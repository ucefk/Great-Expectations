from abc import ABC
from abc import abstractmethod

from contexts.BaseContext import BaseContext


class FileDataSource(ABC):
    def __init__(self, context: BaseContext) -> None:
        self.context = context

    @abstractmethod
    def add_file_data_source(self, datasource_config: dict) -> None:
        pass

    @abstractmethod
    def add_file_data_asset(self, asset_config: dict) -> None:
        pass

    @abstractmethod
    def get_batch_request(self, request_config: dict) -> None:
        pass
