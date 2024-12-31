import logging
from abc import ABC
from abc import abstractmethod
from typing import Union

from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import BatchRequest

from contexts.BaseContext import BaseContext
from exceptions.BaseCheckpointError import BaseCheckpointError


class BaseCheckpoint(ABC):
    def __init__(self, context: BaseContext) -> None:
        self.context = context

    def create_validation(
        self, batch_request: Union[BatchRequest, BatchRequest], expectation_suite: str
    ) -> dict:
        return {"batch_request": batch_request, "expectation_suite_name": expectation_suite}

    # TODO: Changer la définition pour avoir plusieurs validations en une liste
    def get_validations(self, validation: dict) -> list[dict]:
        return [validation]

    def add_checkpoint(self, checkpoint_name: str, validations: list[dict]) -> None:
        logger = logging.getLogger("MainLogger")

        if checkpoint_name in self.context.context.list_checkpoints():
            logger.warning(f"Le checkpoint '{checkpoint_name}' existe déjà")
            return

        _ = self.context.context.add_or_update_checkpoint(
            name=checkpoint_name,
            validations=validations,
        )

    def get_checkpoint(self, checkpoint_name: str) -> Checkpoint:
        logger = logging.getLogger("MainLogger")
        if not checkpoint_name in self.context.context.list_checkpoints():
            logger.critical(f"Le checkpoint '{checkpoint_name}' existe déjà")
            raise BaseCheckpointError(f"Le checkpoint '{checkpoint_name}' existe déjà")

        return self.context.context.get_checkpoint(name=checkpoint_name)

    @abstractmethod
    def set_checkpoints(self, context: BaseContext):
        pass
