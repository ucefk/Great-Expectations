import logging
import os
from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import Union

import great_expectations as gx
from great_expectations.data_context import FileDataContext

from exceptions.BaseContextError import BaseContextError


class BaseContext(ABC):
    def __init__(self, working_dir_path: str = None, context_name: str = "disney") -> None:

        self.context_name = context_name

        self.context_dir: Path = Path(os.path.join(os.path.abspath("."), self.context_name))
        self.great_expectation_config_path: Path = Path("great_expectations/")
        self.config_file_name: Path = Path("great_expectations.yml")

        logger = logging.getLogger("MainLogger")

        if working_dir_path is None:
            logger.info("working_dir_path is None")
            if os.path.isfile(
                os.path.join(
                    self.context_dir, self.great_expectation_config_path, self.config_file_name
                )
            ):
                log_var = os.path.abspath(".")
                logger.info(f"{log_var} contient un fichier de config yaml.")
                logger.info(
                    f"Utilisation du context défini dans '{log_var}/great_expectations.yml'"
                )

                self.context = self.get_context(
                    context_path=os.path.join(self.context_dir, self.great_expectation_config_path)
                )

            else:
                logger.info(f"{os.path.abspath('.')} ne contient aucun fichier de config yaml.")

                self.context = self.create_file_context(new_context_path=self.context_dir)

        else:
            logger.info(f"{working_dir_path} is not None")
            if not os.path.isabs(working_dir_path):
                logger.info(f"{working_dir_path} is not an absolute path.")
                working_dir_path = os.path.join(os.path.abspath("."), working_dir_path)

            if os.path.isfile(
                os.path.join(
                    working_dir_path, self.great_expectation_config_path, self.config_file_name
                )
            ):
                log_var = os.path.join(
                    working_dir_path,
                    self.great_expectation_config_path,
                    self.config_file_name,
                )
                logger.info(f"{working_dir_path} contient un fichier de config yaml.")
                logger.info(f"Utilisation du context défini dans '{log_var}'")

                self.context = self.get_context(
                    context_path=os.path.join(working_dir_path, self.great_expectation_config_path)
                )

            elif os.path.isfile(os.path.join(working_dir_path, self.config_file_name)):
                log_var = os.path.join(
                    working_dir_path,
                    self.great_expectation_config_path,
                    self.config_file_name,
                )
                logger.info(f"{working_dir_path} contient un fichier de config yaml.")
                logger.info(f"Utilisation du context défini dans '{log_var}'")
                self.context = self.get_context(context_path=working_dir_path)
            else:
                logger.info(f"{working_dir_path} ne contient aucun fichier de config yaml.")
                raise BaseContextError(
                    f"{working_dir_path} ne contient aucun fichier de config yaml."
                )

    def create_file_context(self, new_context_path: Union[str, Path]):
        logger = logging.getLogger("MainLogger")
        logger.info(f"Creation d'un nouveau context sous '{new_context_path}'.")
        return FileDataContext.create(project_root_dir=new_context_path)

    def get_context(self, context_path: Union[str, Path]) -> None:
        logger = logging.getLogger("MainLogger")
        logger.info(f"Utilisation du context sous '{context_path}'.")
        return gx.get_context(context_root_dir=context_path)

    @abstractmethod
    def initialize_data_sources(self):
        pass

    @abstractmethod
    def initialize_expectations(self):
        pass

    @abstractmethod
    def initialize_checkpoints(self):
        pass

    # @abstractmethod
    # def initialize_profilers(self):
    #     pass

    # @abstractmethod
    # def initialize_plugins(self):
    #     pass

    # @abstractmethod
    # def initialize_validations_store(self):
    #     pass
