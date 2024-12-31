import logging
from abc import ABC
from abc import abstractmethod

from great_expectations.core import ExpectationConfiguration
from great_expectations.core import ExpectationSuite

from contexts.BaseContext import BaseContext


class BaseExpectationConfigurer(ABC):
    def __init__(self, context: BaseContext) -> None:
        self.context = context

    def expectation_suite_name_existe(self, expectation_suite_name: str) -> None:
        return expectation_suite_name in self.context.context.list_expectation_suite_names()

    def create_or_get_expectation_suite(self, expectation_suite_name: str) -> ExpectationSuite:
        logger = logging.getLogger("MainLogger")

        # Si la suite d'expectations existe déjà on la retourne
        if self.expectation_suite_name_existe(expectation_suite_name=expectation_suite_name):
            logger.info(
                f"{expectation_suite_name} existe déjà "
                f"dans le context {self.context.context_name}"
            )
            return self.context.context.get_expectation_suite(
                expectation_suite_name=expectation_suite_name
            )

        # Sinon on crée une nouvelle suite d'expectation
        expectation_suite = self.context.context.add_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        # self.context.context.update_expectation_suite(expectation_suite)
        return expectation_suite

    def add_expectation(self, expectation_suite_name: str, **kw) -> None:

        # Expectation config data
        expectation_type = kw["expectation_type"]
        kwargs = kw["kwargs"]
        meta = kw["meta"]

        # Définir la configuration de l'expectation à ajouter
        expectation_config = ExpectationConfiguration(
            expectation_type=expectation_type,
            kwargs=kwargs,
            meta=meta,
        )

        self._add_expectation_config_to_suite(
            expectation_suite_name=expectation_suite_name, expectation_config=expectation_config
        )

    def _add_expectation_config_to_suite(
        self, expectation_suite_name: str, expectation_config: ExpectationConfiguration
    ):
        # Créer (ou non si la suite existe déjà) ou obtenir la suite d'expectations
        expectation_suite = self.create_or_get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )

        # Ajouter l'expectation à la suite d'expectations
        expectation_suite.add_expectation(expectation_configuration=expectation_config)

        # Sauvgarder la suite aprés modification
        # expectation_suite = self.create_or_get_expectation_suite(
        #     expectation_suite_name=expectation_suite_name
        # )
        self.context.context.update_expectation_suite(expectation_suite=expectation_suite)

        # self.save_expectation_suite(expectation_suite_name)

    def save_expectation_suite(self, expectation_suite_name: str) -> None:
        expectation_suite = self.create_or_get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        self.context.context.update_expectation_suite(expectation_suite=expectation_suite)

        # validator = self.context.context.get_validator(
        #     expectation_suite_name=expectation_suite_name,
        #     # create_expectation_suite_with_name=expectation_suite_name,
        # )
        # validator.save_expectation_suite(discard_failed_expectations=False)

    @abstractmethod
    def populate_expectation_suite(self, expectation_suite_name: str) -> None:
        # ###########################
        # # Create Expectations interactively with Python
        # ###########################

        # # Create a Validator
        # # Optional.
        # # Run assert "verify_nullity_interactive" in context.list_expectation_suite_names()
        # # to verify the Expectation Suite was created.
        # # context.add_or_update_expectation_suite("verify_nullity_interactive")
        # validator = self.context.context.get_validator(
        #     batch_request=batch_request,
        #     expectation_suite_name="verify_nullity_interactive",
        #     # create_expectation_suite_with_name="verify_nullity_interactive",
        # )

        # # Use the Validator to create and run an Expectation
        # validator.expect_column_values_to_not_be_null(column="VISUAL_ID")

        # # (Optional) Save your Expectations for future use
        # validator.save_expectation_suite(discard_failed_expectations=False)

        # ###########################
        # # Create and edit Expectations based on domain knowledge, without inspecting data directly
        # ###########################

        # # Create an ExpectationSuite
        # null_suite = disney_context.context
        # .add_expectation_suite(expectation_suite_name="verify_nullity")

        # # Create Expectation Configurations
        # expectation_configuration = ExpectationConfiguration(
        #     expectation_type="expect_column_values_to_not_be_null",
        #     kwargs={
        #         "column": "QUANTITY_REGISTRATION",
        #         "mostly": 1.0,
        #     },
        #     meta={
        #         "notes": {
        #             "format": "markdown",
        #             "content": "Vérifie s'il n'y a pas de valeur NULL dans
        #                         la colonne QUANTITY_REGISTRATION. **Markdown** `Supported`",
        #         }
        #     },
        # )

        # # Add an Expectation with the config
        # null_suite.add_expectation(expectation_configuration=expectation_configuration)

        # # Save your Expectations for future use
        # disney_context.context.update_expectation_suite(expectation_suite=null_suite)
        pass

    # self.context.context.get_expectation_suite("")
