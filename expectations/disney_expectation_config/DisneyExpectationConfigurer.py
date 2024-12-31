import contexts.disney_context as dc
from contexts.BaseContext import BaseContext
from exceptions.disney_exceptions.DisneyExpectationConfigurerError import (
    DisneyExpectationConfigurerError,
)
from expectations.BaseExpectationConfigurer import BaseExpectationConfigurer


class DisneyExpectationConfigurer(BaseExpectationConfigurer):
    """Configure les expectations à utiliser"""

    def __init__(self, context: BaseContext) -> None:
        if not isinstance(context, dc.DisneyContext):
            raise DisneyExpectationConfigurerError(
                f"{context.context_name} n'est pas un context disney"
            )
        super().__init__(context)

    def populate_expectation_suite(self, expectation_suite_name: str) -> None:

        suite_name = "disney_nullity_verification"

        self.create_or_get_expectation_suite(expectation_suite_name=suite_name)

        self.verify_nullity(
            suite_name=suite_name,
            columns=[
                "VISUAL_ID",
                "ACTIVITY_DATE",
                "ACTIVITY_DATETIME",
                "ACTIVITY_ENDTIME",
                "REGISTERED_DATE",
                "CODE_PLU",
                "PRODUCT_SEGMENT",
                "PRODUCT_NAME",
                "PRODUCT_NAME_2",
                "PRODUCT_DETAILS",
                "TICKET_PRICING_SEASON",
                "QUANTITY_REGISTRATION",
            ],
        )

        self.save_expectation_suite(expectation_suite_name=expectation_suite_name)

    def verify_nullity(self, suite_name: str, columns: list[str]) -> None:
        for column in columns:
            self.add_expectation(
                expectation_suite_name=suite_name,
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": column, "mostly": 1.0},
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": f"Vérifie s'il n'y a pas de valeur NULL "
                        f"dans la colonne {column}. **Markdown** `Supported`",
                    }
                },
            )
