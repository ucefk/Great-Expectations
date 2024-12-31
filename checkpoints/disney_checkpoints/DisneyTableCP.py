import contexts.disney_context as dc
from checkpoints.BaseCheckpoint import BaseCheckpoint
from contexts.BaseContext import BaseContext
from data_sources.disney_data_sources.DisneyFileDataSource import DisneyFileDataSource
from exceptions.disney_exceptions.DisneyCheckpointError import DisneyCheckpointError


class DisnetTableCP(BaseCheckpoint):
    def __init__(self, context: BaseContext) -> None:
        if not isinstance(context, dc.DisneyContext):
            raise DisneyCheckpointError(f"{context.context_name} n'est pas un context disney")
        super().__init__(context)

    # TODO: Implementer
    def set_checkpoints(self):

        # TODO: Configurer la requete du batch
        batch_req = DisneyFileDataSource(context=self.context).get_batch_request(
            request_config={
                "datasource_name": "disney_csv",
                "asset_name": "disney_asset1",
                "request_dict": {},
            }
        )

        validation = self.create_validation(
            batch_request=batch_req, expectation_suite="disney_nullity_verification"
        )

        validations = self.get_validations(validation=validation)

        checkpoint_name = "TICKET_REGISTRATION_VIEW_V2"

        self.add_checkpoint(checkpoint_name=checkpoint_name, validations=validations)

    # br = (
    #     disney_context.context.get_datasource("disney_csv")
    #     .get_asset("disney_asset1")
    #     .build_batch_request({"year": "2023"})
    # )

    # batches = disney_context.context.get_datasource("disney_csv")\
    #     .get_asset("disney_asset1").get_batch_list_from_batch_request(br)

    # batch_request_list = [batch.batch_request for batch in batches]

    # validations = [
    #     {
    #         "batch_request": batch.batch_request,
    #         "expectation_suite_name": "disney_nullity_verification"
    #     }
    #     for batch in batches
    # ]

    # checkpoint = disney_context.context.add_or_update_checkpoint(
    #     name="nullity_checkpoint",
    #     validations=validations,
    # )
