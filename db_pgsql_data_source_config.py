"""Ce script sert à ajouter une source de données,
un data asset, et un checkpoint (point de vérification des
données).

Ce script est aussi un exemple de modification du contexte après
sa création en obtenant le contexte déjà créé avec DisneyContext()
(get_context()) et en modifiant ce contexte.

"""
import logging

from config.DBConfig import DBConfig
from contexts.disney_context.DisneyContext import DisneyContext

conf = DBConfig()


def set_logger():
    logging.basicConfig(level=logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s]\t%(message)s", datefmt="%d/%m/%Y %I:%M:%S")
    file_handler = logging.FileHandler("output/log/run.log", mode="w")
    console_handler = logging.StreamHandler()
    file_handler.setFormatter(fmt)
    console_handler.setFormatter(fmt)
    logger = logging.getLogger("MainLogger")
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.propagate = False


if __name__ == "__main__":
    set_logger()

    # Obtention du contexte de disney créé
    disney_context = DisneyContext()

    # Ajout de d'une source de données postgres sql
    disney_context.disney_data_source.add_pg_sql_data_source(
        datasource_name="disney_sql",
        PG_DATABASE=conf.POSTGRES_DB,
        PG_USERNAME=conf.POSTGRES_USER,
        PG_PASSWORD=conf.POSTGRES_PASSWORD,
        PG_HOST=conf.POSTGRES_HOST,
        PG_PORT=conf.POSTGRES_PORT,
    )

    # Ajout du data asset à partir de la source de donnée
    # inseré précédement
    disney_context.disney_data_source.add_sql_query_data_asset(
        datasource_name="disney_sql",
        asset_name="disney_sql_query_asset",
        query=None,
        table_name="TICKET_REGISTRATION_VIEW_V2",
    )

    # Obtention du Batch request
    batch_req = disney_context.disney_data_source.get_sql_batch_request()

    # Création d'une validation
    validation = disney_context.disney_checkpoints.create_validation(
        # Prend un batch à tester
        batch_request=batch_req,
        # et une liste de tests à faire sur ce batch
        expectation_suite="disney_nullity_verification",
    )

    validations = disney_context.disney_checkpoints.get_validations(validation=validation)

    # Ajout d'un checkpoint
    checkpoint_name = "TICKET_REGISTRATION_VIEW_V2_SQL"
    disney_context.disney_checkpoints.add_checkpoint(
        # Nom du checkpoint à ajouter
        checkpoint_name=checkpoint_name,
        # Liste des validations à faire
        validations=validations,
    )
