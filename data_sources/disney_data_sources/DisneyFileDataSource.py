import logging
import os

import contexts.disney_context as dc
from config.DBConfig import DBConfig
from contexts.BaseContext import BaseContext
from data_sources.FileDataSource import FileDataSource
from exceptions.disney_exceptions.DisneyFileDataSourceError import DisneyFileDataSourceError


class DisneyFileDataSource(FileDataSource):

    conf = DBConfig()

    def __init__(self, context: BaseContext) -> None:
        if not isinstance(context, dc.DisneyContext):
            raise DisneyFileDataSourceError(f"{context.context_name} n'est pas un context disney")
        super().__init__(context)

    def list_data_source_names(self) -> list[str]:
        """Liste des sources de données du context."""

        data_source_names = []

        for data_source in self.context.context.list_datasources():
            data_source_names.append(data_source["name"])

        return data_source_names

    def list_data_source_base_directories(self) -> list[str]:
        """Liste des répertoires des sources de données."""

        data_source_base_directories = []

        for data_source in self.context.context.list_datasources():
            data_source_base_directories.append(data_source["base_directory"])

        return data_source_base_directories

    def add_file_data_source(self, datasource_config: dict) -> None:
        """Ajout un source de données fichiers.

        Args:
            datasource_config : dict
                Dictionnaire contenant la configuration de la source de données.

        Raises:
            DisneyFileDataSourceError: Si le context séléctionné n'est pas un context disney.

        """

        # Verifier si le context séléctionné est un context disney.
        if not isinstance(self.context, dc.DisneyContext):
            raise DisneyFileDataSourceError("Le context sélectionné n'est pas un context Disney.")

        # Extraction de la config
        datasource_name = datasource_config.get("datasource_name", "default_datasource")
        path_to_folder_containing_csv_files = datasource_config.get(
            "datasource_path",
            os.path.join(os.path.abspath("."), self.conf.base_data_file_directory),
        )

        logger = logging.getLogger("MainLogger")

        # Si la source de données n'est pas dans la liste des sources de données
        # alors en ajouter une nouvelle source de données.
        if datasource_name not in self.list_data_source_names():
            # Create a Datasource
            logger.info(f"Ajout de la source de données '{datasource_name}'.")
            self.context.context.sources.add_pandas_filesystem(
                name=datasource_name, base_directory=path_to_folder_containing_csv_files
            )

        # Sinon la source de données existe déjà.
        else:
            logger.warning(
                f"Le nom '{datasource_name}' est déjà utilisé"
                f" pour une source de données dans le context '{self.context.context_name}'."
            )
            logger.warning(
                f"'{datasource_name} n'a pas été ajouté"
                f" aux sources de données dans le context '{self.context.context_name}'."
            )

    def add_file_data_asset(self, asset_config: dict) -> None:
        """Ajoute un data asset à une source de données fichiers.

        Args:
            asset_config : dict
                Configuration du data asset à ajouter.

        Raises:
            DisneyFileDataSourceError: Si le context séléctionné n'est pas un context disney.

        """
        logger = logging.getLogger("MainLogger")

        # Verifier si le context séléctionné est un context disney.
        if not isinstance(self.context, dc.DisneyContext):
            raise DisneyFileDataSourceError("Le context sélectionné n'est pas un context Disney.")

        # Extraction de la config
        batch_regex = self.conf.csv_file_name
        datasource_name = asset_config.get("datasource_name", "default_datasource")
        asset_name = asset_config.get("asset_name", "default_asset")
        batching_regex = asset_config.get(
            "batching_regex",
            batch_regex
            # r"PROD_CURATED_DLP_TICREGISTRATIONS_VIEW_V2_(?P<year>\d{4})_(?P<month>\d{2})\.csv"
        )

        # Verifie si la source de données existe.
        datasource = self._get_datasource(datasource_name)

        # Verifie si le data asset existe déja.
        if asset_name not in datasource.get_asset_names():

            # Add a Data Asset to the Datasource
            logger.info(f"Ajout du data asset '{asset_name}'.")
            datasource.add_csv_asset(name=asset_name, batching_regex=batching_regex)

        else:
            logger.warning(
                f"Le data asset '{asset_name}' existe déja"
                f" pour la source de données '{datasource_name}'."
            )

    def get_batch_request(self, request_config: dict) -> None:
        """Retourne une requête de batch de données.

        Args:
            request_config : dict
                Dictionnaire contenant la configuration de la requête de batch
                à obtenir.
                Cela inclue:
                    datasource_name: Le nom de la source de données à requêter.
                    asset_name: le nom du data asset à utiliser pour requêter la donnée.
                    request_dict: les paramètres à passer à l'expression régulière pour la
                        requête de données.
                        (Example: avec l'expression régulière suivante:
                        # r"PROD_CURATED_DLP_TICREGISTRATIONS_VIEW_V2_(?P<year>\\d{4}).csv", on lui
                        passe comme paramètre {"year": "2023"} pour obtenir le fichier en batch
                        "PROD_CURATED_DLP_TICREGISTRATIONS_VIEW_V2_2023.csv")

        Returns:
            Le batch de données à valider.

        Raises:
            DisneyFileDataSourceError:
                Si le context n'est pas un context disney;
                Si la source de données n'existe pas;
                Si le data asset n'existe pas.

        """
        logger = logging.getLogger("MainLogger")

        # Vérifier si le context donné est une context disney.
        if not isinstance(self.context, dc.DisneyContext):
            raise DisneyFileDataSourceError("Le context sélectionné n'est pas un context Disney.")

        datasource_name = request_config.get("datasource_name", "default_datasource")
        asset_name = request_config.get("asset_name", "default_asset")
        request_dict = request_config.get("request_dict", {})
        # request_dict = request_config.get("request_dict", {"year": "2023"})

        asset = self._get_data_asset(datasource_name, asset_name)

        # Request data from a Data Asset.
        logger.info(
            f"Acquisition du batch de données à partir de la source de donnée"
            f" '{datasource_name}' et du data asset '{asset_name}'."
        )
        return asset.build_batch_request(options=request_dict)

    # pylint: disable=too-many-arguments
    def add_pg_sql_data_source(
        self,
        datasource_name: str = "disney_sql",
        PG_DATABASE: str = conf.POSTGRES_DB,
        PG_USERNAME: str = conf.POSTGRES_USER,
        PG_PASSWORD: str = conf.POSTGRES_PASSWORD,
        PG_HOST: str = conf.POSTGRES_HOST,
        PG_PORT: str = conf.POSTGRES_PORT,
    ) -> None:

        PG_CONNECTION_STRING = (
            f"postgresql+psycopg2://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
        )

        # Verifier si le context séléctionné est un context disney.
        if not isinstance(self.context, dc.DisneyContext):
            raise DisneyFileDataSourceError("Le context sélectionné n'est pas un context Disney.")

        logger = logging.getLogger("MainLogger")

        # Si la source de données n'est pas dans la liste des sources de données
        # alors en ajouter une nouvelle source de données.
        if datasource_name not in self.list_data_source_names():
            # Create a Datasource
            logger.info(f"Ajout de la source de données '{datasource_name}'.")

            self.context.context.sources.add_sql(
                name=datasource_name, connection_string=PG_CONNECTION_STRING
            )

        # Sinon la source de données existe déjà.
        else:
            logger.warning(
                f"Le nom '{datasource_name}' est déjà utilisé"
                f" pour une source de données dans le context '{self.context.context_name}'."
            )
            logger.warning(
                f"{datasource_name} n'a pas été ajouté"
                f" aux sources de données dans le context '{self.context.context_name}'."
            )

    def add_sql_data_asset(
        self,
    ):
        pass

    def add_sql_query_data_asset(
        self,
        datasource_name: str = "disney_sql",
        asset_name: str = "disney_sql_query_asset",
        query: str = None,
        table_name: str = "TICKET_REGISTRATION_VIEW_V2",
    ) -> None:
        """Ajoute un data asset à une source de données.

        Args:
            asset_config : dict
                Configuration du data asset à ajouter.

        Raises:
            DisneyFileDataSourceError: Si le context séléctionné n'est pas un context disney.

        """
        logger = logging.getLogger("MainLogger")

        # Verifier si le context séléctionné est un context disney.
        if not isinstance(self.context, dc.DisneyContext):
            raise DisneyFileDataSourceError("Le context sélectionné n'est pas un context Disney.")

        # Requête à passer pour créer le data asset.
        # Si aucune requête n'est passée en paramètre,
        # la table 'table_name' sera prise comme data asset.
        table_name = "TICKET_REGISTRATION_VIEW_V2"
        if query is None:
            disney_query = f"SELECT * FROM {table_name}"
        else:
            disney_query = query

        # Verifie si la source de données existe.
        datasource = self._get_datasource(datasource_name=datasource_name)

        # Verifie si le data asset existe déja.
        if asset_name not in datasource.get_asset_names():

            # Add a Data Asset to the Datasource
            logger.info(f"Ajout du data asset {asset_name}.")
            datasource.add_query_asset(
                name=asset_name,
                query=disney_query,
            )

        else:
            logger.warning(
                f"Le data asset '{asset_name}' existe déja"
                f" pour la source de données '{datasource_name}'."
            )

    def get_sql_batch_request(
        self,
        datasource_name: str = "disney_sql",
        asset_name: str = "disney_sql_query_asset",
    ):
        if not isinstance(self.context, dc.DisneyContext):
            raise DisneyFileDataSourceError("Le context sélectionné n'est pas un context Disney.")

        sql_data_asset = self._get_data_asset(
            datasource_name=datasource_name, asset_name=asset_name
        )

        return sql_data_asset.build_batch_request()

    def _get_datasource(self, datasource_name):
        logger = logging.getLogger("MainLogger")
        if datasource_name in self.list_data_source_names():
            datasource = self.context.context.get_datasource(datasource_name=datasource_name)

        else:
            logger.critical(f"La source de données '{datasource_name}' n'est pas définie.")
            raise DisneyFileDataSourceError(
                f"La source de données '{datasource_name}' n'est pas définie."
            )

        return datasource

    def _get_data_asset(self, datasource_name, asset_name):
        logger = logging.getLogger("MainLogger")

        # Verifier si la source de données existe.
        datasource = self._get_datasource(datasource_name)

        # Vérifier si le data asset existe.
        if asset_name in datasource.get_asset_names():
            asset = datasource.get_asset(asset_name=asset_name)

        else:
            logger.critical(
                f"Le data asset '{asset_name}' n'est pas défini"
                f" pour la source de données '{datasource_name}'."
            )
            raise DisneyFileDataSourceError(
                f"Le data asset '{asset_name}' n'est pas défini"
                f" pour la source de données '{datasource_name}'."
            )

        return asset
