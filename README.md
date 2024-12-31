---
marp: true
---

# Great Expectations - Use Case Disney

Validation de la qualité des données avec la librairie [Great Expectations](https://greatexpectations.io/).

Great Expectations est une librairie Python pour définir est valider les états acceptables des données.

---

1. Initialiser la base de données PostgreSQL:

    ```bash
    docker compose up
    ```

    Voir [Compose Yaml file](docker-compose.yml) pour la configuration de la base de données PostgreSQL.

2. Insérer les données dans la base de données:

    ```bash
    python dbinit.py
    ```

3. Initialiser le contexte Great Expectations avec une configuration initiale:

    ```bash
    python main.py
    ```

4. Modifier le contexte créé précédement pour ajouter la base de données comme source de données:

    ```bash
    python db_data_source_config.py
    ```

5. Lancer les tests:

    ```bash
    python run_tests.py <nom_du_checkpoint>
    ```

    Exemple de tests à lancer pour le contexte créé précédement:

    * Pour la source de données fichiers (`.csv`)

        ```bash
        python run_tests.py TICKET_REGISTRATION_VIEW_V2
        ```

    * Pour la source de données SQL (Base de données PostgreSQL)

        ```bash
        python run_tests.py TICKET_REGISTRATION_VIEW_V2_SQL
        ```

    Les resultats des tests sont enregistrés dans:
    * [HTML](./disney/great_expectations/uncommitted/data_docs/local_site/)
    * [JSON](./disney/great_expectations/uncommitted/validations/)
