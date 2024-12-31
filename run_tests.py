import argparse
import os
from pathlib import Path

import great_expectations as gx


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("checkpoint", type=str, help="Nom de la table/checkpoint")

    args = parser.parse_args()

    path_to_gx_yaml = "disney/great_expectations/"
    GX_YAML = "great_expectations.yml"

    absolute_path_gx_folder = os.path.join(os.path.abspath("."), path_to_gx_yaml)

    absolute_path_gx_yaml = os.path.join(os.path.abspath("."), path_to_gx_yaml, GX_YAML)

    if os.path.isfile(absolute_path_gx_yaml):

        checkpoint_name = args.checkpoint

        context = gx.get_context(context_root_dir=Path(absolute_path_gx_folder))

        checkpoint = context.get_checkpoint(name=checkpoint_name)

        checkpoint_result = checkpoint.run(run_name="disney_null_columns_tests_run (run_tests.py)")

    else:
        raise FileNotFoundError(f"Aucun contexte détecté dans '{absolute_path_gx_yaml}' ")
