import logging

from contexts.disney_context.DisneyContext import DisneyContext


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

    disney_context = DisneyContext()

    disney_context.initialize_disney_context()
