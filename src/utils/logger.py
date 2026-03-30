import logging
import time
from contextlib import contextmanager


def get_logger(name: str) -> logging.Logger:
    """
    Retourne un logger prêt à l'emploi.
    Si aucun handler global n'existe (exécution locale), initialise une config minimale.
    """
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        )
    return logging.getLogger(name)


@contextmanager
def log_step(logger: logging.Logger, step_name: str, **meta):
    """
    Log structuré autour d'une étape (début/erreur/fin + durée).
    """
    start = time.perf_counter()
    try:
        if meta:
            logger.info("STEP_START %s meta=%s", step_name, meta)
        else:
            logger.info("STEP_START %s", step_name)
        yield
    except Exception:
        logger.exception("STEP_ERROR %s meta=%s", step_name, meta)
        raise
    finally:
        elapsed = time.perf_counter() - start
        logger.info("STEP_END %s elapsed_seconds=%.3f", step_name, elapsed)
