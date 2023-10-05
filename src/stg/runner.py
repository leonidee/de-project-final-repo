import os
import sys
from datetime import datetime

import dotenv

# sys.path.append(os.getenv("APP_PATH"))
sys.path.append("/home/de-user/code/de-project-final-repo")
from src import get_logger
from src.stg.processor import give_me_name

log = get_logger(__name__)

dotenv.load_dotenv()


def main() -> None:
    log.info("")
    stopwatch = datetime.now()

    give_me_name()

    log.info(f"Done! Its took: {datetime.now() - stopwatch}")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
