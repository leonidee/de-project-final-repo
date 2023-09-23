import sys
from os import getenv

import dotenv

dotenv.load_dotenv()

sys.path.append(getenv("APP_PATH"))
