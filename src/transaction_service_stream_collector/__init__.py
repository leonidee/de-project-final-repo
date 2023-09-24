import os
import sys
import warnings

import dotenv

sys.path.append(os.getenv("APP_PATH"))
from src.transaction_service_stream_collector.query import get_query

if not dotenv.load_dotenv():
    warnings.warn(
        message=".env not loaded or not found. Create one or set required env variables manually",
        category=RuntimeWarning,
    )
