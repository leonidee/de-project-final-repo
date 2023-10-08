import warnings

import dotenv

is_dotenv_loaded = dotenv.load_dotenv()

if not is_dotenv_loaded:
    warnings.warn(
        message=".env not loaded or not found. Create one or set required env variables manually",
        category=RuntimeWarning,
    )
