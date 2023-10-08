from os import getenv

from s3fs import S3FileSystem

S3_ACCESS_KEY_ID = getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = getenv("S3_SECRET_ACCESS_KEY")
S3_ENDPOINT_URL = getenv("S3_ENDPOINT_URL")

if not all(
    [
        S3_ACCESS_KEY_ID,
        S3_SECRET_ACCESS_KEY,
        S3_ENDPOINT_URL,
    ]
):
    raise ValueError(
        "Required environment varialbes not set. See .env.template for more details"
    )


def get_client() -> S3FileSystem:
    return S3FileSystem(
        key=S3_ACCESS_KEY_ID,
        secret=S3_SECRET_ACCESS_KEY,
        endpoint_url=S3_ENDPOINT_URL,
    )
