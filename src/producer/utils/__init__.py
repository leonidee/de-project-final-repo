import uuid
from typing import Any, Iterable

DEFAULT_NAMESPACE = "7f288a2e-0ad0-4039-8e59-6c9838d87307"


def get_uuid(obj: Iterable[Any] | Any) -> str:
    if isinstance(obj, Iterable):
        objs: str = "".join([str(_) for _ in obj])
    else:
        objs: str = str(obj)

    return str(
        uuid.uuid5(
            namespace=uuid.UUID(DEFAULT_NAMESPACE),
            name=objs,
        )
    )
