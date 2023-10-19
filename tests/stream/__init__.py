

import abc
import os  # noqa: F401, E402
import sys

import click

sys.path.append("/home/leonide/code/de-project-final-repo")
from components import first_func, super_func  # noqa: E402


@click.command()
@click.option("--one", default="World", help="Some description", type=str)
@click.option("--value", default=1, help="Some description", type=int)
def main(one: str, value: int) -> None:
    first_func(one)
    super_func(value)

    print(sys.path)

    


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(err)
        sys.exit(1)
