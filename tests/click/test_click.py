import click


@click.command()
@click.option("--name", default="World", help="Who to greet.")
@click.option("--count", default=1, help="How many greetings to print.")
def main(name: str, count: int):
    print(name, count)


if __name__ == "__main__":
    main()
