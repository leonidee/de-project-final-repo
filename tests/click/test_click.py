import click

    def some_fucnt()

@click.command()
@click.option("--name", help="Who to greet.", required=True, type=click.Choice(['prod','test','dev'], case_sensitive=True))
def main(name: str):
    print(name)



if __name__ == "__main__":
    main()
