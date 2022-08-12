import logging
from pathlib import Path

import asyncclick as click
from motor.motor_asyncio import AsyncIOMotorCollection as Collection

from mongo_migration.database import get_collection

from .errors import NotValidJsonError
from .migrate import deserialise


@click.group(invoke_without_command=True)
@click.version_option()
@click.option(
    "--log-level",
    default="DEBUG",
    type=click.Choice(list(logging._nameToLevel.keys())),
)
@click.pass_context
def main(ctx: click.Context, log_level: str):
    """The launch root.

    Args:
        ctx (click.Context): The click context.
        log_level (str): The minimum logging level to be displayed.
    """
    logging.basicConfig(level=log_level)

    if ctx.invoked_subcommand is None:
        click.echo(main.get_help(ctx))


@main.command()
@click.argument("filepath")
@click.option("--database", default="default", help="Which mongo database to use.")
@click.option("--collection", default="default", help="The person to greet.")
async def file(filepath: str, database: str, collection: str):
    """Simple program that puts a json file into a mongo database and collection."""
    logging.info(
        f"Putting file {filepath} into {database} database, {collection} collection"
    )

    coll: Collection = get_collection(database, collection)
    if not Path(filepath).exists():
        msg = f"Provided file {filepath} does not exist"
        logging.error(msg)
        raise FileNotFoundError(msg)

    if not filepath.endswith(".json"):
        msg = f"Provided file {filepath} does not end with .json - not a json file"
        logging.error(msg)
        raise NotValidJsonError(msg)

    data = deserialise(Path(filepath))

    await coll.insert_one(data)
    logging.info("document inserted")

    click.echo("Job Done")


@main.command()
@click.argument("filepath")
@click.option("--database", default="default", help="Which mongo database to use.")
@click.option("--collection", default="default", help="The person to greet.")
async def folder(filepath: str, database: str, collection: str):
    """Simple program that puts a folder of json files into mongo database
    and collection."""
    logging.info(
        f"Putting file {filepath} into {database} database, {collection} collection"
    )

    coll: Collection = get_collection(database, collection)
    if not Path(filepath).exists():
        msg = f"Provided folder {filepath} does not exist"
        logging.error(msg)
        raise FileNotFoundError(msg)

    files = []

    for file in Path(filepath).iterdir():
        if file.is_file() and str(file).endswith(".json"):
            files.append(file)

    for each_json in files:
        await coll.insert_one(deserialise(each_json))
