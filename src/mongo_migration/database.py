import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from motor.motor_asyncio import AsyncIOMotorCollection as Collection

client: AsyncIOMotorClient = motor.motor_asyncio.AsyncIOMotorClient()


def get_collection(database: str, collection: str) -> Collection:
    coll: Collection = client[database][collection]
    return coll
