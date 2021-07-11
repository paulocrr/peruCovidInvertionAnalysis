import pymongo


class DatabaseSettings:
    def __init__(self):
        self.database_location = "localhost"
        self.port = 27017
        self.client = pymongo.MongoClient(self.database_location, self.port)
        self.db = self.client.data_science_project