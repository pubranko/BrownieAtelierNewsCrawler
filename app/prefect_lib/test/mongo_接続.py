from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel

# from pymongo import MongoClient

mongo = MongoModel()
crawler_response = CrawlerResponseModel(mongo)

# rec:Any = crawler_response.find_one(
#     {'url': 'https://www.sankei.com/article/20210925-P5S53DNIGZPR5IBRCMVY77IEBU/'},
# )

# response_body = pickle.loads(rec['response_body'])

# print(response_body)
