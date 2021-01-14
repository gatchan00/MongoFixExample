#CONFIG
batchSize = 5000
latestOffset = None
mongoConfig = {'fullUrl': "mongodb+srv://localhost/mapfre?retryWrites=true",#None, #"mongodb+srv://mvp_ficha_compass:<password>@cluster-ric-mvp-tu2rr.azure.mongodb.net/<dbname>?retryWrites=true&w=majority"
               #'host': 'localhost',
               'port': 27017,
               'db': 'mapfre',
               'collection': 'clientes',
               'pkName': '_id',
               'newArrayName': 'polizas'}
