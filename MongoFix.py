from pymongo import MongoClient, DESCENDING

from mongo_cfg import mongoConfig, batchSize, latestOffset

def createMongoConnection(mongoConfig):
    client = MongoClient(mongoConfig['host'], mongoConfig['port'])
    db = client[mongoConfig['db']]
    mongoCollection = db[mongoConfig['collection']]
    return mongoCollection


def getOffsets(mongoCollection, latestOffset):
    fromOffset = latestOffset
    if not fromOffset:
        result = mongoCollection.find({}).sort(mongoConfig['pkName']).limit(1)
        fromOffset = int(result.next()[mongoConfig['pkName']])
    else:
        fromOffset += 1
    result = mongoCollection.find({}).sort(mongoConfig['pkName'], DESCENDING).limit(1)
    latestOffset = int(result.next()[mongoConfig['pkName']])

    return (fromOffset, latestOffset)


def loop(mongoCollection, mongoConfig, fromOffset, latestOffset):
    fromKey = fromOffset
    toKey = fromOffset+(batchSize-1)
    while (fromKey<latestOffset):
        filterDoc = {'$and':[{mongoConfig['pkName']: {'$gte': fromKey}},
                             {mongoConfig['pkName']: {'$lte': toKey}},
                             {mongoConfig['newArrayName']: {'$exists': False}}]
                     }
        updateDoc = {'$set': {mongoConfig['newArrayName']: []}}
        mongoCollection.update_many(filterDoc, updateDoc)
        print("From " +str(fromKey) +' to '+str(toKey))
        print(filterDoc)
        fromKey += batchSize
        toKey += batchSize

if __name__ == '__main__':
    mongoCollection = createMongoConnection(mongoConfig)
    (fromOffset, latestOffset) = getOffsets(mongoCollection, latestOffset)
    print("From " + str(fromOffset) + " to " + str(latestOffset))
    loop(mongoCollection, mongoConfig, fromOffset, latestOffset)

