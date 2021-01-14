from pymongo import MongoClient, DESCENDING

from mongo_cfg_OID import mongoConfig, batchSize, latestOffset
from bson.objectid import ObjectId
# db.clientes.findOne({polizas:{$exists:false}})
def createMongoConnection(mongoConfig):
    if mongoConfig['fullUrl']:
        print("Full URL connection")
        client = MongoClient(mongoConfig['fullUrl'])
        db = client[mongoConfig['db']]
    else:
        client = MongoClient(mongoConfig['host'], mongoConfig['port'])
        db = client[mongoConfig['db']]
    mongoCollection = db[mongoConfig['collection']]
    return mongoCollection


def getOffsets(mongoCollection, latestOffset):
    fromOffset = latestOffset
    if not fromOffset:
        result = mongoCollection.find({}).sort(mongoConfig['pkName']).limit(1)
        fromOffset = ObjectId(result.next()[mongoConfig['pkName']])
        isResuming = False
    else:
        isResuming = True
    result = mongoCollection.find({}).sort(mongoConfig['pkName'], DESCENDING).limit(1)
    latestOffset = result.next()[mongoConfig['pkName']]

    return (fromOffset, latestOffset, isResuming)


def loop(mongoCollection, mongoConfig, fromOffset, latestOffset, batchSize, isResuming):
    condition = '$gte'
    if isResuming:
        condition = '$gt'
    fromKey = fromOffset
    toKey = mongoCollection.find({mongoConfig['pkName']: {condition: fromKey}}).sort(mongoConfig['pkName'])\
        .skip(batchSize).limit(1).next()[mongoConfig['pkName']]

    filterDoc = {'$and': [{mongoConfig['pkName']: {'$gte': fromKey}},
                          {mongoConfig['pkName']: {'$lte': toKey}},
                          {mongoConfig['newArrayName']: {'$exists': False}}]
                 }
    updateDoc = {'$set': {mongoConfig['newArrayName']: []}}
    mongoCollection.update_many(filterDoc, updateDoc)
    print("From " + str(fromKey) + ' to ' + str(toKey))
    #print(filterDoc)
    while (toKey<latestOffset):
        fromKey = toKey
        cursorToKey = mongoCollection.find({mongoConfig['pkName']: {'$gte': fromKey}}).sort(mongoConfig['pkName']) \
            .skip(batchSize).limit(1)
        aLista = list(cursorToKey)
        if len(aLista) > 0:
            toKey = aLista[0][mongoConfig['pkName']]
        else:
            toKey = latestOffset
        filterDoc = {'$and': [{mongoConfig['pkName']: {'$gt': fromKey}},
                              {mongoConfig['pkName']: {'$lte': toKey}},
                              {mongoConfig['newArrayName']: {'$exists': False}}]
                     }
        updateDoc = {'$set': {mongoConfig['newArrayName']: []}}
        mongoCollection.update_many(filterDoc, updateDoc)
        print("From " +str(fromKey) +' to '+str(toKey))
        #print(filterDoc)

if __name__ == '__main__':
    mongoCollection = createMongoConnection(mongoConfig)
    (fromOffset, latestOffset, isResuming) = getOffsets(mongoCollection, latestOffset)
    print("From " + str(fromOffset) + " to " + str(latestOffset))
    loop(mongoCollection, mongoConfig, fromOffset, latestOffset, batchSize, isResuming)

