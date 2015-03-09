var mongoFactory = require('mongo-factory');

var dbName = (process.env.NODE_ENV === 'test') ? 'testdb' : 'noservdb';

var connectionString = 'mongodb://127.0.0.1:27017/' + dbName;

module.exports.insert = function(collectionName, data, callback) {

    getCollection(collectionName, function(err, collection) {

        if(err)
            callback(err, null);

        data.createdAt = data.updatedAt = new Date();

        console.log('db insert', data);
        verifyData(data);

        try {
            collection.insert(data, function(err, doc) {

                if(err) {
                    console.log('db query error', err);
                    callback(err, null);
                    return;
                }

                if(!data.objectId) {

                    data.objectId = data._id.toHexString();

                    collection.save(data, function(err) {

                        callback(err, data);
                    });
                } else {

                    callback(null, data);
                }
            });
        } catch(e) {

            callback(e, null);
        }
    });
};

module.exports.update = function(collectionName, find, data, callback) {

    getCollection(collectionName, function(err, collection) {

        if(err)
            callback(err, null);

        data.updatedAt = new Date();

        console.log('db update query', find);
        console.log('db update data', data);

        verifyData(data);

        collection.update(find.where, {$set:data}, {safe:true}, function(err, doc) {

            if(err) {
                console.log('db query error', err.message);
                callback(err, null);
                return;
            }

            if(doc === 0)
                return callback(new Error('ResourceNotFound', 10147), null);

            callback(null, data);
        });
    });
};

module.exports.findOne = function(collectionName, data, callback) {

    getCollection(collectionName, function(err, collection) {

        if(err)
            callback(err, null);

        console.log('db findOne', data);
        collection.findOne(data.where, function(err, doc) {

            if(err) {
                console.log('db query error', err.message);
                callback(err, null);
                return;
            }

            if(doc && !doc.objectId) {

                doc.objectId = doc._id.toHexString();
                collection.save(doc, function(err) {});
            }


            callback(null, doc);
        });
    });
};

module.exports.find = function(collectionName, data, callback) {

    getCollection(collectionName, function(err, collection) {

        if(err)
            callback(err, null);

        console.log('db find', data);

        if(data.count && data.count === 1) {

            collection.count(data.where, function(err, num) {

                callback(err, num);

            });

            return;
        }


        var cursor = collection.find(data.where);

        if(data.order)
            cursor.sort(data.order);
        if(data.limit)
            cursor.limit(data.limit);
        if(data.skip)
            cursor.skip(data.skip);

        cursor.toArray(function(err, docs) {

            if(err) {
                console.log('db query error', err.message);
                callback(err, null);
                return;
            }

            for(var i= 0, cnt= docs.length; i<cnt; i++) {

                if(!docs[i].objectId) {

                    docs[i].objectId = docs[i]._id.toHexString();
                    collection.save(docs[i], function(err) {});
                }
            }

            callback(null, docs);
        });
    });
};

module.exports.count = function(collectionName, data, callback) {

    getCollection(collectionName, function(err, collection) {

        if(err)
            callback(err, null);

        console.log('db count', data);
        collection.count(data.where, function(err, count) {

            if(err) {
                console.log('db query error', err.message);
                callback(err, null);
                return;
            }

            callback(null, count);
        });
    });
};

module.exports.remove = function(collectionName, data, callback) {

    getCollection(collectionName, function(err, collection) {

        if(err)
            callback(err, null);

        console.log('db count', data);
        collection.remove(data.where, function(err, count) {

            if(err) {
                console.log('db query error', err.message);
                callback(err, null);
                return;
            }

            callback(null, count);
        });
    });
};

module.exports.group = function(collectionName, keys, condition, callback) {

    getCollection(collectionName, function(err, collection, db) {

        if(err)
            callback(err, null);

        console.log('db group');
        collection.group(keys, condition, {"count":0}, "function (obj, prev) { prev.count++; }", function(err, docs) {

            if(err) {
                console.log('db query error', err.message);
                callback(err, null);
                return;
            }

            callback(null, docs);
        });
    });
};

module.exports.aggregate = function(collectionName, data, callback) {

    getCollection(collectionName, function(err, collection, db) {

        if(err)
            callback(err, null);

        console.log('db aggregate');
        collection.aggregate(data, function(err, docs) {

            if(err) {
                console.log('db query error', err.message);
                callback(err, null);
                return;
            }

            callback(null, docs);
        });
    });
};

module.exports.stats = function(collectionName, callback) {

    getCollection(collectionName, function(err, collection, db) {

        if(err)
            callback(err, null);

        console.log('db stats');
        collection.stats(function(err, doc) {

            if(err) {
                console.log('db query error', err.message);
                callback(err, null);
                return;
            }

            callback(null, doc);
        });
    });
};

function getCollection(collectionName, callback) {

    mongoFactory.getConnection(connectionString).then(function(db) {

        var collection = db.collection(collectionName);

        if(collection)
            callback(null, collection, db);
        else {

            console.log('collection not found');
            callback(new Error('collection not found'), null);
        }

    }).fail(function(err) {

        console.log('db connection error', err.message);
        callback(err, null);
    });
}

String.prototype.trim = function() {
    return this.replace(/(^\s*)|(\s*$)/gi, "");
};

function verifyData(data) {

    for(var key in data) {

        if(typeof(data[key]) === 'object')
            verifyData(data[key]);
        else if(typeof(data[key]) === 'string')
            data[key] = data[key].trim();
    }
}