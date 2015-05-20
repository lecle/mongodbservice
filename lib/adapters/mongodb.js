var mongoFactory = require('mongo-factory');

var connectionString = 'mongodb://127.0.0.1:27017/testdb';

module.exports.setUrl = function(url, port, dbName) {

    connectionString = 'mongodb://' + url + ':' + port + '/' + dbName;
};

module.exports.insert = function(container, collectionName, data, callback) {

    getCollection(container, collectionName, function(err, collection) {

        if(err)
            return callback(err, null);

        data.createdAt = data.updatedAt = new Date();

        container.log.info('db insert', data);
        verifyData(data);

        try {
            collection.insert(data, function(err, doc) {

                if(err) {
                    container.log.error('db query error', err);
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

module.exports.update = function(container, collectionName, find, data, callback) {

    getCollection(container, collectionName, function(err, collection) {

        if(err)
            return callback(err, null);

        data.updatedAt = new Date();

        container.log.info('db update query', find);
        container.log.info('db update data', data);

        verifyData(data);

        var updateData = {};

        for(var key in data) {

            if(key.charAt(0) === '$') {

                updateData[key] = data[key];
            } else {

                if(!updateData.$set)
                    updateData.$set = {};

                updateData.$set[key] = data[key];
            }
        }

        collection.update(find.where, updateData, {safe:true}, function(err, doc) {

            if(err) {
                container.log.error('db query error', err.message);
                callback(err, null);
                return;
            }

            if(doc === 0)
                return callback(new Error('ResourceNotFound', 10147), null);

            callback(null, data);
        });
    });
};

module.exports.findOne = function(container, collectionName, data, callback) {

    getCollection(container, collectionName, function(err, collection) {

        if(err)
            return callback(err, null);

        container.log.info('db findOne', data);
        collection.findOne(data.where, function(err, doc) {

            if(err) {
                container.log.error('db query error', err.message);

                checkGeoNearIndex(err, collection, data.where, function(err) {

                    if(err)
                        return callback(err, null);

                    collection.findOne(data.where, function(err, doc) {

                        return callback(err, doc);
                    });
                });
            } else {

                callback(null, doc);
            }
        });
    });
};

module.exports.find = function(container, collectionName, data, callback) {

    getCollection(container, collectionName, function(err, collection) {

        if(err)
            return callback(err, null);

        container.log.info('db find', data);

        if(data.count && data.count === 1) {

            collection.count(data.where, function(err, num) {

                if(err) {

                    checkGeoNearIndex(err, collection, data.where, function(err) {

                        if(err)
                            return callback(err, null);

                        collection.count(data.where, function(err, num) {

                            return callback(err, num);
                        });
                    });
                } else {

                    callback(err, num);
                }
            });

            return;
        }

        var cursor = collection.find(data.where);

        if(data.order)
            cursor.sort(data.order);
        if(data.limit)
            cursor.limit(data.limit);
        else
            cursor.limit(10000); // default

        if(data.skip)
            cursor.skip(data.skip);

        cursor.toArray(function(err, docs) {

            if(err) {
                container.log.error('db query error', err.message);

                checkGeoNearIndex(err, collection, data.where, function(err) {

                    if(err)
                        return callback(err, null);

                    var cursor = collection.find(data.where);

                    if(data.order)
                        cursor.sort(data.order);
                    if(data.limit)
                        cursor.limit(data.limit);
                    if(data.skip)
                        cursor.skip(data.skip);

                    cursor.toArray(function(err, docs) {

                        return callback(err, docs);
                    });
                });
            } else {

                callback(null, docs);
            }
        });
    });
};

module.exports.count = function(container, collectionName, data, callback) {

    getCollection(container, collectionName, function(err, collection) {

        if(err)
            return callback(err, null);

        container.log.info('db count', data);
        collection.count(data.where, function(err, count) {

            if(err) {
                container.log.error('db query error', err.message);

                checkGeoNearIndex(err, collection, data.where, function(err) {

                    if(err)
                        return callback(err, null);

                    collection.count(data.where, function(err, num) {

                        return callback(err, num);
                    });
                });
            } else {

                callback(null, count);
            }
        });
    });
};

module.exports.remove = function(container, collectionName, data, callback) {

    getCollection(container, collectionName, function(err, collection) {

        if(err)
            return callback(err, null);

        container.log.info('db remove', data);
        collection.remove(data.where, function(err, count) {

            if(err) {
                container.log.error('db query error', err.message);
                callback(err, null);
                return;
            }

            callback(null, count);
        });
    });
};

module.exports.group = function(container, collectionName, data, callback) {

    getCollection(container, collectionName, function(err, collection, db) {

        if(err)
            return callback(err, null);

        container.log.info('db group');
        collection.group(data.key, data.cond, data.initial, data.reduce, data.finalize, function(err, docs) {

            if(err) {
                container.log.error('db query error', err.message);
                callback(err, null);
                return;
            }

            callback(null, docs);
        });
    });
};

module.exports.aggregate = function(container, collectionName, data, callback) {

    getCollection(container, collectionName, function(err, collection, db) {

        if(err)
            return callback(err, null);

        container.log.info('db aggregate');
        collection.aggregate(data, function(err, docs) {

            if(err) {
                container.log.error('db query error', err.message);
                callback(err, null);
                return;
            }

            callback(null, docs);
        });
    });
};

module.exports.stats = function(container, collectionName, callback) {

    getCollection(container, collectionName, function(err, collection, db) {

        if(err)
            return callback(err, null);

        container.log.info('db stats');
        collection.stats(function(err, doc) {

            if(err) {
                container.log.error('db query error', err.message);
                callback(err, null);
                return;
            }

            callback(null, doc);
        });
    });
};

function getCollection(container, collectionName, callback) {

    mongoFactory.getConnection(connectionString).then(function(db) {

        var collection = db.collection(collectionName);

        if(collection)
            callback(null, collection, db);
        else {

            container.log.error('collection not found');
            callback(new Error('collection not found'), null);
        }

    }).fail(function(err) {

        container.log.error('db connection error', err.message);
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

function checkGeoNearIndex(error, collection, query, cb) {

    var message = error.message;

    if(message.indexOf('unable to find index for $geoNear query') >= 0 || message.indexOf("can't find any special indices: 2d (needs index), 2dsphere (needs index)") >= 0) {

        function getPointColName(data) {

            for(var key in data) {

                if(typeof(data[key]) === 'object') {

                    if(data[key]['$near'])
                        return key;
                    else
                        getPointColName(data[key]);
                }
            }

            return null;
        }

        var columnName = getPointColName(query);

        if(!columnName)
            return cb(error);

        var ensureIndexData = {};
        ensureIndexData[columnName] = "2dsphere";

        collection.ensureIndex(ensureIndexData, {}, function (err, indexName) {

            cb(err);
        });

    } else {

        cb(error);
    }
}
