"use strict";

var mongodb = require('./adapters/mongodb');
var parserUtil = require('./utils/parserUtil');
var _ = require('lodash');

exports.container = null;

exports.init = function(container, callback) {

    exports.container = container;

    container.addListener('insert', onInsert);
    container.addListener('update', onUpdate);
    container.addListener('findOne', onFindOne);
    container.addListener('find', onFind);
    container.addListener('count', onCount);
    container.addListener('remove', onRemove);
    container.addListener('group', onGroup);
    container.addListener('aggregate', onAggregate);
    container.addListener('stats', onStats);

    var config = container.getConfig('mongodb');

    if(config && config.host) {

        var port = '27017';
        var dbName = (process.env.NODE_ENV === 'test') ? 'testdb' : 'noservdb';

        if(config.port)
            port = config.port;

        if(config.db)
            dbName = config.db;

        mongodb.setUrl(config.host, port, dbName);
    }

    callback(null);
};

exports.close = function(callback) {

    callback(null);
};

exports.insert = onInsert;
exports.update = onUpdate;
exports.findOne = onFindOne;
exports.find = onFind;
exports.count = onCount;
exports.remove = onRemove;
exports.group = onGroup;
exports.aggregate = onAggregate;
exports.stats = onStats;

function onBefore(type, req, callback) {

    exports.container.getService('FUNCTIONS').then(function(functionService) {

        var param = {

            triggerType : 'before_' + type,
            parameter : req.data
        };

        functionService.send('run', param, function(err, reqChanged) {

            if(err)
                return callback(err, req);

            // arrangement
            if(_.isEmpty(reqChanged))
                return callback(err, req);

            if(reqChanged.data.query)
                req.data.query = reqChanged.data.query;
            if(reqChanged.data.data)
                req.data.data = reqChanged.data.data;
            if(reqChanged.data.group)
                req.data.group = reqChanged.data.group;
            if(reqChanged.data.aggregate)
                req.data.aggregate = reqChanged.data.aggregate;

            callback(err, req);
        });

    }).fail(function (err) {

        callback(err, req);
    });
}

function onAfter(type, req, document, callback) {

    exports.container.getService('FUNCTIONS').then(function(functionService) {

        var param = {
            triggerType : 'after_' + type,
            reqData : req.data,
            parameter : document
        };

        functionService.send('run', param, function(err, docChanged) {

            if(_.isEmpty(docChanged) || _.isEmpty(docChanged.data))
                return callback(err, document);

            callback(err, docChanged.data);
        });

    }).fail(function (err) {

        callback(err, document);
    });
}

function onInsert(req, res) {

    onBefore('insert', req, function(err, req) {

        parserUtil.parseReservedKey(req.data.data);

        mongodb.insert(exports.container, req.data.collectionName, req.data.data, function(err, document) {

            onAfter('insert', req, document, function(err, docChangedData) {

                if(err)
                    return res.error(err);

                res.send(docChangedData);
            });
        });
    });
}

function onUpdate(req, res) {

    onBefore('update', req, function(err, req) {

        parserUtil.parseReservedKey(req.data);

        mongodb.update(exports.container, req.data.collectionName, req.data.query, req.data.data, function(err, document) {

            onAfter('update', req, document, function(err, docChangedData) {

                if(err)
                    return res.error(err);

                res.send(docChangedData);
            });
        });
    });
}

function onFindOne(req, res) {

    onBefore('findone', req, function(err, req) {
        parserUtil.parseReservedKey(req.data.query);

        mongodb.findOne(exports.container, req.data.collectionName, req.data.query, function(err, document) {

            onAfter('findone', req, document, function (err, docChangedData) {

                if (err)
                    return res.error(err);

                res.send(docChangedData);
            });
        });
    });
}

function onFind(req, res) {

    onBefore('find', req, function(err, req) {
        parserUtil.parseReservedKey(req.data.query);

        mongodb.find(exports.container, req.data.collectionName, req.data.query, function(err, document) {

            onAfter('find', req, document, function (err, docChangedData) {

                if (err)
                    return res.error(err);

                res.send(docChangedData);
            });
        });
    });
}

function onCount(req, res) {

    onBefore('count', req, function(err, req) {
        parserUtil.parseReservedKey(req.data.query);

        mongodb.count(exports.container, req.data.collectionName, req.data.query, function(err, document) {

            onAfter('count', req, document, function (err, docChangedData) {

                if (err)
                    return res.error(err);

                res.send(docChangedData);
            });
        });
    });
}

function onRemove(req, res) {

    onBefore('remove', req, function(err, req) {
        parserUtil.parseReservedKey(req.data.query);

        mongodb.remove(exports.container, req.data.collectionName, req.data.query, function(err, document) {

            onAfter('remove', req, document, function (err, docChangedData) {

                if (err)
                    return res.error(err);

                res.send(docChangedData);
            });
        });
    });
}

function onGroup(req, res) {

    onBefore('group', req, function(err, req) {
        parserUtil.parseReservedKey(req.data.group);

        mongodb.group(exports.container, req.data.collectionName, req.data.group, function(err, document) {

            onAfter('group', req, document, function (err, docChangedData) {

                if (err)
                    return res.error(err);

                res.send(docChangedData);
            });
        });
    });
}

function onAggregate(req, res) {

    onBefore('aggregate', req, function(err, req) {
        parserUtil.parseReservedKey(req.data.aggregate);

        mongodb.aggregate(exports.container, req.data.collectionName, req.data.aggregate, function(err, document) {

            onAfter('aggregate', req, document, function (err, docChangedData) {

                if (err)
                    return res.error(err);

                res.send(docChangedData);
            });
        });
    });
}

function onStats(req, res) {

    mongodb.stats(exports.container, req.data.collectionName, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}
