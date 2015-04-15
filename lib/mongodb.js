"use strict";

var mongodb = require('./adapters/mongodb');

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

function onInsert(req, res) {

    mongodb.insert(req.data.collectionName, req.data.data, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}

function onUpdate(req, res) {

    mongodb.update(req.data.collectionName, req.data.query, req.data.data, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}

function onFindOne(req, res) {

    mongodb.findOne(req.data.collectionName, req.data.query, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}

function onFind(req, res) {

    mongodb.find(req.data.collectionName, req.data.query, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}

function onCount(req, res) {

    mongodb.count(req.data.collectionName, req.data.query, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}

function onRemove(req, res) {

    mongodb.remove(req.data.collectionName, req.data.query, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}

function onGroup(req, res) {

    mongodb.group(req.data.collectionName, req.data.group, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}

function onAggregate(req, res) {

    mongodb.aggregate(req.data.collectionName, req.data.aggregate, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}

function onStats(req, res) {

    mongodb.stats(req.data.collectionName, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}
