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

    mongodb.group(req.data.collectionName, req.data.keys, req.data.query, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}

function onAggregate(req, res) {

    mongodb.aggregate(req.data.collectionName, req.data.query, function(err, document) {

        if(err)
            return res.error(err);

        res.send(document);
    });
}