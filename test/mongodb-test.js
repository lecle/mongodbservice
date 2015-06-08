var mongodb = require('../lib/mongodb');

var req = {
    data : {
        collectionName : 'testcol',
        query : {where : {test : 'data', _className : 'test'}},
        data : {
            _className : 'test',
            test : 'data',
            num : 1,
            "point1": {
                "type": "Point",
                "coordinates": [
                    -73.97,
                    40.77
                ]
            },
            "point2": {
                "type": "Point",
                "coordinates": [
                    -72.97,
                    40.77
                ]
            },
            "point3": {
                "type": "Point",
                "coordinates": [
                    -71.97,
                    40.77
                ]
            },
            "point4": {
                "type": "Point",
                "coordinates": [
                    -71.97,
                    40.77
                ]
            }
        },
        group : {
            key : ['_className'],
            cond : {},
            initial : {"count":0},
            reduce : "function (obj, prev) { prev.count++; }"
        },
        aggregate : [
            { $match : { test : 'data2' }},
            { $group : {
                _id : { test : "$test" },
                num : { $sum : "$num" }
            }}
        ]
    }
};

var res = function(done) {

    return {
        send : function() {done();},
        error : function(err) {done(err);}
    };
};

describe('mongodb', function() {
    describe('#init()', function() {
        it('should initialize without error', function(done) {

            // manager service load
            var dummyContainer = {
                addListener:function(){},
                getConfig:function(){return null;},
                log : {
                    info : function(log) { console.log(log)},
                    error : function(log) { console.log(log)}
                },
                getService : function(name) {

                    return {
                        then : function(callback){ callback({
                            send : function(command, data, callback) {

                            callback(null, {data : {}});
                        }});

                            return {fail : function(){}};
                        }
                    };
                }
            };

            mongodb.init(dummyContainer, function(err) {

                mongodb.close(done);
            });
        });
    });

    describe('#insert()', function() {
        it('should insert without error', function(done) {

            mongodb.insert(req, res(done));
        });
    });

    describe('#findOne()', function() {
        it('should findOne without error', function(done) {

            mongodb.findOne(req, res(done));
        });
    });

    describe('#find()', function() {
        it('should find without error', function(done) {

            mongodb.find(req, res(done));
        });
    });

    describe('#count()', function() {
        it('should count without error', function(done) {

            mongodb.count(req, res(done));
        });
    });

    describe('$near', function() {

        it('should findOne($near) without error', function(done) {

            var reqNear = {data : {collectionName : 'testcol', query : {where : {point1 : {$near : { $geometry: { type: "Point",  coordinates: [ -73.9667, 40.78 ] }, $minDistance: 1000, $maxDistance: 5000} } }}}};
            mongodb.findOne(reqNear, res(done));
        });

        it('should find($near) without error', function(done) {

            var reqNear = {data : {collectionName : 'testcol', query : {where : {point2 : {$near : { $geometry: { type: "Point",  coordinates: [ -73.9667, 40.78 ] }, $minDistance: 1000, $maxDistance: 5000} } }}}};
            mongodb.find(reqNear, res(done));
        });

        it('should find(count, $near) without error', function(done) {

            var reqNear = {data : {collectionName : 'testcol', count : 1, query : {where : {point3 : {$near : { $geometry: { type: "Point",  coordinates: [ -73.9667, 40.78 ] }, $minDistance: 1000, $maxDistance: 5000} } }}}};
            mongodb.find(reqNear, res(done));
        });

        it('should count($near) without error', function(done) {

            var reqNear = {data : {collectionName : 'testcol', query : {where : {point4 : {$near : { $geometry: { type: "Point",  coordinates: [ -73.9667, 40.78 ] }, $minDistance: 1000, $maxDistance: 5000} } }}}};
            mongodb.count(reqNear, res(done));
        });
    });

    describe('#group()', function() {
        it('should group without error', function(done) {

            mongodb.group(req, res(done));
        });
    });

    describe('#update()', function() {
        it('should update without error', function(done) {

            req.data.data = {test:'data2'};

            mongodb.update(req, res(done));
        });
    });

    describe('#aggregate()', function() {
        it('should aggregate without error', function(done) {

            mongodb.aggregate(req, res(done));
        });
    });

    describe('#stats()', function() {
        it('should stats without error', function(done) {

            mongodb.stats(req, res(done));
        });
    });

    describe('use ISODate', function() {
        it('should stats without error', function(done) {

            req.data.query.where = { updatedAt: { '$gt': {"$ISODate" : "2013-01-23T02:46:54.429Z" } } };

            mongodb.find(req, res(done));
        });
    });

    describe('#remove()', function() {
        it('should remove without error', function(done) {

            mongodb.remove(req, res(done));
        });
    });
});
