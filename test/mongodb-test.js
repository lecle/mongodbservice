var mongodb = require('../lib/mongodb');

var req = {
    data : {
        collectionName : 'testcol',
        query : {where : {test : 'data'}},
        data : {test : 'data'},
        keys : ['test']
    }
};

var res = function(done) {

    var res = {
        send : function() {done();},
        error : function(err) {done(err);}
    };

    return res;
};

describe('mongodb', function() {
    describe('#init()', function() {
        it('should initialize without error', function(done) {

            // manager service load
            var dummyContainer = {addListener:function(){}};

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

    describe('#remove()', function() {
        it('should remove without error', function(done) {

            mongodb.remove(req, res(done));
        });
    });
});