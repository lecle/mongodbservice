var mongodb = require('../lib/mongodb');

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

});