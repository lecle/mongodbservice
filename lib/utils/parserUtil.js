"use strict";

module.exports.parseReservedKey = function(query) {

    for(var key in query) {

        if(query[key] && typeof(query[key]) === 'object') {

            if(query[key]['$ISODate']) {

                query[key] = new Date(query[key]['$ISODate']);
            } else {

                module.exports.parseReservedKey(query[key]);
            }
        }
    }
};
