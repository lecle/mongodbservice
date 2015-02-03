"use strict";

exports.container = null;

exports.init = function(container, callback) {

    exports.container = container;

    callback(null);
};

exports.close = function(callback) {

    callback(null);
};
