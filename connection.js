(function () {
    'use strict';
    
    var MONGO_URL = 'mongodb://127.0.0.1/aftership';

    // Declare imports
    var Request = require('request');
    var Fivebeans = require('fivebeans');
    var MongoClient = require('mongodb').MongoClient;
    var bs_client = null;
    var mongo_client = null;

    // Constructor
    function connectionMaganer() {
        //console.log('new connectionMaganer');
    }
    
    connectionMaganer.prototype.getBeanstalkClient = function (cb) {
        console.log('new getBeanstalkClient');
        if(bs_client){
            cb(null, bs_client);
        }else{
            Request.post({ url: 'http://challenge.aftership.net:9578/v1/beanstalkd', headers: {'aftership-api-key': 'a6403a2b-af21-47c5-aab5-a2420d20bbec'} }, function (e, r, body) {
                if(!e){
                    //console.log(body);
                    var obj = JSON.parse(body);
                    bs_client = new Fivebeans.client(obj.data.host, obj.data.port);
                }
                cb(e, bs_client);
            });
        }
    };
    
    connectionMaganer.prototype.getMongoClient = function (cb) {
        console.log('new getMongoClient');
        if(mongo_client){
            cb(null, mongo_client);
        }else{
            MongoClient.connect(MONGO_URL, function(err, database) {
				if(err) {
					console.log("Error during initial connection pool: " + err);
				}
				else {
					//console.log("Connection opened");
					mongo_client = database;
				}
				cb(err, mongo_client);
			});
        }
    };
    
    
    module.exports = connectionMaganer;
}())

