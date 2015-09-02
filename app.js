var conn = require('./connection');
var async = require('async');

var conn_mgr = new conn();
//console.log(conn_mgr);

var bs_client;
var mongo_client;

var init = function(cb){
    console.log('init');
    async.parallel({
    bs: function(callback){conn_mgr.getBeanstalkClient(callback);},
    mongo: function(callback){conn_mgr.getMongoClient(callback);}
    },
    function(err, results) {
        //console.log(results);
        bs_client = results.bs;
        mongo_client = results.mongo;
        cb();
    });
};

var job1 = {tube_name:'waihk', times:0, retry:0, data:{from:"HKD",to:"USD"}};

init(function(){
    bs_client
    .on('connect', function()
    {
        // client can now be used
        bs_client.use('waihk', function(err, tname)
	    {
		    console.log("using " + tname);
		    bs_client.list_tube_used(function(err, tubename) {
		        console.log(tubename);
		    });
		    
		    bs_client.put(100, 60, 10, JSON.stringify(job1), function(err, jobid)
		    {
		        console.log(new Date(), 'queued a string reverse job in waihk: ', jobid);
		        process.exit();
		    });
	    });
    })
    .on('error', function(err)
    {
        // connection failure
        console.log('bs:', err);
    })
    .on('close', function()
    {
        // underlying connection has closed
    })
    .connect();
});


