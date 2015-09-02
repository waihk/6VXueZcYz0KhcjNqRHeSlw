var conn = require('./connection');
var async = require('async');
var Request = require('request');

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

init(function(){
    bs_client
    .on('connect', function()
    {
        // client can now be used
        bs_client.watch('waihk', function(err, numwatched) {
            console.log('watching waihk:', numwatched)
            /*bs_client.list_tubes_watched(function(err, tubelist) {
                console.log(tubelist);
            });*/
            var job1 = {};
            var reserve = function() {
                bs_client.reserve(function(err, jobid, payload) {
                    if(err) console.log('err: ', err);
                    else{
                        //console.log('Time: ', new Date());
                        console.log(new Date(), 'got job: ', jobid);
                        //console.log('got job data: ', Buffer.isBuffer(payload),  payload.length, payload.toString());
                        job1 =  JSON.parse(payload.toString());
                        console.log('got job data: ',  JSON.stringify(job1));
                        if(!job1.tube_name||job1.tube_name!='waihk')
                            bs_client.release(jobid, null, null, function(err) {
                                console.log(err);
                                reserve();
                            });
                        else
                            bs_client.destroy(jobid, function(err) {
                                console.log('destroyed job');
                                getRate(job1.data, function(success){
                                    if(success){
                                        job1.times = job1.times + 1;
                                        job1.retry = 0;
                                        if(job1.times >= 10){
                                            //process.exit();
                                            reserve();
                                        }else
                                            bs_client.put(100, 60, 10, JSON.stringify(job1), function(err, jobid){
                                                console.log(new Date(), jobid);
                                                reserve();
                                            });
                                    }else{
                                        job1.retry = job1.retry + 1;
                                        if(job1.retry >= 3){
                                            //process.exit();
                                            reserve();
                                        }else
                                            bs_client.put(100, 3, 10, JSON.stringify(job1), function(err, jobid){
                                                console.log(new Date(), jobid);
                                                reserve();
                                            });
                                    }
                                    
                                });
                                //reserve();
                            });
                    }
                });
            };
            reserve();
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

var getRate = function (data, cb){
    var url = 'https://currency-api.appspot.com/api/'+data.from+'/'+data.to+'.JSON';
    //console.log(url);
    Request.get(url, function(e,r,body){
        var obj = JSON.parse(body);
        console.log(JSON.stringify(obj));
        if(obj.success){
            var rate = Math.round((parseFloat(obj.rate) + 0.00001) * 100) / 100;
            //console.log(rate);
            var save_object={"from": data.from,
                            "to": data.to,
                            "created_at": (new Date()).getTime(),
                            "rate": rate.toString()
            };
            mongo_client.collection('rates').save(save_object, function(e,r){
                cb(obj.success);
            });
        }else{
            cb(obj.success);
        }
    });
};

var getYORate = function (data, cb){
    var url = 'https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.xchange%20where%20pair%20in%20(%22'+data.from+data.to+'%22)&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys';
    //console.log(url);
    Request.get(url, function(e,r,body){
        var obj = JSON.parse(body);
        //console.log(obj);
        cb();
    });
};