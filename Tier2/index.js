var express = require('express');
var amqp = require('amqplib/callback_api');
var mysql = require('mysql-clusterfarm');

var clusterConfig = {
    debug: true,
    useDeadlockHandling: false,
    deadlockConfig: {
        retries: 5,
        minMillis: 1,
        maxMillis: 100
    }
};
var poolClusterFarm = mysql.createPoolClusterFarm(clusterConfig);

poolClusterFarm.addMaster('MASTER0', {
    database: 'master',
    user: 'root',
    password: '',
    host: 'localhost'
});

poolClusterFarm.addMaster('MASTER0', {
    database: 'master2',
    user: 'root',
    password: '',
    host: 'localhost'
});
/*
poolClusterFarm.addSlave('SLAVE0', 'MASTER0', {
    database: 'slave',
    user: 'root',
    password: '',
    host: 'localhost'
});
*/

var app = express();
var port = 8888;


amqp.connect('amqp://localhost', (err, conn) => {
    conn.createChannel((err, ch) => {
        var q = 'rpc_queue';
        ch.assertQueue(q, {
            durable: false
        });
        console.log("Waiting for GET request from tier1");
        ch.prefetch(1);
        ch.consume(q, function reply(msg) {
            console.log("request for get nodes");
            var response = {
                "status": 200,
                "result": 'Done!'
            };
            poolClusterFarm.query('SELECT * FROM `nodes`', function (err, rows) {
                console.log(err);
                console.log(rows);
                response.result = rows;
                ch.sendToQueue(msg.properties.replyTo, new Buffer(JSON.stringify(response)), {
                    correlationId: msg.properties.correlationId
                });
                ch.ack(msg);
            });
        });
    });
});


amqp.connect('amqp://localhost', function (err, conn) {
    conn.createChannel(function (err, ch) {
        var q = 'rpc_queue_post';
        ch.assertQueue(q, {
            durable: false
        });
        console.log("Waiting for POST request from tier1");
        ch.prefetch(1);
        ch.consume(q, function reply(msg) {
            var res = {
                "status": 201,
                "result": ''
            };
            var req = JSON.parse(msg.content.toString());
            var nodename = req.name;
            poolClusterFarm.query('INSERT INTO `nodes` (`id`, `nodename`) VALUES(NULL, ?)', nodename, function (err, rows) {
                console.log(err);
                console.log(rows);
                if (err) {
                    res.status = 500;
                    res.result = err;
                } else {
                    res.result = rows;
                }
                ch.sendToQueue(msg.properties.replyTo, new Buffer(JSON.stringify(res)), {
                    correlationId: msg.properties.correlationId
                });
                ch.ack(msg);
            });
            
        });
    });
});

app.listen(port, () => {
    console.log("app is listening at port: " + port);
});

module.exports = app;