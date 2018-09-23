var express = require('express');
var amqp = require('amqplib/callback_api');
var bodyParser = require("body-parser");

var app = express();
var port = 7788;

app.use(bodyParser.urlencoded({
    extended: true
}));
app.use(bodyParser.json());

app.get('/graph.facebook.com', (req, res) => {
    amqp.connect('amqp://localhost', (err, conn) => {
        conn.createChannel((err, ch) => {
            ch.assertQueue('', {
                exclusive: true
            }, (err, q) => {
                let correlationId = Math.random().toString();
                ch.consume(q.queue, (msg) => {
                    if (msg.properties.correlationId == correlationId) {
                        let response = JSON.parse(msg.content.toString());
                        res
                            .status(parseInt(response.status))
                            .json(response.result);
                    }
                }, {
                    noAck: true
                });
                ch.sendToQueue('rpc_queue',
                    new Buffer("request for get nodes"), {
                        correlationId: correlationId,
                        replyTo: q.queue
                    });
            });
        });
    });
});


app.post('/graph.facebook.com', (req, res) => {
    var requestBody = req.body;
    amqp.connect('amqp://localhost', (err, conn) =>{
        conn.createChannel( (err, ch) => {
            ch.assertQueue('', {
                exclusive: true
            }, (err, q) => {
                var correlationId = Math.random().toString();
                ch.consume(q.queue, (msg) => {
                    if (msg.properties.correlationId == correlationId) {
                        res
                            .status(201)
                            .json(msg.content.toString());
                    }
                }, {
                    noAck: true
                });
                ch.sendToQueue('rpc_queue_post',
                    new Buffer(JSON.stringify(requestBody)), {
                        correlationId: correlationId,
                        replyTo: q.queue,
                        content_type: 'application/json'
                    });
            });
        });
    });
});



app.listen(port, () => {
    console.log("app is listening at port: " + port);
});

module.exports = app;