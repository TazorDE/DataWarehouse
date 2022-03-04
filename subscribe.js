// set up hivemq
var mqtt = require("mqtt");
var client = mqtt.connect("mqtt://broker.hivemq.com");

// set up postgres database
var pg = require("pg");
var conString = "postgres://postgres:setpassword@localhost:5432/postgres";
var pgClient = new pg.Client(conString);
pgClient.connect();

client.on("connect", function () {
    console.log("connected");
    client.subscribe("DataMgmt/FIN");
    client.on("message", handleMessage);
});

function handleMessage(topic, message) {
    console.log(topic.toString(), JSON.parse(message.toString()));
    

    // insert data into Postgres database
    //pgClient.query(``);
}