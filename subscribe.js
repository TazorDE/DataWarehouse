// set up hivemq
const mqtt = require("mqtt");
const client = mqtt.connect("mqtt://broker.hivemq.com");
const dotenv = require("dotenv");
dotenv.config();

// set up postgres database
const pg = require("pg");
// connection string with environment variables
let dbUser = process.env.DB_USER;
let dbPassword = process.env.DB_PASSWORD;
const conString = `postgres://${dbUser}:${dbPassword}@localhost:5432/postgres`;
const pgClient = new pg.Client(conString);
pgClient.connect();

// when connected to hivemq
client.on("connect", function () {
    console.log("connected");
    client.subscribe("DataMgmt/FIN");
    client.on("message", handleMessage);
});

function handleMessage(topic, message) {
    console.log(topic.toString(), JSON.parse(message.toString()));
    
    // postgres insert statement
    let insertStatement = "INSERT INTO staging.messung (payload, quelle) VALUES ($1, $2)";
    let insertValues = [JSON.parse(message.toString()), "MQTT"];

    // insert into postgres
    pgClient.query(insertStatement, insertValues, function (err, result) {
        if (err) {
            console.log(err);
        } else {
            console.info(result);
        }
    });
}