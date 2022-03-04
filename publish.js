// set up hivemq
var mqtt = require("mqtt");
var client = mqtt.connect("mqtt://broker.hivemq.com");
var cron = require("node-cron");

console.log(generateData());

client.on("connect", function () {
    console.log("connected");
    client.subscribe("DataMgmt/FIN");
    // cronjob that runs every 5 seconds
    cron.schedule("*/5 * * * * *", function () {
        let data = generateData();
        console.log(data);
        client.publish("DataMgmt/FIN", JSON.stringify(data));
    });
});

function generateData() {
    return {
        fin: "WVWIAmVeryRandom",
        zeit: new Date().getTime(),
        geschwindigkeit: Math.random() * 100,
        ort: Math.floor(Math.random() * 100) + 1,
    };
}
