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
    // format time as DD.MM.YYYY HH24:MI:SS.MS
    let date = new Date();
    let time = date.getDate() + "." + (date.getMonth() + 1) + "." + date.getFullYear() + " " + date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds() + "." + date.getMilliseconds();

    return {
        fin: "WVWIAmVeryRandom",
        zeit: time,
        geschwindigkeit: Math.random() * 200,
        ort: 3,
    };
}
