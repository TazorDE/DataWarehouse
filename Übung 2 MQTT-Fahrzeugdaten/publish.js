// set up hivemq
var mqtt = require("mqtt");
var client = mqtt.connect("mqtt://broker.hivemq.com");
var cron = require("node-cron");

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
    let day = date.getDate();
    let month = date.getMonth() + 1;
    let year = date.getFullYear();
    let hours = date.getHours();
    let minutes = date.getMinutes();
    let seconds = date.getSeconds();
    let milliseconds = date.getMilliseconds();
    if (day < 10) {
        day = "0" + day;
    }
    if (month < 10) {
        month = "0" + month;
    }
    if (hours < 10) {
        hours = "0" + hours;
    }
    if (minutes < 10) {
        minutes = "0" + minutes;
    }
    if (seconds < 10) {
        seconds = "0" + seconds;
    }
    if (milliseconds < 10) {
        milliseconds = "00" + milliseconds;
    } else if (milliseconds < 100) {
        milliseconds = "0" + milliseconds;
    }
    let time = day + "." + month + "." + year + " " + hours + ":" + minutes + ":" + seconds + "." + milliseconds;

    return {
        fin: "WVWIAmVeryRandom",
        zeit: time,
        geschwindigkeit: Math.floor(Math.random() * 200),
        ort: 3,
    };
}
