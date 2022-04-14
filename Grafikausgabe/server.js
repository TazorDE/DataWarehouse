const express = require('express');
const app = express();
const moment = require("moment");
const port = 3000;

const { Client } = require("pg");

const dotenv = require("dotenv");
dotenv.config();

let dbUser = process.env.DB_USER;
let dbPassword = process.env.DB_PASSWORD;
const conString = `postgres://${dbUser}:${dbPassword}@localhost:5432/postgres`;

// set ejs as view engine
app.set('view engine', 'ejs');

app.get('/', async (req, res) => {
    let renderingData = await getData();
    res.render('index', { renderingData: renderingData });
})

async function getData() {
    // connect to postgres
    const pgClient = new Client(conString);
    await pgClient.connect();

    // get data from postgres
    let result_f_fzg_messung = await pgClient.query('SELECT * FROM mart.d_kunde');
    //console.log(result_f_fzg_messung.rows);
    
    //'Morning', 'Afternoon', 'Evening'
    let z = [[null, null, null, null, null], [null, null, null, null, null], [null, null, null, null, null]];

    result_f_fzg_messung.rows.forEach((row) => {
        // classify time
        const date = moment.unix(row.messung_erzeugt);
        const weekday = date.day(); // get weekday as number, sunday starts at 0
        const hours = date.hour();

        if(0 < weekday < 6) {
            if(hours < 12){
                // morning
                let time = z[0];
                time[(weekday-1)] += row.geschwindigkeit;
                time[(weekday-1)] = Math.round(time[(weekday-1)] / 2);
            } else if (hours < 18) {
                // afternoon
                let time = z[1];
                time[(weekday-1)] += row.geschwindigkeit;
                time[(weekday-1)] = Math.round(time[(weekday-1)] / 2);
            } else {
                // evening
                let time = z[2];
                time[(weekday-1)] += row.geschwindigkeit;
                time[(weekday-1)] = Math.round(time[(weekday-1)] / 2);
            }
        }        
    });
    console.log(z);
    // prepare data for rendering
    let renderData = {
        z: z,
        x: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
        y: ['Morning', 'Afternoon', 'Evening'],
        type: 'heatmap',
        hoverongaps: false
    }
    // disconnect from postgres
    await pgClient.end();

    // return data
    return renderData;
}

// start server
app.listen(port, () => console.log(`Example app listening on http://localhost:${port}`));