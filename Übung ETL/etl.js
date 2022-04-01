const dotenv = require("dotenv");
dotenv.config();

const moment = require("moment");

// set up postgres database
const { Client, Pool } = require("pg");
const pg = require("pg");

if (!String.prototype.trim) {
    String.prototype.trim = function () {
        return this.replace(/^\s+|\s+$/g, "");
    };
}

// connection string with environment variables
let dbUser = process.env.DB_USER;
let dbPassword = process.env.DB_PASSWORD;
const conString = `postgres://${dbUser}:${dbPassword}@localhost:5432/postgres`;

// Tables in staging: messung, land, ort, kfzzuordnung, kunde, fahrzeug, hersteller
// Tables in mart: d_kunde, f_fzg_messung, d_fahrzeug, d_ort

// extract data from staging
async function extract() {
    const pgClient = new Client(conString);
    await pgClient.connect();

    // extract data from messung
    let messung = await pgClient.query(`
        SELECT
            messung_id,
            payload,
            erstellt_am
        FROM
            staging.messung
        WHERE
            payload IS NOT NULL
    `);

    // extract data from land
    let land = await pgClient.query(`
        SELECT
            land_id,
            land
        FROM
            staging.land
    `);

    // extract data from ort
    let ort = await pgClient.query(`
        SELECT
            ort_id,
            ort,
            land_id
        FROM
            staging.ort
    `);

    // extract data from kfzzuordnung
    let kfzzuordnung = await pgClient.query(`
        SELECT
            fin,
            kfz_kennzeichen
        FROM
            staging.kfzzuordnung
    `);

    // extract data from kunde
    let kunde = await pgClient.query(`
        SELECT
            kunde_id,
            vorname,
            nachname,
            anrede,
            geschlecht,
            geburtsdatum,
            wohnort
        FROM
            staging.kunde
    `);

    // extract data from fahrzeug
    let fahrzeug = await pgClient.query(`
        SELECT
            fin,
            hersteller_code,
            kunde_id,
            baujahr,
            modell
        FROM
            staging.fahrzeug
    `);

    // extract data from hersteller
    let hersteller = await pgClient.query(`
        SELECT
            hersteller_code,
            hersteller
        FROM
            staging.hersteller
    `);
    await pgClient.end();

    return {
        messung: messung.rows,
        land: land.rows,
        ort: ort.rows,
        kfzzuordnung: kfzzuordnung.rows,
        kunde: kunde.rows,
        fahrzeug: fahrzeug.rows,
        hersteller: hersteller.rows,
    };
}

async function transform(pgClient) {
    let data = await extract();
    // console.log(data);
    // welche Daten brauchen wir von den ausgelesenen?
    // f_messung: d_fahrzeug_id, d_ort_id, d_kunde_id, messung_erzeugt, messung_eingetroffen, geschwindigkeit

    // Daten in neue Struktur für Mart bringen
    let kundenArray = [];
    let ortArray = [];
    let fahrzeugArray = [];
    let messungArray = [];

    let returnData = {
        kunde: kundenArray,
        ort: ortArray,
        fahrzeug: fahrzeugArray,
        messung: messungArray,
    };

    // d_kunde: d_kunde_id, kunde_id, vorname, nachname, anrede, geschlecht, geburtsdatum, ort, land
    data.kunde.forEach((res) => {
        wohnort = res.wohnort;
        // get name of wohnort_id
        let tempOrt = data.ort.find((element) => element.ort_id == wohnort);

        let land = tempOrt.land_id;
        // get name of land_id
        let tempLand = data.land.find((element) => element.land_id == land);

        // add tempkunde to returnData.kunde
        returnData.kunde.push({
            kunde_id: res.kunde_id,
            vorname: res.vorname,
            nachname: res.nachname,
            anrede: res.anrede,
            geschlecht: res.geschlecht,
            geburtsdatum: res.geburtsdatum,
            wohnort_id: tempOrt.ort_id,
            ort: tempOrt.ort,
            land: tempLand.land,
        });
    });

    // d_ort: d_ort_id, ort, land
    data.ort.forEach((res) => {
        // get land from land_id
        let tempLand = data.land.find(
            (element) => element.land_id == res.land_id
        );

        if (tempLand != undefined) {
            // push to array
            returnData.ort.push({
                ort: res.ort,
                land: tempLand.land,
            });
        } else {
            console.error(
                "land_id is not correctly defined: ",
                res.ort,
                res.land_id
            );
        }
    });

    // d_fahrzeug: d_fahrzeug_id, fin, baujahr, modell, kfz_kennzeichen, hersteller
    data.fahrzeug.forEach((res) => {
        // trim fin
        let fin = res.fin.trim();
        res.fin = fin;

        // get hersteller from hersteller_code
        let tempHersteller = data.hersteller.find(
            (element) => element.hersteller_code == res.hersteller_code
        );

        // get kennzeichen from kfzzuordnung
        let tempKennzeichen = data.kfzzuordnung.find(
            (element) => element.fin.trim() == res.fin.trim()
        );

        // push to array
        returnData.fahrzeug.push({
            fin: fin,
            baujahr: res.baujahr,
            modell: res.modell,
            hersteller: tempHersteller.hersteller,
            kfz_kennzeichen: tempKennzeichen.kfz_kennzeichen,
        });
    });

    // f_messung: d_fahrzeug_id, d_ort_id, d_kunde_id, messung_erzeugt, messung_eingetrofen, geschwindigkeit
    data.messung.forEach((msg) => {
        let payload = msg.payload;

        // get d_fahrzeug_id from fin
        let tempFahrzeug = data.fahrzeug.find(
            (element) => element.fin == payload.fin
        );

        if (tempFahrzeug != undefined) {
            // get d_ort_id from ort_id
            let tempOrt = data.ort.find(
                (element) => element.ort_id == payload.ort
            );

            if (tempOrt != undefined) {
                // push to array
                returnData.messung.push({
                    messung_erzeugt: payload.zeit,
                    messung_eingetroffen: msg.erstellt_am,
                    geschwindigkeit: payload.geschwindigkeit,
                    fin: payload.fin,
                    kunde_id: tempFahrzeug.kunde_id,
                    ort_id: tempOrt.ort
                });
            } else {
                console.error(
                    "kunde_id is not in db: ",
                    tempFahrzeug.kunde_id
                );
            }
        } else {
            console.error("fin is not in db: ", payload.fin);
        }
    });

    return returnData;
}
async function load() {

    let client = new Client(conString);
    client.connect(err => {
        if (err) {
            console.error('connection error', err.stack)
        } else {
            console.log('connected')
        }
    });

    let martData = await transform(client);

    await client.query(`TRUNCATE TABLE mart.d_kunde`);
    await client.query(`TRUNCATE TABLE mart.d_ort`);
    await client.query(`TRUNCATE TABLE mart.d_fahrzeug`);
    await client.query(`TRUNCATE TABLE mart.f_fzg_messung`);



    martData.kunde.forEach(async (res) => {
        // insert into d_kunde
        let sql = `INSERT INTO mart.d_kunde (kunde_id, vorname, nachname, anrede, geschlecht, geburtsdatum, wohnort_id, ort, land) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`;
        let values = [
            res.kunde_id,
            res.vorname,
            res.nachname,
            res.anrede,
            res.geschlecht,
            res.geburtsdatum,
            res.wohnort_id,
            res.ort,
            res.land,
        ];
        let kunderes = await client.query(sql, values);
        // console.log('kunde', kunderes);
    });

    martData.ort.forEach(async (res) => {
        // insert into d_ort
        let sql = `INSERT INTO mart.d_ort (ort, land) VALUES ($1, $2)`;
        let values = [
            res.ort,
            res.land
        ];
        let ortres = await client.query(sql, values);
        // console.log('ort', ortres);
    });

    martData.fahrzeug.forEach(async (res) => {
        // insert into d_fahrzeug
        let sql = `INSERT INTO mart.d_fahrzeug (fin, baujahr, modell, kfz_kennzeichen, hersteller) VALUES ($1, $2, $3, $4, $5)`;
        let values = [
            res.fin,
            res.baujahr,
            res.modell,
            res.kfz_kennzeichen,
            res.hersteller,
        ];
        let fahrzeugres = await client.query(sql, values);
        // console.log('fahrzeug', fahrzeugres);
    });

    // get curr data
    let sql = `SELECT * FROM mart.d_kunde`;
    let kundeRes = await client.query(sql);
    //console.log(kundeRes.rows);

    sql = `SELECT * FROM mart.d_ort`;
    let ortRes = await client.query(sql);
    //console.log(ortRes.rows);

    sql = `SELECT * FROM mart.d_fahrzeug`;
    let fahrzeugRes = await client.query(sql);
    //console.log(fahrzeugRes.rows);

    martData.messung.forEach(async (res) => {

        /**
         {
            messung_erzeugt: '31.03.2022 11:05:20.488',
            messung_eingetroffen: 2022-03-31T09:05:20.483Z,
            geschwindigkeit: 130,
            fin: 'WVWIAmVeryRandom',
            kunde_id: 532985,
            ort_id: 3
          }
         */

        // get d_fahrzeug_id from fin
        let tempFahrzeug = fahrzeugRes.rows.find(
            (element) => element.fin.trim() == res.fin.trim()
        );

        // get d_ort_id from ort_id
        let tempOrt = ortRes.rows.find(
            (element) => element.ort == res.ort_id
        );

        // get d_kunde_id from kunde_id
        let tempKunde = kundeRes.rows.find(
            (element) => element.kunde_id == res.kunde_id
        );


        sql = `INSERT INTO mart.f_fzg_messung (d_fahrzeug_id, d_ort_id, d_kunde_id, messung_erzeugt, empfang_eingetroffen, geschwindigkeit) VALUES ($1, $2, $3, $4, $5, $6)`;
        values = [
            Number.parseInt(tempFahrzeug.d_fahrzeug_id),
            Number.parseInt(tempOrt.d_ort_id),
            Number.parseInt(tempKunde.d_kunde_id),
            Date.parse(moment(res.messung_erzeugt, 'DD.MM.YYYY HH:mm:ss.SSS').toLocaleString()),
            Date.parse(moment(res.messung_eingetroffen, 'DD.MM.YYYY HH:mm:ss.SSS').toLocaleString()),
            Number.parseInt(res.geschwindigkeit),
        ];

        if (values[3].isNan() || values[4].isNan()) {
            console.error('invalid date: ', res.messung_eingetroffen);
        } else {
            console.log('inserting: ', values);
            //console.log('now', await client.query('SELECT timeofday()'));
            try {
                await client.query(sql, values);
            } catch (error) {
                console.error('error: ', error);
            }
        }
    });
}

load().then(() => {
    process.exit(1);
});