const dotenv = require("dotenv");
dotenv.config();

// set up postgres database
const { Client, Pool } = require("pg");

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
    /*
    data.messung.forEach((msg) => {
        let payload = msg.payload;

        // get d_fahrzeug_id from fin
        let tempFahrzeug = data.fahrzeug.find(
            (element) => element.fin == payload.fin
        );

        if (tempFahrzeug != undefined) {
            // get d_fahrzeug_id
            let tempDFahrzeug = returnData.fahrzeug.find(
                (element) => element.fin.trim() == payload.fin.trim()
            );

            // get d_ort_id from ort_id
            let tempOrt = data.ort.find(
                (element) => element.ort_id == payload.ort
            );

            if (tempOrt != undefined) {
                // get d_ort_id from tempOrt.ort
                let tempDOrt = returnData.ort.find(
                    (element) => element.ort == tempOrt.ort
                );

                // get d_kunde_id from tempFahrzeug.kunde_id
                let tempDKunde = returnData.kunde.find(
                    (element) => element.kunde_id == tempFahrzeug.kunde_id
                );

                if (tempDKunde != undefined) {
                    // push to array
                    returnData.messung.push({
                        d_fahrzeug_id: tempDFahrzeug.d_fahrzeug_id,
                        d_ort_id: tempDOrt.d_ort_id,
                        d_kunde_id: tempDKunde.d_kunde_id,
                        messung_erzeugt: payload.zeit,
                        messung_eingetroffen: msg.erstellt_am,
                        geschwindigkeit: payload.geschwindigkeit,
                    });
                } else {
                    console.error(
                        "kunde_id is not in db: ",
                        tempFahrzeug.kunde_id
                    );
                }
            } else {
                console.error("ort_id is not in db: ", payload.ort);
            }
        } else {
            console.error("fin is not in db: ", payload.fin);
        }
    });*/

    return returnData;
}
async function load() {
    const pgPool = new Pool({
        host: "localhost",
        user: dbUser,
        password: dbPassword,
        max: 20,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 2000,
    });

    let martData = await transform();

    await pgPool.connect((err, client, done) => {
        // Neue Daten in Mart laden
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
            await client.query(sql, values);
        });

        martData.ort.forEach(async (res) => {
            // insert into d_ort
            let sql = `INSERT INTO mart.d_ort (ort, land) VALUES ($1, $2)`;
            let values = [res.ort, res.land];
            await client.query(sql, values);
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
            await client.query(sql, values);
        });

        // martData.messung.forEach(async (res) => {
        //     // insert into f_messung
        //     let sql = `INSERT INTO mart.f_fzg_messung (d_fahrzeug_id, d_ort_id, d_kunde_id, messung_erzeugt, messung_eingetroffen, geschwindigkeit) VALUES ($1, $2, $3, $4, $5, $6)`;
        //     let values = [
        //         res.d_fahrzeug_id,
        //         res.d_ort_id,
        //         res.d_kunde_id,
        //         res.messung_erzeugt,
        //         res.messung_eingetroffen,
        //         res.geschwindigkeit,
        //     ];
        //     await client.query(sql, values);
        // });
        done();
    });

    await pgPool.end();
}

async function build_messung() {
    const pgPool = new Pool({
        host: "localhost",
        user: dbUser,
        password: dbPassword,
        max: 20,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 2000,
    });

    await pgPool.connect(async (err, client, done) => {

        // get all data from mart.d_kunde, mart.d_ort, mart.d_fahrzeug
        let sql = `SELECT * FROM mart.d_kunde;`;
        let currentKunden = await pgPool.query(sql);
        sql = `SELECT * FROM mart.d_ort;`;
        let currentOrte = await pgPool.query(sql);
        sql = `SELECT * FROM mart.d_fahrzeug;`;
        let currentFahrzeuge = await pgPool.query(sql);

        console.log(currentKunden.rows);
        done();
    });

    await pgPool.end();
}

async function startSys() {
    await load();
}
startSys();
