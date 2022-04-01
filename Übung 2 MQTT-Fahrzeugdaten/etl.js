const dotenv = require("dotenv");
dotenv.config();

// set up postgres database
const { Client } = require("pg");

// connection string with environment variables
let dbUser = process.env.DB_USER;
let dbPassword = process.env.DB_PASSWORD;
const conString = `postgres://${dbUser}:${dbPassword}@localhost:5432/postgres`;

let d_kunde_id_start;
let d_ort_id_start;
let d_fahrzeug_id_start;

// Tables in staging: messung, land, ort, kfzzuordnung, kunde, fahrzeug, hersteller
// Tables in mart: d_kunde, f_fzg_messung, d_fahrzeug, d_ort

async function generateStartValues() {
    // create d/f_id start values
    let d_kunde_id_start = 0;
    let d_ort_id_start = 0;
    let d_fahrzeug_id_start = 0;

    // check entries for existing ids and load the highest id+1 as a new start value
    let client = new Client(conString);
    client.connect();

    await client.end();
    return {
        d_kunde_id_start,
        d_ort_id_start,
        d_fahrzeug_id_start,
    };
}

// extract data from staging
async function extract() {
    let pgClient = new Client(conString);
    pgClient.connect();

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

async function transform() {
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
            d_kunde_id: d_kunde_id_start,
            kunde_id: res.kunde_id,
            vorname: res.vorname,
            nachname: res.nachname,
            anrede: res.anrede,
            geschlecht: res.geschlecht,
            geburtsdatum: res.geburtsdatum,
            ort: tempOrt.ort,
            land: tempLand.land,
        });
        d_kunde_id_start++;
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
                d_ort_id: d_ort_id_start,
                ort: res.ort,
                land: tempLand.land,
            });
            d_ort_id_start++;
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
        // get hersteller from hersteller_code
        let tempHersteller = data.hersteller.find(
            (element) => element.hersteller_code == res.hersteller_code
        );

        // get kennzeichen from kfzzuordnung
        let tempKennzeichen = data.kfzzuordnung.find(
            (element) => element.fin == res.fin
        );

        // push to array
        returnData.fahrzeug.push({
            d_fahrzeug_id: d_fahrzeug_id_start,
            fin: res.fin,
            baujahr: res.baujahr,
            modell: res.modell,
            hersteller: tempHersteller.hersteller,
            kfz_kennzeichen: tempKennzeichen.kfz_kennzeichen,
        });
        d_fahrzeug_id_start++;
    });

    // f_messung: d_fahrzeug_id, d_ort_id, d_kunde_id, messung_erzeugt, messung_eingetrofen, geschwindigkeit
    data.messung.forEach((msg) => {
        let fin = msg.payload.fin;

        let tempFahrzeug = data.fahrzeug.find((element) => element.fin == fin);
        if (tempFahrzeug != undefined) {
            let tempDFahrzeug = returnData.fahrzeug.find(
                (element) => element.fin == fin
            );

            // get kunde from returnData.kunde where kunde_id == tempFahrzeug.kunde_id
            let tempKunde = returnData.kunde.find(
                (element) => element.kunde_id == tempFahrzeug.kunde_id
            );

            if (tempKunde != undefined) {
                let messungOrt = msg.payload.ort;
                // get ort from data.ort where ort_id == messungOrt
                let tempOrt = data.ort.find(
                    (element) => element.ort_id == messungOrt
                );

                if (tempOrt != undefined) {
                    // get d_ort_id from returnData.ort where ort == tempOrt.ort
                    let tempDOrt = returnData.ort.find(
                        (element) => element.ort == tempOrt.ort
                    );

                    // push to array
                    returnData.messung.push({
                        d_fahrzeug_id: tempDFahrzeug.d_fahrzeug_id,
                        d_ort_id: tempDOrt.d_ort_id,
                        d_kunde_id: tempKunde.d_kunde_id,
                        messung_erzeugt: new Date(msg.payload.zeit),
                        messung_eingetroffen: new Date(msg.erstellt_am),
                        geschwindigkeit: msg.payload.geschwindigkeit,
                    });

                } else {
                    console.error("ort_id is not in db: ", messungOrt);
                }
            } else {
                console.error("kunde_id is not in db: ", tempFahrzeug.kunde_id);
            }
        } else {
            console.error("fin is not in db: ", fin);
        }
    });

    return returnData;
}
async function load() {
    let martData = await transform();
    console.log(martData);
    // Neue Daten in Mart laden
}

async function startSys() {
    let startValues = await generateStartValues();
    d_kunde_id_start = startValues.d_kunde_id_start;
    d_ort_id_start = startValues.d_ort_id_start;
    d_fahrzeug_id_start = startValues.d_fahrzeug_id_start;

    load();
}
startSys();
