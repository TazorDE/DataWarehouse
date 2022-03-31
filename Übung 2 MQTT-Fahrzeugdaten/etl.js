const dotenv = require("dotenv");
dotenv.config();

// set up postgres database
const {Client} = require("pg");

// connection string with environment variables
let dbUser = process.env.DB_USER;
let dbPassword = process.env.DB_PASSWORD;
const conString = `postgres://${dbUser}:${dbPassword}@localhost:5432/postgres`;

// Tables in staging: messung, land, ort, kfzzuordnung, kunde, fahrzeug, hersteller
// Tables in mart: d_kunde, f_fzg_messung, d_fahrzeug, d_ort

// extract data from staging
async function extract() {

    let pgClient = new Client(conString);
    pgClient.connect();

    // extract data from messung
    let messung = await pgClient.query(`
        SELECT
            messung_id,
            payload
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
        hersteller: hersteller.rows
    };
}

async function transform(){
    let data = await extract();
    console.log(data);

}
transform();

function load(){

}