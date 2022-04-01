const dotenv = require("dotenv");
dotenv.config();

// set up postgres database
const { Client } = require("pg");

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

async function transform() {
    let data = await extract();
    // console.log(data);
    // welche Daten brauchen wir von den ausgelesenen?
    // f_messung: d_fahrzeug_id, d_ort_id, d_kunde_id, messung_erzeugt, messung_eingetroffen, geschwindigkeit

    // Daten in neue Struktur für Mart bringen
    let martKundeStruktur = {
        kunde_id: '',
        vorname: '',
        nachname: '',
        anrede: '',
        geschlecht: '',
        geburtsdatum: '',
        ort: '',
        land: ''
    };
    let martOrtStruktur = {
        ort: '',
        land: ''
    }

    let martFahrzeugStruktur = {
        fin: '',
        baujahr: '',
        modell: '',
        kfz_kennzeichen: '',
        hersteller: ''
    }

    let martMessungStruktur = {
        fahrzeug_id: '',
        ort: '',
        kunde: '',
        messung_erzeugt: '',
        messung_eingetroffen: '',
        geschwindigkeit: ''
    }

    let kundenArray = [];
    let ortArray = [];
    let fahrzeugArray = [];
    let messungArray = [];

    // d_kunde: kunde_id, vorname, nachname, anrede, geschlecht, geburtsdatum, ort, land
    data.kunde.forEach(res => {
        let tempKunde = martKundeStruktur;

        tempKunde.kunde_id = res.kunde_id;
        tempKunde.vorname = res.vorname;
        tempKunde.nachname = res.nachname;
        tempKunde.anrede = res.anrede;
        tempKunde.geschlecht = res.geschlecht;
        tempKunde.geburtsdatum = res.geburtsdatum;

        wohnort = res.wohnort;

        // get name of wohnort_id
        let tempOrt = data.ort.find(element => element.ort_id == wohnort);
        tempKunde.ort = tempOrt.ort;

        let land = tempOrt.land_id;
        // get name of land_id
        let tempLand = data.land.find(element => element.land_id == land);
        tempKunde.land = tempLand.land;

        // push to array
        kundenArray.push(tempKunde);
    });

    // d_ort: ort, land
    data.ort.forEach(res => {
        let tempOrt = martOrtStruktur;
        tempOrt.ort = res.ort;

        // get land from land_id
        let tempLand = data.land.find(element => element.land_id == res.land_id);

        if (tempLand != undefined) {
            tempOrt.land = tempLand.land;

            // push to array
            ortArray.push(tempOrt);
        } else {
            console.error('land_id is not correctly defined: ', res.ort, res.land_id);
        }
    });

    // d_fahrzeug: fin, baujahr, modell, kfz_kennzeichen, hersteller
    data.fahrzeug.forEach(res => {
        let tempFahrzeug = martFahrzeugStruktur;

        tempFahrzeug.fin = res.fin;
        tempFahrzeug.baujahr = res.baujahr;
        tempFahrzeug.modell = res.modell;

        // get hersteller from hersteller_code
        let tempHersteller = data.hersteller.find(element => element.hersteller_code == res.hersteller_code);
        tempFahrzeug.hersteller = tempHersteller.hersteller;

        // get kennzeichen from kfzzuordnung
        let tempKennzeichen = data.kfzzuordnung.find(element => element.fin == res.fin);
        tempFahrzeug.kfz_kennzeichen = tempKennzeichen.kfz_kennzeichen;

        // push to array
        fahrzeugArray.push(tempFahrzeug);
    });

    console.log(kundenArray, ortArray, fahrzeugArray);
    return {
        kundenArray,
        ortArray,
        fahrzeugArray
    }
}
async function load() {
    let martData = await transform();
    // Neue Daten in Mart laden

}
load();