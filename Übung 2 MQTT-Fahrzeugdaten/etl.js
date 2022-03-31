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

// Tables in staging: messung, land, ort, kfzzuordnung, kunde, fahrzeug, hersteller
// Tables in mart: d_kunde, f_fzg_messung, d_fahrzeug, d_ort

//extract data from staging
async function extractData() {
    //extract data for kunde: kunde_id, vorname, nachname, anrede, geschlecht, geburtsdatum, wohnort
    let kundeStatement = "SELECT kunde_id, vorname, nachname, anrede, geschlecht, geburtsdatum, wohnort FROM staging.kunde";
    
    let kunde = {
        kunde_id: "",
        vorname: "",
        nachname: "",
        anrede: "",
        geschlecht: "",
        geburtsdatum: "",
        wohnort: ""
    };

    const kundenArray = [];
    
    await pgClient.query(kundeStatement, function (err, result) {
        if (err) {
            console.log(err);
        } else {
            console.info(result);
            let temp = kunde;
            temp.kunde_id = result.rows[0].kunde_id;
            temp.vorname = result.rows[0].vorname;
            temp.nachname = result.rows[0].nachname;
            temp.anrede = result.rows[0].anrede;
            temp.geschlecht = result.rows[0].geschlecht;
            temp.geburtsdatum = result.rows[0].geburtsdatum;
            temp.wohnort = result.rows[0].wohnort;
            kundenArray.push(temp);
        }
    });

    console.log(kundenArray);
}

extractData();

//transform data
function transformData() {

}

//load data in mart
function loadData() {

}