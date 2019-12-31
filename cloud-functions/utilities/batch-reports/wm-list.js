const fs = require('fs');
const excel = require("exceljs");
const cli = require('yargs');
const chalk = require('chalk');
const path = require('path');
const env = "dev";

const {Storage} = require('@google-cloud/storage');
const { Datastore } = require("@google-cloud/datastore");

const options = cli
 .usage("Usage: node wm-list [ -b <bucket>] [ -f <folder>] -x <xlsx file> -s <sheet> [ -p <pause>]")
 .option("b", { alias: "bucket", describe: "google storage bucket name", type: "string", default: "oncampusmarketing", demandOption: true })
 .option("f", { alias: "folder", describe: "google storage bucket folder", type: "string", default: "dd-1920/", demandOption: true })
 .option("x", { alias: "xlsx", describe: "path to instruction xlsx", type: "string", demandOption: true })
 .option("s", { alias: "sheet", describe: "sheet name", type: "string", demandOption: true })
 .option("o", { alias: "outpath", describe: "output path", type: "string", demandOption: true })
 .option("p", { alias: "pause", describe: "pause ", type: "number", default: 60, demandOption: false })
 .help()
 .argv;

const storage = new Storage();
const datastore = new Datastore();

var ProgressBar = require('progress');
console.log(`Running Excel file ${chalk.blue(options.xlsx)} sheet ${chalk.yellow(options.sheet)}`);

async function main () {
    // first get a list of all files in the bucket
    const storageOptions = {prefix: options.folder, delimiter: "/", };
    const [sbfiles] = await storage.bucket(options.bucket).getFiles(storageOptions);

    var files = [];
    sbfiles.forEach(function (item, index){
        var name = path.basename(item.name);
        var dash = name.indexOf("-");
        if (dash > 0) {
            var school = name.substr(dash + 1, 3).toUpperCase();
            if (!files[school]) {
                files[school] = [];
            }
            files[school].push(name);
        }
    });

    // next let's read the sheet
    var workbook = null;
    var worksheet = null;
    try {
        workbook = await new excel.Workbook().xlsx.readFile(options.xlsx);
    }
    catch (err) {
        console.log(`Exception opening workbook: ${chalk.red.bold(err)}`);
    }
    if (workbook != null) {
        try {
            worksheet = workbook.getWorksheet(options.sheet);
        }
        catch (err) {
            console.log(`Exceptoin opening worksheet: ${chalk.red.bold(err)}`);
        }
    }
    if (worksheet != null) {
        var columns = [];
        for (var c = 0; c < worksheet.columnCount; c++) {
            var column = worksheet.getColumn(c + 1);
            if (column.values[1]) {
                columns[column.values[1]] = c + 1;
            }
        }
        var schools = [];
        for (var r = 1; r < worksheet.rowCount; r++) {
            var row = worksheet.getRow(r + 1);
            if (!row.hidden) {
                var schoolcode = worksheet.getCell(r +1, columns["School Code"]).value;
                if (schoolcode) {
                    schools.push(schoolcode);
                }
            }
        }

        var bar = new ProgressBar('Processing [:bar] :current/:total :percent', {
            complete: '=',
            head: ".",
            incomplete: ' ',
            width: 60,
            total: schools.length
        });

        const owners = await getOwners();     
        var errors = [];
        for (var s in schools) {
            var school = schools[s];
            bar.tick(1);
            if (!files[school]) {
                errors.push(`${chalk.redBright(school)}: no matching files in bucket`);
            }
            else if (!owners[school]) {
                errors.push(`${chalk.redBright(school)}: no matching owner found in datastore`);
            }
            else {
                var filter = worksheet.getCell(1, columns["Anticipated Target Group"]).value.toUpperCase();
                for (var f in files[school]) {
                    var file = files[school][f];
                    if (file.toUpperCase().indexOf(filter) > -1) {
                        const storageOptions = {
                            version: "v2",
                            action: "read",
                            expires: Date.now() + 1000 * 60 * 60 // one hour
                        };
                        
                        const [signedUrl] = await storage.bucket(options.bucket).file(`${options.folder}/${file}`).getSignedUrl(storageOptions);
                    }
                };
                sleep(2);
            }
        };
        errors.forEach(function (t, i) {
            console.log(t);
        });
    }
}

async function getOwners() {
    datastore.namespace = `wemade-${env}`;
    const query = datastore.createQuery('Customer');
    const [customers] = await datastore.runQuery(query);
    var owners = [];
    customers.forEach(function (customer, index) {
        var owner = customer.Owner;
        var school = owner.substr(0, 3).toUpperCase();
        if (!owners[school]) {
            owners[school] = {
                Owner: customer.Owner,
                AccessKey: customer.AccessKey,
            };
        }
    });
    return owners;
}

main()
    //.then(console.log)
    .catch(console.error)


function msleep(n) {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, n);
}

function sleep(n) {
    msleep(n*1000);
}