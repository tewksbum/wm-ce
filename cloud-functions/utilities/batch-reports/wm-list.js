const fs = require('fs');
const excel = require("exceljs");
const cli = require('yargs');
const chalk = require('chalk');
const path = require('path');
const {Storage} = require('@google-cloud/storage');
const options = cli
 .usage("Usage: node wm-list [ -b <bucket>] [ -f <folder>] -x <xlsx file> -s <sheet> [ -p <pause>]")
 .option("b", { alias: "bucket", describe: "google storage bucket name", type: "string", default: "oncampusmarketing", demandOption: true })
 .option("f", { alias: "folder", describe: "google storage bucket folder", type: "string", default: "dd-1920/", demandOption: true })
 .option("x", { alias: "xlsx", describe: "path to instruction xlsx", type: "string", demandOption: true })
 .option("s", { alias: "sheet", describe: "sheet name", type: "string", demandOption: true })
 .option("p", { alias: "pause", describe: "pause ", type: "number", default: 60, demandOption: false })
 .help()
 .argv;

const storage = new Storage();
console.log(options);
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
            var school = name.substr(dash + 1, 3)
            if (!files[school]) {
                files[school] = [];
            }
            files[school].push(name);
        }
    })

    // next let's read the sheet
    var workbook = null;
    var worksheet = null;
    try {
        workbook = await new excel.Workbook().xlsx.readFile(options.xlsx);
    }
    catch (err) {
        console.log(chalk.red.bold(err));
    }
    if (workbook != null) {

    }
    // console.log(files);
}

main()
    //.then(console.log)
    .catch(console.error)