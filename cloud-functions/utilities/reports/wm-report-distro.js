const excel = require("exceljs");
const cli = require('yargs');
const chalk = require('chalk');
const path = require('path');
const mysql = require('mysql2/promise');
const csv = require('async-csv');
const PDFDocument = require('pdfkit');
const fs = require('fs');


const { Storage } = require('@google-cloud/storage');
const { Datastore } = require("@google-cloud/datastore");

const options = cli
    .usage("Usage: node wm-list [ -b <bucket>] [ -f <folder>] -x <xlsx file> -s <sheet> [ -p <pause>]")
    .option("b", { alias: "bucket", describe: "google storage bucket name", type: "string", default: "oncampusmarketing", demandOption: true })
    .option("f", { alias: "folder", describe: "google storage bucket folder", type: "string", default: "dd-1920/", demandOption: true })
    .option("x", { alias: "xlsx", describe: "path to instruction xlsx", type: "string", demandOption: true })
    .option("s", { alias: "sheet", describe: "sheet name", type: "string", demandOption: true })
    .option("o", { alias: "outpath", describe: "output path", type: "string", demandOption: true })
    .option("p", { alias: "pause", describe: "pause ", type: "number", default: 60, demandOption: false })
    .option("e", { alias: "env", describe: "environment", type: "string", default: "dev", demandOption: true, choices: ["dev", "prod"] })
    .help()
    .argv;

var env = options.env;

const programs = {
    "frames": "DD",
}

const storage = new Storage();
const datastore = new Datastore();

var ProgressBar = require('progress');
console.log(`Running Excel file ${chalk.blue(options.xlsx)} sheet ${chalk.yellow(options.sheet)}`);

var ListColumns = [
    { key: "SALUTATION SLUG" },
    { key: "FIRST NAME" },
    { key: "LAST NAME" },
    { key: "STREET ADDRESS 1" },
    { key: "STREET ADDRESS 2" },
    { key: "CITY" },
    { key: "STATE" },
    { key: "ZIPCODE" },
    { key: "SCHOOL CODE" },
    { key: "PROGRAM" },
    { key: "MAILER TYPE" },
    { key: "FILE OUTPUT DATE" },
    { key: "GENDER" },
    { key: "DISTRIBUTION" },
];
ListColumns.forEach(function (item, index) {
    item.header = item.key;
});

var ReportColumns = [
    { key: "Name", width: 30 },
    { key: "Value", width: 15 },
];

var TargetFilter = {
    "allgrads": " and title <= '2019'",
    "allsen": " and title = '2019'",
    "gradboth": " and title <= '2019'",
    "gradgrad": " and title <= '2019'",
    "gradsen": " and title = '2019'",
};


async function main() {
    // first get a list of all files in the bucket
    const storageOptions = { prefix: options.folder, delimiter: "/", };

    var segmentdb = env === "dev" ? "segment_dev" : "segment";
    var cloudsql = await mysql.createConnection({
        host: '35.222.83.59',
        user: 'jyang',
        password: 'jyang',
        database: segmentdb
    });

    // const [sbfiles] = await storage.bucket(options.bucket).getFiles(storageOptions);

    // var files = [];
    // sbfiles.forEach(function (item, index){
    //     var name = path.basename(item.name);
    //     var dash = name.indexOf("-");
    //     if (dash > 0) {
    //         var school = name.substr(dash + 1, 3).toUpperCase();
    //         if (!files[school]) {
    //             files[school] = [];
    //         }
    //         files[school].push(name);
    //     }
    // });

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
                var schoolcode = cellValue(worksheet.getCell(r + 1, columns["School Code"]).value);
                var program = cellValue(worksheet.getCell(r + 1, columns["Program"]).value);
                var year = cellValue(worksheet.getCell(r + 1, columns["Year"]).value);
                var targetgroup = cellValue(worksheet.getCell(r + 1, columns["Anticipated Target Group"]).value);
                var salutation = cellValue(worksheet.getCell(r + 1, columns["Salutation"]).value);
                var distribution = cellValue(worksheet.getCell(r + 1, columns["Distribution"]).value);
                var schoolname = cellValue(worksheet.getCell(r + 1, columns["School"]).value);
                var dropdate = cellValue(worksheet.getCell(r + 1, columns["Ant Drop Dte (Yellow = Date Moved Earlier)"]).value);
                var adcode = cellValue(worksheet.getCell(r + 1, columns["Adcode"]).value);
                var output = cellValue(worksheet.getCell(r + 1, columns["Output File Name"]).value);
                var printsize = cellValue(worksheet.getCell(r + 1, columns["Print Size"]).value);

                if (schoolcode) {
                    schools.push({
                        schoolcode: schoolcode,
                        program: program,
                        year: year,
                        targetgroup: targetgroup,
                        salutation: salutation,
                        distribution: distribution,
                        schoolname: schoolname,
                        dropdate: dropdate,
                        adcode: adcode,
                        output: output,
                        printsize: printsize,
                    });
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

            if (!owners[school.schoolcode.toUpperCase()]) {
                errors.push(`${chalk.redBright(school.schoolcode.toUpperCase())}: no owner found in wemade customer DS for this school code`);
                continue;
            }
            var titleFilter = "";
            if (TargetFilter[school.targetgroup.toLowerCase()]) {
                titleFilter = TargetFilter[school.targetgroup.toLowerCase()];
            }
            titleFilter = "";
            var tablename = `seg_people_${owners[school.schoolcode.toUpperCase()].Owner}`;
            try {
                var query = "SELECT firstName, lastName, address1, address2, city, state, zip, gender  FROM `" + tablename + "` WHERE role != 'Parent'"; // WHERE role = 'student'" + titleFilter;
                const [results, fields] = await cloudsql.query(query);

                if (results && results.length) {

                    var file = new excel.Workbook();
                    var sheetList = file.addWorksheet("DM_LIST", { views: [{ ySplit: 1 }] });
                    var sheetReport = file.addWorksheet("DM_LIST_REPORT", { properties: { tabColor: { argb: 'FFC0000' } } });
                    var nameAlpha = "", nameOmega = "";

                    // populate first sheet
                    sheetList.columns = ListColumns;
                    var outputDate = new Date().toLocaleDateString('en-US');
                    var csvs = [];
                    // add row 2
                    var row2 = {};
                    ListColumns.forEach(function (item, index) {
                        row2[item.key] = school.schoolname;
                    });
                    csvs.push(row2);
                    sheetList.addRow(row2);
                    results.forEach(function (row, index) {
                        var row = {
                            "SALUTATION SLUG": school.salutation,
                            "FIRST NAME": row["firstName"],
                            "LAST NAME": row["lastName"],
                            "STREET ADDRESS 1": row["address1"],
                            "STREET ADDRESS 2": row["address2"],
                            "CITY": row["city"],
                            "STATE": row["state"],
                            "ZIPCODE": row["zip"],
                            "SCHOOL CODE": school.schoolcode,
                            "PROGRAM": programs[school.program.toLowerCase()],
                            "MAILER TYPE": "",
                            "FILE OUTPUT DATE": outputDate,
                            "GENDER": row["gender"],
                            "DISTRIBUTION": school.distribution
                        }
                        sheetList.addRow(row);
                        csvs.push(row);
                    });
                    sheetList.getRow(1).font = { bold: true };

                    nameAlpha = results[0]["firstName"] + " " + results[0]["lastName"];
                    nameOmega = results[results.length - 1]["firstName"] + " " + results[results.length - 1]["lastName"];
                    sheetReport.columns = ReportColumns;
                    var report = [
                        ["OCM Mailing List Form", ""],
                        ["School Name", school.schoolname],
                        ["School Code", school.schoolcode],
                        ["Program", programs[school.program.toLowerCase()]],
                        ["Adcode:", school.adcode],
                        ["Best Drop Date", school.dropdate ? school.dropdate.toLocaleDateString() : ""],
                        ["Distribution", school.distribution],
                        ["File Name", school.output],
                        ["", ""],
                        ["List Type", "S"],
                        ["First Name on List", nameAlpha],
                        ["Last Name on List", nameOmega],
                        ["Quantity Output", results.length],
                        ["Print Size", school.printsize],
                        ["Importe"],
                        ["Imported Date"],
                        ["Used"],
                        ["Used Date"],
                        ["SJ1 Label Amount"],
                        ["SJ1 Last Record Name/Number"],
                        ["", ""],
                        ["SJ2 Label Amount"],
                        ["SJ2 Last Record Name/Number"],
                        ["", ""],
                        ["SJ3 Label Amount"],
                        ["SJ3 Last Record Name/Number"]
                    ];
                    sheetReport.addRows(report);

                    var xlsxFilename = path.join(options.outpath, ensureSuffix(school.output, ".xlsx"));
                    var csvFilename = path.join(options.outpath, ensureSuffix(school.output, ".txt"));
                    var pdfFilename = path.join(options.outpath, ensureSuffix(school.output, ".pdf"));
                    await file.xlsx.writeFile(xlsxFilename);

                    var csvData = await convertToTabDelimited(csvs);
                    fs.writeFileSync(csvFilename, csvData);

                    const pdf = new PDFDocument();
                    pdf.pipe(fs.createWriteStream(pdfFilename)); 
                    report.forEach(function(item, index) {
                        if (item[0]) {
                            pdf.text(`${item[0]}: ${item[1] ? item[1] : ""}`);
                        }
                        else {
                            pdf.text(" ");
                        }
                    });
                    pdf.end();

                }
                else {
                    errors.push(`${chalk.redBright(school)}: no recordsd returned from segment query`);
                }
            }
            catch (error) {
                errors.push(`${chalk.redBright(school.schoolcode)}: error occurred - ${error}`);
            }

        }
        if (errors.length > 0) {
            errors.forEach(function (t, i) {
                console.log(t);
            });
        }
    };

    process.exit();
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
    .catch(console.error);


function msleep(n) {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, n);
}

function sleep(n) {
    msleep(n * 1000);
}

function cellValue(v) {
    if (v != null && v.result) {
        return v.result;
    }
    return v;
}

function ensureSuffix(a, b) {
    if (!a.endsWith(b)) {
        return a + b;
    }
    return a;
}

async function convertToTabDelimited(data, header = true, allColumns = false) {
    if (data.length === 0) {
      return '';
    }
  
    const columnNames =
      allColumns
        ? [...data
          .reduce((columns, row) => { // check each object to compile a full list of column names
            Object.keys(row).map(rowKey => columns.add(rowKey));
            return columns;
          }, new Set())]
        : Object.keys(data[0]); // just figure out columns from the first item in array
  
    if (allColumns) {
      columnNames.sort(); // for predictable order of columns
    }
  
    // This will hold data in the format that `async-csv` can accept, i.e.
    // an array of arrays.
    let csvInput = [];
    if (header) {
      csvInput.push(columnNames);
    }
  
    // Add all other rows:
    csvInput.push(
      ...data.map(row => columnNames.map(column => row[column])),
    );
  
    return await csv.stringify(csvInput, {delimiter: "\t"} );
  }