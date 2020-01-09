
const xlsx = require('exceljs');
const fs = require('fs');
const path = require('path');
const {Storage} = require('@google-cloud/storage');
const config = require( "./config.json" );

const storage = new Storage();
const storageBucket = config.gcp.bucket;
const storageFilter = config.gcp.filter;
const storageRecursive = config.gcp.recursive;

const xlsxPath = config.local.xlsx.fileName;
const xlsxSheet = config.local.xlsx.sheetName;
const xlsxAppend = config.local.xlsx.append;
const bucketFilter = config.gcp.bucket.filter;

async function existsFile() {
    try {
        fs.accessSync(xlsxPath, fs.constants.R_OK);
        return new Promise((resolve, reject) => {
            return resolve("file exists");
        });
 
    }
    catch (err) {
        var wb = new xlsx.Workbook();
        var ws = wb.addWorksheet(xlsxSheet);
        ws.getCell("A1").value = "Path";
        ws.getCell("B1").value = "File";

        await wb.xlsx.writeFile(xlsxPath);
        return new Promise((resolve, reject) => {
            return resolve("created");
        });

    }
}

async function existsSheet() {
    var workbook = new xlsx.Workbook();
    workbook.xlsx.readFile(xlsxPath)
    .then(async function() {
        var worksheet = workbook.getWorksheet(xlsxSheet);
        if (worksheet) {
            if (worksheet.getCell("A1").value == "Path" && worksheet.getCell("B1").value == "File") {
                return new Promise((resolve, reject) => {
                    return resolve("sheet exists");
                });
            }
            else {
                return new Promise((resolve, reject) => {
                    return reject("excel sheet does not contain Path and File in column A and B");
                });
            }
        }
        else {
            var worksheet = workbook.addWorksheet(xlsxSheet);
            worksheet.getCell("A1").value = "Path";
            worksheet.getCell("B1").value = "File";            
            await workbook.xlsx.writeFile(xlsxPath);
            return new Promise((resolve, reject) => {
                return resolve("sheet created");
            });
            
        }
    });
}

async function main () {
    const storageOptions = {
        prefix: storageFilter,
    };
    
    if (!storageRecursive) {
        storageOptions.delimiter = "/";
    }

    console.log(await existsFile());
    console.log(await existsSheet());
    var workbook = new xlsx.Workbook();
    workbook.xlsx.readFile(xlsxPath)
    .then(async function() {
        var pos = 0;
        var scode = "";
        var worksheet = workbook.getWorksheet(xlsxSheet);
        const [files] = await storage.bucket(storageBucket).getFiles(storageOptions);
        var lastRow = worksheet.rowCount
        
        worksheet.getCell("A1").value = "Path";
        worksheet.getCell("B1").value = "File";
        worksheet.getCell("C1").value = "Enabled";
        worksheet.getCell("D1").value = "Success";
        worksheet.getCell("E1").value = "School";
        worksheet.getCell("F1").value = "Owner";
        worksheet.getCell("G1").value = "AccessKey";
        worksheet.getCell("H1").value = "RequestId";
        worksheet.getCell("I1").value = "fileUrl";
        worksheet.getCell("J1").value = "Sequence";
        worksheet.getCell("K1").value = "Class";
        
        for (var i = 1; i < files.length; i++) {
            var p = path.dirname(files[i].name)
            var f = path.basename(files[i].name)
            if (p != ".") {
                pos = f.indexOf('-');
                scode = f.slice(pos + 1, pos + 5);
                scode = scode.replace(/[0-9]/g, "");
                worksheet.getCell("A" + (lastRow + i)).value = p;
                worksheet.getCell("B" + (lastRow + i)).value = f;
                worksheet.getCell("C" + (lastRow + i)).value = "TRUE";
                worksheet.getCell("E" + (lastRow + i)).value = scode;
                worksheet.getCell("J" + (lastRow + i)).value = 1;
                worksheet.getCell("K" + (lastRow + i)).value = "allfresh";
            }
        }

        await workbook.xlsx.writeFile(xlsxPath).then(function() {
            return new Promise((resolve, reject) => {
                return resolve("sheet created");
            });
        });
    });    
}
main()
    //.then(console.log)
    .catch(console.error)

    