#!/usr/bin/node
const request = require("sync-request");
const fs = require("fs");
const Excel = require("exceljs");
(async () => {
  const reportURL =
    "https://us-central1-wemade-core.cloudfunctions.net/wm-dev-report-api";
  let inputFilename = "../_input/input.xlsx";
  if (process.argv.length === 2) {
    console.error(`Using default input file ${inputFilename}`);
  } else {
    inputFilename = process.argv[2];
  }
  var workBook = new Excel.Workbook();
  workBook = await workBook.xlsx.readFile(inputFilename);

  //report logging
  const today = new Date();
  const logFile = `./logs/xdetailreport-log-${today.toISOString()}.json`;
  const stream = fs.createWriteStream(logFile, { flags: "a" });
  stream.write("[\n");
  let sep = "";
  console.log(`Logging reports on ${logFile}`);
  // const rawData = fs.readFileSync(
  //   "./logs/xdetailreport-log-2019-12-20T20:33:33.655Z.json"
  // );
  // const reportStatic = JSON.parse(rawData);
  const worksheet = workBook.getWorksheet(1);
  for (let index = 2; index <= worksheet.rowCount; index++) {
    
    var workBookTarget = new Excel.Workbook();
    var DetailSheet1 = workBookTarget.addWorksheet("Records");
    var DetailSheet2 = workBookTarget.addWorksheet("Fibers");
    let currentRow = worksheet.getRow(index);
    if (!currentRow.getCell(8).value) {
      continue;
    }
    currentRow.hidden = false;
    const enabled = currentRow.values[3];
    if (!enabled) {
      console.log("row " + index + " not enabled");
      continue;
    }
    let currentUL = {
      schoolcode: currentRow.getCell(5).value,
      fileURL: currentRow.getCell(9).value,
      owner: currentRow.getCell(6).value,
      accessKey: currentRow.getCell(7).value,
      requestId: currentRow.getCell(8).value
    };
    currentRow.commit();
    if (currentUL.error != undefined) {
      console.log(`Skipping ${index}, error present`);
      continue;
    }
    const reportRequest = {
      owner: currentUL.owner,
      accessKey: currentUL.accessKey,
      reportType: "detail",
      requestId: currentUL.requestId
    };
    console.log(
      `Getting report for ${currentUL.owner} id ${currentUL.requestId}`
    );
    // get the report data
    try {
      var res = request("POST", reportURL, { json: reportRequest });
      var report = JSON.parse(res.getBody("utf8"));
      stream.write(sep + res.getBody("utf8"));
    } catch (error) {
      console.log(reportRequest);
      continue;
    }
    // Debug only
    // const reportIndex = index - 2;
    // if (reportIndex < reportStatic.length) {
    //   var report = reportStatic[reportIndex];
    // } else {
    //   continue;
    // }
    sep = sep === "" ? ",\n" : sep;
    if (report == undefined || report.GridRecord === null) {
      console.log(`Skipping Empty report for ${currentUL.requestId}`);
      continue;
    }
    //DetailSheet.getRow(1).values = report.GridHeader;
    // console.log(report.GridRecords);
    await report.GridRecords.forEach(r => DetailSheet1.addRow(r));
    await report.GridFibers.forEach(r => DetailSheet2.addRow(r));

    worksheet.getRow(index).values[10]

    const xlsFileName = `../_output/${currentUL.owner}-detail-report-${currentRow.getCell(2).value}-${currentUL.requestId}.xlsx`;
    await workBookTarget.xlsx.writeFile(xlsFileName).then(function() {
      console.log(`Saved xls file as ${xlsFileName}`);
    });
  }
  //close log stream
  stream.write("\n]", () => {
    stream.end();
  });

})();
