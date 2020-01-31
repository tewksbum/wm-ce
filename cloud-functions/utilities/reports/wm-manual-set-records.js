#!/usr/bin/node
const request = require("sync-request");
const fs = require("fs");
const Excel = require("exceljs");
(async () => {
  const reportURL =
    "http://localhost:8090/wm-prod-report-api";
  
  var workBook = new Excel.Workbook();
  
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
    
  var workBookTarget = new Excel.Workbook();
  var DetailSheet1 = workBookTarget.addWorksheet("Records");
  var DetailSheet2 = workBookTarget.addWorksheet("Fibers");
  const reportRequest = {
    owner: "wiu-saa",
    accessKey: "cf255eb8fd38d343e86e11bad1711b9e9710ce1e",
    reportType: "setrecord",
    requestId: "68156d5e-9587-456f-a610-cddde25f1ac2"
  };

  // get the report data
  try {
    var res = request("POST", reportURL, { json: reportRequest });
    var report = JSON.parse(res.getBody("utf8"));
    stream.write(sep + res.getBody("utf8"));
  } catch (error) {
    console.log(reportRequest);
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
    console.log(`Skipping Empty report`);
  }
  //DetailSheet.getRow(1).values = report.GridHeader;
  // console.log(report.GridRecords);
  await report.GridRecords.forEach(r => DetailSheet1.addRow(r));
  
  const xlsFileName = `../_output/${reportRequest.owner}-${reportRequest.reportType}-${reportRequest.requestId}.xlsx`;

  await workBookTarget.xlsx.writeFile(xlsFileName).then(function() {
    console.log(`Saved xls file as ${xlsFileName}`);
  });
  
  //close log stream
  stream.write("\n]", () => {
    stream.end();
  });

})();
