#!/usr/bin/node
const request = require("sync-request");
const fs = require("fs");
const Excel = require("exceljs");
function toColumnName(num) {
  for (var ret = "", a = 1, b = 26; (num -= a) >= 0; a = b, b *= 26) {
    ret = String.fromCharCode(parseInt((num % b) / a) + 65) + ret;
  }
  return ret;
}
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
  
  let lastRCC = 0;
  let maxCol = 0;

  var MergeSheet = workBook.addWorksheet("Merge purge");
  var emptyHeader = ["", "", "", "", "", "", "", "", ""];

  var f = emptyHeader
    .concat(["Student"])
    .concat(["", "", "", "", "", "", ""])
    .concat(["Parents"]);
  MergeSheet.addRow(f);
  for (let index = 10; index < 3 * 8; index += 8) {
    let fc = `${toColumnName(index)}1`;
    let sc = `${toColumnName(index + 7)}1`;
    MergeSheet.mergeCells(fc, sc);
    console.log(fc);
  }
  MergeSheet.getCell("J1").fill = {
    type: "gradient",
    gradient: "path",
    center: { left: 0.5, top: 0.5 },
    stops: [
      { position: 0, color: { argb: "FF8A2BE2" } },
      { position: 1, color: { argb: "FF8A2BE2" } }
    ]
  };
  MergeSheet.getCell("R1").fill = {
    type: "gradient",
    gradient: "path",
    center: { left: 0.5, top: 0.5 },
    stops: [
      { position: 0, color: { argb: "FFFFFF00" } },
      { position: 1, color: { argb: "FFFFFF00" } }
    ]
  };
  var s = emptyHeader;
  for (let index = 0; index < 2; index++) {
    s = s.concat(["New", "", "", ""]);
    s = s.concat(["Existing", "", "", ""]);
  }
  MergeSheet.addRow(s);
  var t = emptyHeader;
  for (let index = 0; index < 4; index++) {
    t = t.concat(["Domestic", "", "International", ""]);
  }
  MergeSheet.addRow(t);
  var MergeHeaderColumns = [
    { header: "Organization", key: "organization", width: 10 },
    { header: "fileName", key: "fileName", width: 55 },
    { header: "Owner", key: "Owner", width: 10 },
    { header: "TimeStamp", key: "TimeStamp", width: 10 },
    { header: "Duration", key: "Duration", width: 10 },
    { header: "Records", key: "Records", width: 10 },
    { header: "Purged", key: "Purged", width: 10 },
    { header: "Dupes", key: "Purged", width: 10 },
    { header: "Invalid", key: "Invalid", width: 10 }
  ];
  var MergeHeaderIds = [
    "Organization",
    "fileName",
    "Owner",
    "TimeStamp",
    "Duration",
    "Rows",
    "Purged",
    "Dupes",
    "Non Valid Address",
    "Freshman",
    "Upperclassmen",
    "Freshman",
    "Upperclassmen",
    "Freshman",
    "Upperclassmen",
    "Freshman",
    "Upperclassmen",
    "Freshman",
    "Upperclassmen",
    "Freshman",
    "Upperclassmen",
    "Freshman",
    "Upperclassmen",
    "Freshman",
    "Upperclassmen"
  ];
  MergeSheet.getRow(4).values = MergeHeaderIds;
  MergeSheet.columns = MergeHeaderColumns;
  MergeSheet.getCell("A1").value = "";
  MergeSheet.getCell("B1").value = "";
  MergeSheet.getCell("C1").value = "";
  MergeSheet.getCell("D1").value = "";
  MergeSheet.getCell("E1").value = "";
  MergeSheet.getCell("F1").value = "";
  MergeSheet.getCell("G1").value = "";
  MergeSheet.getCell("H1").value = "";
  MergeSheet.getCell("I1").value = "";

  //report logging
  const today = new Date();
  const logFile = `./logs/xreport-log-${today.toISOString()}.json`;
  const stream = fs.createWriteStream(logFile, { flags: "a" });
  stream.write("[\n");
  let sep = "";
  console.log(`Logging reports on ${logFile}`);
  // const rawData = fs.readFileSync(
  //   "./logs/xreport-log-2019-12-17T02:53:18.753Z.json"
  // );
  // const reportStatic = JSON.parse(rawData);
  // Here we start going through each row
  const worksheet = workBook.getWorksheet(1);
  for (let index = 2; index < worksheet.rowCount; index++) {
    let currentRow = worksheet.getRow(index);
    currentRow.hidden = false;
    const enabled = currentRow.values[3] ? currentRow.values[3] : false;
    if (enabled !== true) if (enabled.formula !== "TRUE()") continue;
    let currentUL = {
      schoolcode: currentRow.getCell(5).value,
      fileURL: currentRow.getCell(9).value,
      owner: currentRow.getCell(6).value,
      accessKey: currentRow.getCell(7).value,
      requestId: currentRow.getCell(8).value
    };
    if (currentUL.error != undefined) {
      console.log(`Skipping ${index}, error present`);
      continue;
    }
    const reportRequest = {
      owner: currentUL.owner,
      accessKey: currentUL.accessKey,
      reportType: "file",
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
    if (report == undefined || report.Columns === null) {
      console.log(`Skipping Empty report for ${currentUL.requestId}`);
      console.log(report);
      continue;
    }
    let sheetIndex = index + 1;
    //This will add the necesary columns
   
    //Mergepush sheet
    let MergeRow = [
      currentUL.schoolcode,
      currentUL.fileURL,
      currentUL.owner,
      report.ProcessedOn,
      report.PcocessTime,
      report.RowCount,
      report.Fibers.Throwaway,
      report.Fibers.Dupe,
      report.Fibers.Invalid,
      report.Fibers.NDFS,
      report.Fibers.NDUS,
      report.Fibers.NIFS,
      report.Fibers.NIUS,
      report.Fibers.EDFS,
      report.Fibers.EDUS,
      report.Fibers.EIFS,
      report.Fibers.EIUS,
      report.Fibers.NDFP,
      report.Fibers.NDUP,
      report.Fibers.NIFP,
      report.Fibers.NIUP,
      report.Fibers.EDFP,
      report.Fibers.EDUP,
      report.Fibers.EIFP,
      report.Fibers.EIUP
    ];
    //Add row to merge sheet
    MergeSheet.addRow(MergeRow);
  }
  //close log stream
  stream.write("\n]", () => {
    stream.end();
  });
 
  //Create hyperlinks
  MergeSheet.getColumn(2).eachCell(cell => {
    if (cell.value === null || !cell.value.startsWith("http")) {
      return;
    }
    cell.value = {
      text: cell.value,
      hyperlink: cell.value,
      tooltip: cell.value
    };
  });
 
  worksheet.columns.forEach(c => (c.hidden = false));
  worksheet.getRow(1).hidden = false;
  const xlsFileName = "../_output/report.xlsx";
  workBook.xlsx.writeFile(xlsFileName).then(function() {
    console.log(`Saved xls file as ${xlsFileName}`);
  });
})();