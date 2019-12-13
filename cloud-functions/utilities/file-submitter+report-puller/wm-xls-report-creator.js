const request = require("sync-request");
const fs = require("fs");
const Excel = require("exceljs");
// TODO
// Add boyle schema
function toColumnName(num) {
  for (var ret = "", a = 1, b = 26; (num -= a) >= 0; a = b, b *= 26) {
    ret = String.fromCharCode(parseInt((num % b) / a) + 65) + ret;
  }
  return ret;
}

const reportURL =
  "https://us-central1-wemade-core.cloudfunctions.net/wm-dev-report-api";
const rawdata = fs.readFileSync("submitter-log-2019-12-13T04:51:09.199Z.json");
const uploadLog = JSON.parse(rawdata);
// const reportRawData = fs.readFileSync(
//   "report-log-2019-12-11T17:37:49.016Z.json"
// );
// const reportStatic = JSON.parse(reportRawData);
var workBook = new Excel.Workbook();

var ACDSheet = workBook.addWorksheet("ACD");
ACDSheet.addRow([]);
var ACDHeaderColumns = [
  { header: "Organization", key: "organization", width: 10 },
  { header: "Owner", key: "Owner", width: 10 },
  { header: "EventId", key: "EventId", width: 10 },
  { header: "fileName", key: "fileName", width: 55 },
  { header: "Records", key: "Records", width: 10 }
];
var ACDHeaderIds = ["Organization", "Owner", "EventId", "fileName", "Records"];

let lastRCC = 0;
let maxCol = 0;

var MergeSheet = workBook.addWorksheet("Merge push");
var emptyHeader = ["", "", "", "", "", "", "", ""];

var f = emptyHeader
  .concat(["Student"])
  .concat(["", "", "", "", "", "", ""])
  .concat(["Parents"]);
MergeSheet.addRow(f);
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
  { header: "Purged", key: "Purged", width: 10 }
];
var MergeHeaderIds = [
  "Organization",
  "fileName",
  "Owner",
  "TimeStamp",
  "Duration",
  "Records",
  "Dupe",
  "Purged",
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

// for (let index = 9; index < 26; index += 8) {
//   let fc = `${toColumnName(index)}1`;
//   let sc = `${toColumnName(index + 7)}1`;
//   console.log(fc, sc);
//   MergeSheet.mergeCells(fc, sc);
// }
// merge the headerrs
MergeSheet.mergeCells("I1:P1");
MergeSheet.mergeCells("Q1:X1");
MergeSheet.mergeCells("I2:L2");
MergeSheet.mergeCells("M2:P2");
MergeSheet.mergeCells("Q2:T2");
MergeSheet.mergeCells("U2:X2");
MergeSheet.mergeCells("I3:J3");
MergeSheet.mergeCells("K3:L3");
MergeSheet.mergeCells("M3:N3");
MergeSheet.mergeCells("O3:P3");
MergeSheet.mergeCells("Q3:R3");
MergeSheet.mergeCells("S3:T3");
MergeSheet.mergeCells("U3:V3");
MergeSheet.mergeCells("W3:X3");



//report logging
const today = new Date();
const logFile = `./report-log-${today.toISOString()}.json`;
const stream = fs.createWriteStream(logFile, { flags: "a" });
stream.write("[\n");
let sep = "";
console.log(`Logging reports on ${logFile}`);

for (let index = 0; index < uploadLog.length; index++) {
  let currentUL = uploadLog[index];
  if (currentUL.error != undefined) {
    console.log(`Skipping ${index}, error present`);
    continue;
  }
  const reportRequest = {
    owner: currentUL.owner,
    accessKey: currentUL.accessKey,
    reportType: "file",
    requestId: currentUL.streamerResponse.id
  };
  console.log(
    `Getting report for ${currentUL.owner} id ${currentUL.streamerResponse.id}`
  );
  // continue;
  // get the report data
  try {
    var res = request("POST", reportURL, { json: reportRequest });
    var report = JSON.parse(res.getBody("utf8"));
  } catch (error) {
    console.log(reportRequest);
    continue;
  }

  stream.write(sep + res.getBody("utf8"));
  // For debugging purposes sometimes is better to pull from a local variable instead of
  // requesting the reports
  // report = reportStatic[index];
  sep = sep === "" ? ",\n" : sep;
  if (report == undefined || report.Columns === null || report.RowCount == report.Fibers.Throwaway) {
    console.log(`Skipping Empty report for ${currentUL.streamerResponse.id}`);
    console.log(report);
    continue;
  }
  let sheetIndex = index + 1;
  //This will add the necesary columns
  for (let index = 0; index < report.ColumnCount - lastRCC; index++) {
    [
      { header: "Name", id: "Name", width: 20 },
      { header: "Mapped", id: "Mapped", width: 10 },
      { header: "Min", id: "Min", width: 10 },
      { header: "Max", id: "Max", width: 10 },
      { header: "Sparsity", id: "Sparsity", width: 10 }
    ].forEach(e => ACDHeaderColumns.push(e));

    ["Name", "Mapped", "Min", "Max", "Sparsity"].forEach(e =>
      ACDHeaderIds.push(e)
    );
  }
  ACDSheet.getRow(2).values = ACDHeaderIds;
  ACDSheet.columns = ACDHeaderColumns;

  lastRCC = report.ColumnCount;
  maxCol = report.ColumnCount > maxCol ? report.ColumnCount : maxCol;
  let ACDRow = [
    currentUL.schoolcode,
    currentUL.owner,
    report.RequestID,
    currentUL.fileURL,
    report.RowCount
  ];
  report.Columns.forEach(Column => {
    mapped = "";
    if (Column.Mapped) {
      mapped = Column.Mapped.join();
    }

    [
      Column.Name,
      mapped,
      Column.Min,
      Column.Max,
      (Column.Sparsity / report.RowCount) * 100
    ].forEach(e => [ACDRow.push(e)]);
  });
  // here we add the acd row
  ACDSheet.addRow(ACDRow);

  //Mergepush sheet
  let MergeRow = [
    currentUL.schoolcode,
    currentUL.fileURL,
    currentUL.owner,
    report.ProcessedOn,
    report.PcocessTime,
    report.RowCount,
    report.Fibers.Dupe,
    report.Fibers.Throwaway,
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
const MapBlacklist = ["FINITIAL", "ADTYPE", "ADBOOK", "ZIP5"];
const PinkBlacklist = ["ZIP,ZIP5"];
console.log(`Ignoring ${MapBlacklist} for the mapped coloring`);
ACDSheet.getRow(1).values = [];
//Set ACD merge columns logic
let columncounter = 1;
for (let index = 6; index < (maxCol + 1) * 5; index += 5) {
  let fc = `${toColumnName(index)}1`;
  let sc = `${toColumnName(index + 4)}1`;
  ACDSheet.mergeCells(fc, sc);
  ACDSheet.getCell(fc).value = `Set ${columncounter}`;
  columncounter += 1;
}
//Create hyperlinks
ACDSheet.getColumn(4).eachCell(cell => {
  if (cell.value === null || !cell.value.startsWith("http")) {
    return;
  }
  cell.value = {
    text: cell.value,
    hyperlink: cell.value,
    tooltip: cell.value
  };
});
// Mapped checks
for (let index = 7; index < (maxCol + 2) * 5; index += 5) {
  ACDSheet.getColumn(index).eachCell(cell => {
    if (cell.value === "Mapped" || cell.value == null || cell.row === 1) {
      return;
    }
    //Paint the cell red if mapped has more than one element ignoring the blacklist
    mapped = cell.value.split(",").filter(function(el) {
      return MapBlacklist.indexOf(el) < 0;
    });
    if (mapped.length > 1) {
      cell.fill = {
        type: "gradient",
        gradient: "path",
        center: { left: 0.5, top: 0.5 },
        stops: [
          { position: 0, color: { argb: "FFFF0000" } },
          { position: 1, color: { argb: "FFFF0000" } }
        ]
      };
      return;
    }
    // Paint the cell pink if name is in the mapped column and not in the PinkBlacklist
    const name = cell._row.getCell(cell.col - 1).value;
    const inBlackList = PinkBlacklist.indexOf(name) > 0;
    // Check if name is in the cell
    const matches = cell.value.match(new RegExp(name, "g"));
    // If the name is not in the cell paint it yellow
    if (matches === null) {
      cell.fill = {
        type: "gradient",
        gradient: "path",
        center: { left: 0.5, top: 0.5 },
        stops: [
          { position: 0, color: { argb: "FFFFFF99" } },
          { position: 1, color: { argb: "FFFFFF99" } }
        ]
      };
      return;
    }
    if (matches.length > 1 && inBlackList) {
      cell.fill = {
        type: "gradient",
        gradient: "path",
        center: { left: 0.5, top: 0.5 },
        stops: [
          { position: 0, color: { argb: "FFFF9999" } },
          { position: 1, color: { argb: "FFFF9999" } }
        ]
      };
    }
  });
}

//Sparcity check
for (let index = 10; index < (maxCol + 2) * 5; index += 5) {
  ACDSheet.getColumn(index).eachCell(cell => {
    if (cell.value === "Sparsity" || cell.value == null || cell.row === 1) {
      return;
    }
    const curSparsity = cell.value;
    if (curSparsity < 70) {
      cell.fill = {
        type: "gradient",
        gradient: "path",
        center: { left: 0.5, top: 0.5 },
        stops: [
          { position: 0, color: { argb: "FFFF9999" } },
          { position: 1, color: { argb: "FFFF9999" } }
        ]
      };
    }
  });
}
const xlsFileName = "acdValidation.xlsx";
workBook.xlsx.writeFile(xlsFileName).then(function() {
  console.log(`Saved xls file as ${xlsFileName}`);
});
