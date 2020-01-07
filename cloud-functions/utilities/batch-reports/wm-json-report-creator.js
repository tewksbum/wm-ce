const request = require("sync-request");
const fs = require("fs");
const Excel = require("exceljs");
// TODO
function toColumnName(num) {
  for (var ret = "", a = 1, b = 26; (num -= a) >= 0; a = b, b *= 26) {
    ret = String.fromCharCode(parseInt((num % b) / a) + 65) + ret;
  }
  return ret;
}
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

var MergeSheet = workBook.addWorksheet("Merge pull");
var emptyHeader = ["", "", "", "", "", "", "", ""];

var f = emptyHeader
  .concat(["Student"])
  .concat(["", "", "", "", "", "", ""])
  .concat(["Parents"]);
MergeSheet.addRow(f);
for (let index = 9; index < 3 * 8; index += 8) {
  let fc = `${toColumnName(index)}1`;
  let sc = `${toColumnName(index + 7)}1`;
  MergeSheet.mergeCells(fc, sc);
  console.log(fc);
}
MergeSheet.getCell("I1").fill = {
  type: "gradient",
  gradient: "path",
  center: { left: 0.5, top: 0.5 },
  stops: [
    { position: 0, color: { argb: "FF8A2BE2" } },
    { position: 1, color: { argb: "FF8A2BE2" } }
  ]
};
MergeSheet.getCell("Q1").fill = {
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
  { header: "Dupes", key: "Purged", width: 10 }
];
var MergeHeaderIds = [
  "Organization",
  "fileName",
  "Owner",
  "TimeStamp",
  "Duration",
  "Records",
  "Purged",
  "Dupes",
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

//report logging
const today = new Date();
const logFile = `./logs/report-log-${today.toISOString()}.json`;
const stream = fs.createWriteStream(logFile, { flags: "a" });
stream.write("[\n");
let sep = "";
console.log(`Logging reports on ${logFile}`);

const reportURL =
  "https://us-central1-wemade-core.cloudfunctions.net/wm-dev-report-api";
// const rawdata = fs.readFileSync("report-input.json");
// const uploadLog = JSON.parse(rawdata);
const uploadLog = [
  {
    file: "RHL-ABR1R8LQQQNewResAdditionalAldersonBroaddusUniv.xlsx",
    fileURL: "replace with your file url",
    owner: "abr-sa",
    schoolcode: "ABR",
    accessKey: "9acdb71706185155366ed2fd3d7700a31aabfe1c",
    streamerResponse: {
      success: true,
      message: "Request queued",
      id: "7b3e9850-2949-41aa-b9d2-49804911be67"
    }
  }
];
// const reportRawData = fs.readFileSync(
//   "report-log-2019-12-11T17:37:49.016Z.json"
// );
const reportStatic = [
  {
    RequestID: "7b3e9850-2949-41aa-b9d2-49804911be67",
    RowCount: 226,
    ColumnCount: 8,
    Columns: [
      {
        Name: "ZIP",
        Min: "11575",
        Max: "95376",
        Sparsity: 226,
        Mapped: ["ZIP", "ZIP5"]
      },
      {
        Name: "Fake",
        Min: "11575",
        Max: "95376",
        Sparsity: 226,
        Mapped: []
      },
      { Name: "STATE", Min: "AZ", Max: "WV", Sparsity: 226, Mapped: ["STATE"] },
      {
        Name: "PREFERRED  EMAIL  ADDRESS",
        Min: "abrilja@battlers.ab.edu",
        Max: "zirklekn@battlers.ab.edu",
        Sparsity: 226,
        Mapped: ["EMAIL"]
      },
      {
        Name: "LAST  NAME",
        Min: "Abril",
        Max: "Zirkle",
        Sparsity: 226,
        Mapped: ["LNAME"]
      },
      {
        Name: "FIRST  NAME",
        Min: "Abdullah",
        Max: "William",
        Sparsity: 226,
        Mapped: ["FNAME", "FINITIAL"]
      },
      {
        Name: "CITY",
        Min: "Accident",
        Max: "Zanesville",
        Sparsity: 226,
        Mapped: ["CITY"]
      },
      {
        Name: "ADDRESS  LINE 2",
        Min: "220 Vireo Ln",
        Max: "PO Box 8009",
        Sparsity: 2,
        Mapped: ["AD1", "ADTYPE", "ADBOOK", "AD2"]
      },
      {
        Name: "ADDRESS  LINE 1",
        Min: "1 Steuben Dr",
        Max: "Urb Surena 85 Via Delsol St",
        Sparsity: 226,
        Mapped: ["AD1", "ADTYPE", "ADBOOK", "AD2"]
      }
    ],
    ProcessedOn: "2019-12-13T13:01:34.464633Z",
    PcocessTime: "33.642174 s",
    Fibers: {
      Person: 226,
      Dupe: 226,
      Throwaway: 0,
      HouseHold: 0,
      NDFS: 0,
      NDFP: 0,
      NDUS: 0,
      NDUP: 0,
      NIFS: 0,
      NIFP: 0,
      NIUS: 0,
      NIUP: 0,
      EDFS: 0,
      EDFP: 0,
      EDUS: 0,
      EDUP: 0,
      EIFS: 0,
      EIFP: 0,
      EIUS: 0,
      EIUP: 0
    }
  }
];
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
  if (reportStatic !== undefined) {
    var report = reportStatic[index];
  } else {
    try {
      var res = request("POST", reportURL, { json: reportRequest });
      var report = JSON.parse(res.getBody("utf8"));
      stream.write(sep + res.getBody("utf8"));
    } catch (error) {
      console.log(reportRequest);
      continue;
    }
  }

  // For debugging purposes sometimes is better to pull from a local variable instead of
  // requesting the reports
  // report = reportStatic[index];
  sep = sep === "" ? ",\n" : sep;
  if (report == undefined || report.Columns === null) {
    console.log(`Skipping Empty report for ${currentUL.streamerResponse.id}`);
    console.log(report);
    continue;
  }
  let sheetIndex = index + 1;
  //This will add the necesary columns
  for (let index = 0; index < report.ColumnCount - lastRCC; index++) {
    [
      { header: "Source", id: "Name", width: 20 },
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
    report.Fibers.Throwaway,
    report.Fibers.Dupe,
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
      cell.fill = undefined;
      return;
    }
    //Paint the cell red if mapped has more than one element ignoring the blacklist
    mapped = cell.value.split(",").filter(function(el) {
      return MapBlacklist.indexOf(el) < 0;
    });
    if (mapped.length > 1) {
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FF0000" }
      };
      return;
    }
    // Paint the cell pink if name is in the mapped column and not in the PinkBlacklist
    const name = cell._row.getCell(cell.col - 1).value;
    const inBlackList = PinkBlacklist.indexOf(name) > 0;
    // Check if name is in the cell
    const NameMappedmatches = cell.value.match(new RegExp(name, "g"));
    const MappedNamematches = name.match(new RegExp(cell.value, "g"));
    // If the name is not in the cell paint it yellow
    if (NameMappedmatches === null && MappedNamematches === null) {
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FFFF99" }
      };
      return;
    }
    //pink
    if ((NameMappedmatches || MappedNamematches) && inBlackList) {
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FF9999" }
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
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FF9999" }
      };
    }
  });
}
const xlsFileName = "acdValidation.xlsx";
workBook.xlsx.writeFile(xlsFileName).then(function() {
  console.log(`Saved xls file as ${xlsFileName}`);
});
