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
(async () => {
  const reportURL =
    "https://us-central1-wemade-core.cloudfunctions.net/wm-dev-report-api";
  let inputFilename = "input.xlsx";
  if (process.argv.length === 2) {
    console.error(`Using default input file ${inputFilename}`);
  } else {
    inputFilename = process.argv[2];
  }
  var workBook = new Excel.Workbook();
  workBook = await workBook.xlsx.readFile(inputFilename);
  var ACDSheet = workBook.addWorksheet("ACD");
  ACDSheet.addRow([]);
  var ACDHeaderColumns = [
    { header: "Organization", key: "organization", width: 10 },
    { header: "Owner", key: "Owner", width: 10 },
    { header: "EventId", key: "EventId", width: 10 },
    { header: "fileName", key: "fileName", width: 55 },
    { header: "Records", key: "Records", width: 10 }
  ];
  var ACDHeaderIds = [
    "Organization",
    "Owner",
    "EventId",
    "fileName",
    "Records"
  ];

  let lastRCC = 0;
  let maxCol = 0;

  var MergeSheet = workBook.addWorksheet("Merge purge");
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
  const logFile = `./logs/xreport-log-${today.toISOString()}.json`;
  const stream = fs.createWriteStream(logFile, { flags: "a" });
  stream.write("[\n");
  let sep = "";
  console.log(`Logging reports on ${logFile}`);

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
    // continue;
    // get the report data
    try {
      var res = request("POST", reportURL, { json: reportRequest });
      var report = JSON.parse(res.getBody("utf8"));
      stream.write(sep + res.getBody("utf8"));
    } catch (error) {
      console.log(reportRequest);
      continue;
    }

    sep = sep === "" ? ",\n" : sep;
    if (report == undefined || report.Columns === null) {
      console.log(`Skipping Empty report for ${currentUL.requestId}`);
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
  const whiteList = [
    "ZIP,ZIP5",
    "AD1,ADTYPE,ADBOOK",
    "AD2,ADTYPE,ADBOOK",
    "FNAME,FINITIAL",
    "TITLE,STATUS"
  ];
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
      const inWhiteList = whiteList.indexOf(cell.value) > -1;
      //white
      if (inWhiteList) {
        cell.fill = undefined;
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
  const xlsFileName = "report.xlsx";
  workBook.xlsx.writeFile(xlsFileName).then(function() {
    console.log(`Saved xls file as ${xlsFileName}`);
  });
})();
