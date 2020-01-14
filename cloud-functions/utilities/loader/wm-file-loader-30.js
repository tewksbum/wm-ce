#!/usr/bin/node
const fs = require("fs");
const { Storage } = require("@google-cloud/storage");
const Excel = require("exceljs");
const request = require("sync-request");
const storage = new Storage();
const schoolCodes = require("./schoolCodes.json");
const { getOwners } = require("./utils");

const today = new Date();
const streamerURL =
  "https://us-central1-wemade-core.cloudfunctions.net/wm-dev-file-api";
const logFile = `./logs/xsubmitter-log-${today.toISOString()}.json`;
const stream = fs.createWriteStream(logFile, { flags: "a" });
stream.write("[\n");

console.log(`Starting wm-file submmiter`);
console.log(`Pushing files to ${streamerURL}`);
console.log(`Logging results in ${logFile}`);
var sep = "";
var skippedSchoolCodes = [];
(async () => {
  let inputFilename = "../_input/input.xlsx";
  if (process.argv.length === 2) {
    console.error(`Using default input file ${inputFilename}`);
  } else {
    inputFilename = process.argv[2];
  }
  var workbook = new Excel.Workbook();
  workbook = await workbook.xlsx.readFile(inputFilename);
  var worksheet = workbook.getWorksheet("Files");
  var header = worksheet.getRow(1);
  header.hidden = false;
  worksheet.properties.defaultRowHeight = 15;
  worksheet.columns = [
    { header: "Path", id: "Name", width: 10 },
    { header: "File", id: "Mapped", width: 25 },
    { header: "Enabled", id: "Min", width: 10 },
    { header: "Success", id: "Min", width: 10 },
    { header: "School code", id: "Min", width: 10 },
    { header: "Owner", id: "Min", width: 10 },
    { header: "AccessKey", id: "Min", width: 25 },
    { header: "RequestId", id: "Min", width: 10 },
    { header: "fileUrl", id: "Min", width: 10 },
    { header: "Sequence", id: "Min", width: 10 }
  ];

  var lfiles = worksheet.rowCount;
  let wroteFlag = false;
  let index = 2;
  let seq = 1;

  // console.log(`starting file scan... files: `, worksheet.rowCount);
  // for (let seq = 1; seq < 6; seq++) {
  while (seq < 10) {
    console.log(`checking for sequence: `, seq);
    while (index <= lfiles) {
      const currentRow = worksheet.getRow(index);
      // for (index; index < lfiles; index++) {
      // console.log(`current row seq: `, worksheet.getRow(index).values[10]);
      if (seq == currentRow.values[10]) {
        console.log(`classYear: `, worksheet.getRow(index).values[11]);
        console.log(`processing file...`);
        await sendRequest(currentRow, currentRow.values[11]);
        wroteFlag = true;
      }
      currentRow.hidden = false;
      index++;
    }
    if (wroteFlag) {
      // console.log(`waiting for files to process`);
      await nap(2500);
    }
    // console.log(`reset wait`);
    wroteFlag = false;
    index = 2;
    seq++;
  }

  // await workbook.xlsx.writeFile("input.xlsx");
  //To avoid corrupting files we should only save them if there was any change.
  await workbook.xlsx.writeFile(inputFilename);
  console.log(`Saved xls file as workBook`);
  stream.write("\n]", () => {
    stream.end();
    process.exit();
  });
})();

async function sendRequest(row, classYear) {
  const enabled = row.values[3] ? row.values[3] : false;
  if (enabled !== true) if (enabled.formula !== "TRUE()") return;
  const file = row.getCell(2).value;
  const source = row.getCell(1).value;
  console.log(`${source}/${file}`);
  console.log("Processing " + file);
  const programName = file.substring(0, 4); //4x for FEP, 3x for DD
  // var schoolcode = file.substring(4, 7);
  var schoolcode = row.values[5];
  var schoolName = schoolCodes[schoolcode];
  var titleYear = row.values[12];
  if (typeof titleYear == "undefined") {
    titleYear = ""
  }
  if (typeof classYear == "undefined") {
    classYear = ""
  }
  if (schoolName === undefined) {
    let error = `Couldn't find <${schoolcode}> in schoolCodes`;
    console.log(error);
    row.getCell(4).value = "false";
    streamError = JSON.stringify(
      {
        file: file,
        schoolcode: schoolcode,
        error: error
      },
      null,
      2
    );
    stream.write(sep + streamError);
    sep = sep === "" ? ",\n" : sep;
    skippedSchoolCodes.push(schoolcode);
    return;
  }
  const owners = await getOwners("dev");
  if (owners[schoolcode] == undefined) {
    let error = `skipped, invalid schoolCode ${schoolcode}`;
    console.log(error);
    row.getCell(4).value = "false";
    streamError = JSON.stringify(
      {
        file: file,
        schoolcode: schoolcode,
        error: error
      },
      null,
      2
    );
    stream.write(sep + streamError);
    sep = sep === "" ? ",\n" : sep;
    return;
  }
  const accessKey = owners[schoolcode].AccessKey;
  const owner = owners[schoolcode].Owner;

  const bucketName = "oncampusmarketing";
  const options = {
    version: "v2",
    action: "read",
    expires: Date.now() + 1000 * 60 * 60 // one hour
  };

  // Get a v2 signed URL for the file
  const [url] = await storage
    .bucket(bucketName)
    .file(`${source}/${file}`)
    .getSignedUrl(options);

  // This is what we send
  var streamerData = {
    accessKey: `${accessKey}`,
    fileUrl: url,
    maxRows: 30,
    owner: `${owner}`,
    source: "RHA",
    passthrough: {},
    attributes: {
      Organization: schoolcode,
      CampaignName: programName,
      Title: classYear.toString(),
      TitleYear: titleYear.toString()
    }
  };

  try {
    var res = request("POST", streamerURL, { json: streamerData });
    var streamerResponse = res.getBody("utf8");
    streamLog = JSON.stringify(
      {
        file: file,
        fileURL: streamerData.fileUrl,
        schoolcode: schoolcode,
        owner: streamerData.owner,
        accessKey: streamerData.accessKey,
        streamerResponse: JSON.parse(streamerResponse)
      },
      null,
      2
    );
    responseObject = JSON.parse(streamerResponse);
    row.getCell(4).value = "TRUE";
    row.getCell(5).value = schoolcode;
    row.getCell(6).value = streamerData.owner;
    row.getCell(7).value = streamerData.accessKey;
    row.getCell(8).value = responseObject.id;
    row.getCell(9).value = url;
    stream.write(sep + streamLog);
    sep = sep === "" ? ",\n" : sep;
    sleep(20000);
  } catch (error) {
    row.getCell(4).value = "false";
    console.log(error);
    streamError = JSON.stringify(
      {
        file: file,
        schoolcode: schoolcode,
        error: error
      },
      null,
      2
    );
    stream.write(sep + streamError);
    sep = sep === "" ? ",\n" : sep;
  }
  row.commit();
  return;
}

function sleep(time) {
  var stop = new Date().getTime();
  while (new Date().getTime() < stop + time) {}
}

function nap(ms) {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}