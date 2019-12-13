const fs = require("fs");
const { Storage } = require("@google-cloud/storage");
const { Datastore } = require("@google-cloud/datastore");
const Excel = require("exceljs");
const request = require("sync-request");
const storage = new Storage();
const datastore = new Datastore();
const schoolCodes = require("./schoolCodes.json");
const sponsorSchools = require("./sponsorSchools.json");
// TODO
// Should we just make the files public? or should we copy them and reupload?
// Make it actually submit a file
// Maybe log file url aswell

// Name of the folder iterate through
const folder = "./input";
// Name of the bucket to upload the rawfile to
const bucket = "ocm_school_raw_files";
const today = new Date();
const streamerURL =
  "https://us-central1-wemade-core.cloudfunctions.net/wm-dev-file-api";
const logFile = `./logs/xsubmitter-log-${today.toISOString()}.json`;
const stream = fs.createWriteStream(logFile, { flags: "a" });
stream.write("[\n");

var responses = [];
console.log(`Starting wm-file submmiter`);
console.log(`Pushing files to ${streamerURL}`);
console.log(`Logging results in ${logFile}`);
var sep = "";
var skippedSchoolCodes = [];
(async () => {
  var workbook = new Excel.Workbook();
  workbook = await workbook.xlsx.readFile("input.xlsx");
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
    { header: "fileUrl", id: "Min", width: 10 }
  ];
  var lfiles = worksheet.rowCount;
  for (let index = 2; index < lfiles; index++) {
    await sendRequest(worksheet.getRow(index));
  }

  await workbook.xlsx.writeFile("input.xlsx");
  console.log(`Saved xls file as workBook`);
  stream.write("\n]", () => {
    stream.end();
    process.exit();
  });
})();

async function sendRequest(row) {
  row.hidden = false;
  const enabled = row.values[3] ? row.values[3] : false;
  if (enabled !== true) if (enabled.formula !== "TRUE()") return;
  const file = row.getCell(2).value;
  const source = row.getCell(1).value;
  console.log(`${source}/${file}`);
  console.log("Processing " + file);
  const programName = file.substring(0, 3);
  var schoolcode = file.substring(4, 7);
  var schoolName = schoolCodes[schoolcode];
  console.log(schoolName);
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
  if (sponsorSchools[schoolcode] == undefined) {
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
  var sponsorName = sponsorSchools[schoolcode][0];
  //Get customer data
  const query = datastore
    .createQuery("Customer")
    .filter("Owner", sponsorName)
    .limit(1);
  query.namespace = "wemade-dev";
  var accessKey = "";
  var owner = "";

  await datastore
    .runQuery(query)
    .then(results => {
      const customers = results[0];
      customers.forEach(customer => {
        accessKey = customer.AccessKey;
        owner = customer.Owner;
        const cusKey = customer[datastore.KEY];
      });
    })
    .catch(err => {
      console.error("ERROR:", err);
    });

  const bucketName = "oncampusmarketing";
  // These options will allow temporary read access to the file
  const options = {
    version: "v2", // defaults to 'v2' if missing.
    action: "read",
    expires: Date.now() + 1000 * 60 * 60 // one hour
  };

  // Get a v2 signed URL for the file
  const [url] = await storage
    .bucket(bucketName)
    .file(`${source}/${file}`)
    .getSignedUrl(options);

  console.log(`The signed url for ${file} is ${url}.`);
  // return;
  var streamerData = {
    accessKey: `${accessKey}`,
    fileUrl: url,
    owner: `${owner}`,
    source: "RHAA",
    passthrough: {},
    attributes: {
      Organization: schoolcode,
      CampaignName: programName,
      Title: "2016"
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
