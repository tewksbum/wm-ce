const fs = require("fs");
const { Storage } = require("@google-cloud/storage");
const { Datastore } = require("@google-cloud/datastore");
const request = require("sync-request");
const storage = new Storage();
const datastore = new Datastore();
const schoolCodes = require("./schoolCodes.json");
const sponsorSchools = require("./sponsorSchools.json");

// Name of the folder iterate through
const folder = "./input";
// Name of the bucket to upload the rawfile to
const bucket = "ocm_school_raw_files";
const today = new Date();
const streamerURL =
  "https://us-central1-wemade-core.cloudfunctions.net/wm-dev-file-api";
const logFile = `./submitter-log-${today.toISOString()}.json`;
const stream = fs.createWriteStream(logFile, { flags: "a" });
stream.write("[\n");

findFile = function(dir) {
  var jsFiles = [];
  fs.readdirSync(dir).forEach(function(file) {
    jsFiles.push(file);
  });
  return jsFiles;
};
var responses = [];
console.log(`Starting wm-file submmiter`);
console.log(`Pushing files to ${streamerURL}`);
console.log(`Logging results in ${logFile}`);
var sep = "";
var skippedSchoolCodes = [];
(async () => {
  var files = findFile(folder);

  for (const file of files) {
    console.log("Processing " + file);
    var dataFile = folder + "/" + file;
    const programName = file.substring(8, 11);
    var schoolcode = file.substring(13, 16);

    var schoolName = schoolCodes[schoolcode];
    console.log(schoolName);
    if (schoolName === undefined) {
      let error = `Couldn't find <${schoolcode}> in schoolCodes`;
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
      skippedSchoolCodes.push(schoolcode);
      continue;
    }
    if (sponsorSchools[schoolcode] == undefined) {
      let error = `skipped, invalid schoolCode ${schoolcode}`;
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
      continue;
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

    //Upload to bucket before sending to file-api
    var bucketfile = `/drop/wm-submitter/input/${file}`;
    await storage.bucket(bucket).upload(dataFile, {
      destination: bucketfile,
      contentType: "application/octet-stream"
    });
    var streamerData = {
      // accessKey: `${accessKey}-new`,
      accessKey: `${accessKey}`,
      fileUrl: `https://storage.googleapis.com/ocm_school_raw_files/drop/wm-submitter/input/${file}`,
      // owner: `${owner}-new`,
      owner: `${owner}`,
      source: "RHAA",
      passthrough: {},
      attributes: {
        Organization: schoolcode,
        CampaignName: programName,
        Title: "2016"
      }
    };
    var res = request("POST", streamerURL, { json: streamerData });

    var streamerResponse = res.getBody("utf8");
    streamLog = JSON.stringify(
      {
        file: file,
        fileURL: streamerData.fileUrl,
        owner: streamerData.owner,
        schoolcode: schoolcode,
        accessKey: streamerData.accessKey,
        streamerResponse: JSON.parse(streamerResponse)
      },
      null,
      2
    );
    stream.write(sep + streamLog);
    sep = sep === "" ? ",\n" : sep;
  }
  stream.write("\n]", () => {
    stream.end();
    process.exit();
  });
})();
