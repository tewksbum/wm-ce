const { Storage } = require("@google-cloud/storage");
const { PubSub } = require("@google-cloud/pubsub");
const moment = require("moment");
const storage = new Storage();
const Client = require("ssh2-sftp-client");

const HOST = "secureftp.customerportfolios.com";
const PORT = "22";
const USER = "OCM_CRM";
const PASS = "8NBI/TSnTf";

const topicName = "wm-status-updater-dev";
const projectID = "wemade-core";

const bucketName = "wm_cp_upload_dev";
//const bucketName = "wm_cp_uploaded/dev";
const bucketNameDest = "gs://wm_cp_uploaded_dev/";

const pubSubClient = new PubSub({ projectID });
const sftp = new Client();

const config = {
  host: HOST,
  port: PORT,
  username: USER,
  password: PASS,
};

let remotePath;

const getBucketFiles = async () => {
  try {
    const [files] = await storage.bucket(bucketName).getFiles();
    for (const file of files) {
      console.log("Uploading file: " + file.name);
      var splitFilename = file.name.split(".", 5);
      var program = splitFilename[1];

      switch (program.toLowerCase()) {
        case "fep":
        case "ffep":
        case "sfep":
        case "wfep":      
        case "fepwinter":
        case "fepsummer":
        case "fepfall":
        case "fepspring":
          remotePath = "/FEP/Input/";
          break;
        case "cwp":
        case "ocgifting":
          remotePath = "/CWP/Input/";
          break;
        case "frames":
        case "dd":  
        case "ddwinter":
        case "ddsummer":
        case "ddfall":
        case "ddspring":
          remotePath = "/FRAMES/Input/";
          break;
        case "carpet":
        case "car":
          remotePath = "/RHC/Input/";
          break;
        case "linens":
        case "linen":
        case "ocmovein":
        case "off2school":
        case "rhl":
        case "rhlwinter":
        case "rhlsummer":
        case "rhlfall":
        case "rhlspring":
          remotePath = "/RHL/Input/";
          break;
      }

      if (bucketName === "wm_cp_upload_dev") {
        remotePath = "/WeMade/test/";
      }

 if (remotePath) {
       const ok = await uploadSFTPFile(file);
        console.log(`Uploaded file.name: ${ok}`);
        if (ok) {
          //copy the file to the uploaded folder
          await file.move(bucketNameDest + file.name, function (
            err,
            destinationFile,
            apiResponse
          ) {});
          //console.log(`${file.name} uploaded to ${bucketNameDest}.`);

          data = {
            eventId: splitFilename[3],
            message:
              "PROCESSED date:" +
              moment().format("YYYY.MM.DD HH:mm:ss") +
              " count:" +
              splitFilename[4],
          };
          await publishMessage(data);
        }
      } else {
        data = {
          eventId: splitFilename[3],
          message:
            "The program [" +
            program +
            "] doesn't have a destination folder on CP.",
        };
        await publishMessage(data);
      }
    }
  } catch (err) {
    console.log("Error: ", err);
  }
};

const uploadSFTPFile = async (file) => {
  try {
    const data = await file.download();
    await sftp.connect(config);
    await sftp.put(data[0], remotePath + file.name);
    const stats = await sftp.stat(remotePath + file.name);
    await sftp.end();

    if (file.metadata.size == stats.size) {
      return true;
    }

    return false;
  } catch (e) {
    console.log("An error ocurred: " + e.message);
    return false;
  }
};

const publishMessage = async (data) => {
  const dataBuffer = Buffer.from(JSON.stringify(data));

  const messageId = await pubSubClient.topic(topicName).publish(dataBuffer);
  console.log(`Message ${messageId} published.`);
};

(async () => {
  await getBucketFiles();
})();
