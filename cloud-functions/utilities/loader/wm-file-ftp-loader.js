const { Storage } = require("@google-cloud/storage");
const { PubSub } = require('@google-cloud/pubsub');
const storage = new Storage();
const Client = require('ssh2-sftp-client');
const sftp = new Client();

const HOST = 'secureftp.customerportfolios.com';
const PORT = '22';
const USER = 'OCM_CRM';
const PASS = '8NBI/TSnTf';
const REMOTE_PATH = '/FEP/Input/';

const topicName = "ftp-file-output-dev"
const projectID = "wemade-core"

const pubSubClient = new PubSub({ projectID });

const config = {
  host: HOST,
  port: PORT,
  username: USER,
  password: PASS,
  readyTimeout: 60000
};

const getBucketFiles = async () => {
  const bucketName = "wm_cp_upload";//"wm_cp_uploaded";
  const bucketNameDest = "wm_cp_uploaded";//"wm_cp_upload";
  try {
    const [files] = await storage
      .bucket(bucketName)
      .getFiles();
    for (const file of files) {
      console.log("Uploading file: " + file.name);
      const ok = await uploadSFTPFile(file);
      console.log(`Uploaded file.name: ${ok}`);
      if (ok) {
        //copy the file to the uploaded folder
        const anotherBucket = storage.bucket(bucketNameDest);

        await file.move(anotherBucket, function (err, destinationFile, apiResponse) { })
        console.log(`${file.name} uploaded to ${bucketNameDest}.`);

        data = {
          filename: file.name,
          eventType: "UPLOAD",
          status: "SUCCESS",
          date: new Date()
        }

      } else {
        data = {
          filename: file.name,
          eventType: "UPLOAD",
          status: "ERROR",
          date: new Date()
        }
      }

      await publishMessage(data);
    }
  }
  catch (err) {
    console.log("Error: ", err);
  }
}

const uploadSFTPFile = async file => {
  try {
    const data = await file.download();
    await sftp.connect(config);
    await sftp.put(data[0], REMOTE_PATH + file.name);
    const stats = await sftp.stat(REMOTE_PATH + file.name);

    console.log(`Filename: ${file.name}`);
    console.log(`Bucket Size: ${file.metadata.size}`);
    console.log(`Remote Size: ${stats.size}`);
    await sftp.end();
    return true;
  } // try ...
  catch (e) {
    console.log("An error ocurred: " + err.message);
    return false;
  } // catch ...
} // uploadSFTPFile ...

const publishMessage = async data => {
  const dataBuffer = Buffer.from(JSON.stringify(data));

  const messageId = await pubSubClient.topic(topicName).publish(dataBuffer);
  console.log(`Message ${messageId} published.`);
}

(async () => {

  console.log(`starting file scan...: `);

  await getBucketFiles();

})();