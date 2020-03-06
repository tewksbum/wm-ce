const { Storage } = require("@google-cloud/storage");
const storage = new Storage();
const Client = require('ssh2-sftp-client');
const { Datastore } = require("@google-cloud/datastore");

const HOST = 'secureftp.customerportfolios.com';
const PORT = '22';
const USER = 'OCM_CRM';
const PASS = '8NBI/TSnTf';
const REMOTE_PATH = '/FEP/Input/';

const projectId = "fire-core-wm"
const ENV = "dev"

const datastore = new Datastore({ projectId });
const sftp = new Client();

const config = {
  host: HOST,
  port: PORT,
  username: USER,
  password: PASS
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
      console.log(`Uploaded file.name: ${file.name} status: ${ok}`);
      if (ok) {
        // copy the file to the uploaded folder
        const anotherBucket = storage.bucket(bucketNameDest);

        await file.move(anotherBucket, function (err, destinationFile, apiResponse) { })
        console.log(`${file.name} uploaded to ${bucketNameDest}.`);
        await updateEvent((file.name).replace(/\.[^/.]+$/, ""));
      }
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
    console.log("An error ocurred: " + e.message);
    return false;
  } // catch ...
} // uploadSFTPFile ...

const updateEvent = async eventId => {
  datastore.namespace = `wemade-${ENV}`;
  const query = datastore.createQuery("Event").filter("EventID", "=", eventId).limit(1);
  try {
    const event = await datastore.runQuery(query);
    if (event) {
      event[0][0].Status = "Uploaded in CP on " + new Date();

      //Update event
      await datastore.save(event[0][0]);
      console.log(`Event ${eventId} updated successfully.`)
    }
  } catch (e) {
    console.log("An error ocurred: " + e.message);
  }
}

(async () => {

  console.log(`starting file scan...: `);

  await getBucketFiles();

})();