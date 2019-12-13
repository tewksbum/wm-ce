## wm-json-file-submitter.js

Run it with `node wm-json-file-submitter.js`

- Submits every file on the `input/` folder on the root of the project to wemade's pipeline with a school code and owner based on the files names which it gets it from datastore and logs the results on `submitter-log*.json`

### Pre-requisistes

- Install Nodejs
- run `$npm install` on the root of the folder
- Have a google json credential set up from a service account, checkout documentation [here](https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-nodejs)

## wm-json-report-creator.js

Run it with `node wm-json-report-creator.js`

- Creates an ACD and merge push xls report from the `report-input.json` which is based on the output of the wm-json-file-submitter, logs the reports to a `report-log*.json` file
  There is an an example on the `report-input.json` currently, but the best way to get it is to push the files yourself and copy the `wm-file-submitter-log` contents into it

### Pre-requisistes

- Install Nodejs
- run `$npm install` on the root of the folder

## wm-xlsx-file-submitter.js

Pretty much the same as the json one.
Run it with `node wm-xlsx-file-submitter.js`

- Reads the `input.xlsx` file and submits them from the oncampus marketing folder based on their name and pathto wemade's pipeline with a school code and owner based on the files names which it gets it from datastore and logs the results on `xsubmitter-log*.json` and on the `input.xlsx`

### Pre-requisistes

- Install Nodejs
- Install the requirements with `$npm install` on the batch-report folder
- Prepare an `input.xlsx` file, you can use the `list.js` on `list-bucket-to-excel` to get the bucket file list
- Have a google json credential set up from a service account, checkout documentation [here](https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-nodejs)

## wm-xlsx-report-creator.js

Run it with `node wm-xlsx-report-creator.js`

- Creates an ACD and merge push xls report from the `input.xlsx` which is the output of the wm-xls-file-submitter, logs the reports to a `xreport-log*.json` file

### Pre-requisistes

- Install Nodejs
- run `$npm install` on the root of the folder
