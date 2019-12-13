**fep.js**

- loads pre-processed OCM FEP file into database, run lookups, write output
- requires mounting OCM network share to a local mount point

**rhl.js**

- loads pre-processed OCM RHL files into database

## wm-json-file-submitter.js

Run it with `node wm-json-file-submitter.js`

- Submits every file on the `input/` folder on the root of the project to wemade's pipeline with a school code and owner based on the files names which it gets it from datastore and logs the results on `submitter-log*.json`

### Pre-requisistes

- Install Nodejs
- run `$npm install` on the root of the folder
- Have a google json credential set up from a service account, checkout documentation [here](https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-nodejs)

## wm-xls-report-creator.js

Run it with `node wm-xls-report-creator.js`

- Creates an ACD and merge push xls report from the `report-input.json` which is based on the output of the wm-json-file-submitter, logs the reports to a `report-log*.json` file
  There is an an example on the `report-input.json` currently, but the best way to get it is to push the files yourself and copy the `wm-file-submitter-log` contents into it

### Pre-requisistes

- Install Nodejs
- run `$npm install` on the root of the folder
