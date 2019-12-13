const fs = require("fs");
if (process.argv.length === 2) {
  console.error("Expected at least one argument!");
  process.exit(1);
}
let rawdata = fs.readFileSync(process.argv[2]);
let logfile = JSON.parse(rawdata);
let filesWithError = logfile.filter(record => record.error);
console.log(`Files processed ${logfile.length}`);
console.log(`Files with error ${filesWithError.length}`);
console.log(`Files submitted ${logfile.length - filesWithError.length}`);
