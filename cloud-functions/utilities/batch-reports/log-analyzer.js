const fs = require("fs");
if (process.argv.length === 2) {
  console.error("Expected at least one argument!");
  process.exit(1);
}
let rawdata = fs.readFileSync(process.argv[2]);
let logfile = JSON.parse(rawdata);
let emtpyReports = logfile.filter(record => record.Columns === null);
console.log(`Files processed ${logfile.length}`);
console.log(`Files with error ${emtpyReports.length}`);
console.log(`Files submitted ${logfile.length - emtpyReports.length}`);
