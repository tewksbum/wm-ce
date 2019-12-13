const spsc = require("./sponsorSchoolsProd.json");
const fs = require("fs");
var output = {};
spsc.sponsorSchools.forEach(element => {
  if (output[element.school_code]) {
    output[element.school_code].push(element.tag);
  } else {
    output[element.school_code] = [element.tag];
  }
});
fs.writeFileSync("./data.json", JSON.stringify(output, null, 2), "utf-8");
