const path = require('path');
const fs = require('fs');
const xlsx = require('xlsx');
const request = require('sync-request');
const {Storage} = require('@google-cloud/storage');
const {Client} = require('@elastic/elasticsearch')
const flatten = require('flat');
// const config = require( "./config.json" );

//const folder = "/mnt/z-drive";
const folder = "/mnt/p/temp/ocm/pipeline/fep";
const output = "/mnt/p/temp/ocm/pipeline";
const result = "/mnt/p/temp/ocm/pipeline/result";
const bucket = "ocm_school_raw_files"
const streamer = "https://us-central1-wemade-core.cloudfunctions.net/streamer-api-dev";
const storage = new Storage();
const es = new Client({ node: 'http://104.198.136.122:9200', auth: {
    username: 'elastic',
    password: 'TsLv8BtM'
  } })
//const output = "/mnt/p/temp";


findFile = function(dir)
{
    var jsFiles = [];
    fs.readdirSync(dir).forEach(function(file) {
        filepath = path.resolve(dir, file);
        filestat = fs.lstatSync(filepath);
        if (filestat.isDirectory()) {
            if (file != "Cleaned Files" && file != "CP Uploads") {
                nestedFiles = findFile(filepath);
                jsFiles = jsFiles.concat(nestedFiles);
            }
        }
        else {
            jsFiles.push(filepath);
        }
    });
    return jsFiles;
};

(async () => {
    
    var files = findFile(folder);
    
    for (const file of files) {
        if (!file.endsWith(".xlsx")) {
            continue;
        }

        console.log("Processing " + file);

        var workbook = xlsx.readFile(file);
        var basename = path.basename(file);
        
        var sheetName = workbook.SheetNames[0];
        var worksheet = workbook.Sheets[sheetName];
        var aoa = xlsx.utils.sheet_to_json(worksheet);
        var ws = xlsx.utils.json_to_sheet(aoa.slice(0, 9));

        // locate the cp sheet
        var cpsheet = null;
        for (var name in workbook.SheetNames) {
            if (workbook.SheetNames[name].toUpperCase() == "CP UPLOAD") {
                cpsheet = workbook.Sheets[workbook.SheetNames[name]];
                break;
            }
        }        
        var cp = null;
        if (cpsheet) {
            var cpa = xlsx.utils.sheet_to_json(cpsheet);
            cp = xlsx.utils.json_to_sheet(cpa.slice(0, 9));
        }

        var wb = xlsx.utils.book_new();
        xlsx.utils.book_append_sheet(wb, ws, "Worksheet");
        var xlsxfile = basename.substr(0, 7) + ".xlsx";
        var newfile = path.join(output, xlsxfile);
        xlsx.writeFile(wb, newfile);
        var bucketfile = "/drop/test/input/" + xlsxfile;
        await storage.bucket(bucket).upload(newfile, {
            destination: bucketfile,
            contentType: "application/octet-stream"
        });       
        
        // call endpoint to post it
        var streamerData = {
            accessKey: "4ZFGVumXw9043yH1SKFd9vubWHxMBAt3",
            fileUrl: "https://storage.googleapis.com/ocm_school_raw_files/drop/test/input/" + xlsxfile,
            source: "fep",
            organization: "BRLK"
        };

        var res = request("POST", streamer, {json: streamerData });        
        var streamerResponse = res.getBody('utf8');
        console.log(streamerResponse);
        var streamerResponseObj = eval('(' + streamerResponse + ')');
        
        if (streamerResponseObj.success) {
            var requestId = streamerResponseObj.id;
            console.log(requestId);

            await sleep(60000);
            const { body } = await es.search({
                index: 'people',
                body: {
                  query: {
                    match: {
                        Request: requestId
                    }
                  },
                  sort: ["Row"]

                }
            });
            
            console.log(body.hits.hits);
            var hits = body.hits.hits;
            var esdata = [];
            for(var h in hits) {
                esdata.push(flatten(hits[h]._source));
            }
            console.log(esdata);

            // let's write result
            var wbr = xlsx.utils.book_new();
            xlsx.utils.book_append_sheet(wbr, ws, "Input");
            xlsx.utils.book_append_sheet(wbr, cp, "Human");
            var ml = xlsx.utils.json_to_sheet(esdata);
            xlsx.utils.book_append_sheet(wbr, ml, "Machine");

            bucketfile = "/drop/test/output/" + xlsxfile;
            newfile = path.join(result, xlsxfile);
            xlsx.writeFile(wbr, newfile);
            await storage.bucket(bucket).upload(newfile, {
                destination: bucketfile
            });       
            
        }

        process.exit();
    }    
    process.exit();


})();

// sql.on('error', err => {
//     console.log(err);
// })
function sleep(ms){
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}