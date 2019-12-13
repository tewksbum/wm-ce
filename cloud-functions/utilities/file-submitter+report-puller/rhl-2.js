const sql = require('mssql');
const fs = require('fs');
const parse = require('csv-parse/lib/sync')
const config = require( "./config.json" );

const folder = "/mnt/p/temp/ocm/rhl3";
const badfolder = "/mnt/p/temp/ocm/rhl.bad";


findFile = function(dir)
{
    var jsFiles = [];
    fs.readdirSync(dir).forEach(function(file) {
        jsFiles.push(file);
    });
    return jsFiles;
};

(async () => {
    let sqlconn = await sql.connect(config.mssql);
    
    var files = findFile(folder);
    console.log(files);

    for (const file of files) {
        if (!file.startsWith("RHL")) {
            continue;
        }
        var dataFile = folder + "/" + file;
        var contents = fs.readFileSync(dataFile, 'utf8');

        //var csvData = parse(contents, { bom: true, quote: '', ltrim: true, rtrim: true, delimiter: '\t' });
        var csvData = parse(contents, { bom: true, delimiter: '\t', quoting: true, trim: true, columns: true });
        //console.log(csvData[0]);

        if (csvData.length > 0) {
            if (!csvData[0]["SCHOOL CODE"] && !csvData[0]["FIRST NAME"]) {
                fs.renameSync(dataFile, badfolder + "/" + file);
                continue;
            }
            for (var i = 1; i < csvData.length; i++) {
                // await console.log(sqlrun);
                const request = new sql.Request();
                var schoolcode = file.substring(4, 3);
                if (csvData[i]["SCHOOL CODE"]) {
                    schoolcode = csvData[i]["SCHOOL CODE"];
                }
                request.input('p1', csvData[i]["SALUTATION SLUG"]);
                request.input('p2', csvData[i]["FIRST NAME"]);
                request.input('p3', csvData[i]["LAST NAME"]);
                request.input('p4', csvData[i]["STREET ADDRESS 1"]);
                request.input('p5', csvData[i]["STREET ADDRESS 2"]);
                request.input('p6', csvData[i]["CITY"]);
                request.input('p7', csvData[i]["STATE"]);
                request.input('p8', csvData[i]["ZIPCODE"]);
                request.input('p9', schoolcode);
                request.input('p10', csvData[i]["PROGRAM"]);
                request.input('p11', csvData[i]["MAILER TYPE"]);
                request.input('p12', csvData[i]["FILE OUTPUT DATE"]);
                request.input('p13', csvData[i]["CRM"]);
                request.input('p14', csvData[i]["COM"]);
                request.input('p15', csvData[i]["GENDER"]);
                request.input('p16', csvData[i]["DISTRIBUTION DESIGNATION"]);
                request.input('p17', csvData[i]["Q DISTRIBUTION"]);
                request.input('p18', csvData[i]["B DISTRIBUTION"]);
                request.input('p19', csvData[i]["C DISTRIBUTION"]);
                request.input('p20', file);

                await request.query("insert into linen ([SALUTATION SLUG],[FIRST NAME],[LAST NAME],[STREET ADDRESS 1],[STREET ADDRESS 2],[CITY],[STATE],[ZIPCODE],[SCHOOL CODE],[PROGRAM],[MAILER TYPE],[FILE OUTPUT DATE],[CRM],[COM],[GENDER],[DISTRIBUTION DESIGNATION],[Q DISTRIBUTION],[B DISTRIBUTION],[C DISTRIBUTION],[filename]) values (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20)");
            }
            console.log("done with " + file);
        }

        // fs.createReadStream(dataFile)
        //     //.pipe(parse({from: 2, relax_column_count: true, skip_empty_lines: true}))
        //     .pipe(parse({ bom: true, relax: true, from: 2, quote: '"', ltrim: true, rtrim: true, delimiter: '\t' }))
        //     .on('data', function(csvrow) {
        //         csvData.push(csvrow);
        //     })
        //     .on('end', async function() {
        //         console.log(file); 
        //         if (csvData.length > 0) {
        //             for (var i = 0; i < csvData.length; i++) {
        //                 var sqlrun = `insert into linen ([SALUTATION SLUG],[FIRST NAME],[LAST NAME],[STREET ADDRESS 1],[STREET ADDRESS 2],[CITY],[STATE],[ZIPCODE],[SCHOOL CODE],[PROGRAM],[MAILER TYPE],[FILE OUTPUT DATE],[CRM],[COM],[GENDER],[DISTRIBUTION DESIGNATION],[Q DISTRIBUTION],[B DISTRIBUTION],[C DISTRIBUTION],[filename]) values (${csvData[i][0]}, ${csvData[i][1]}, ${csvData[i][2]}, ${csvData[i][3]}, ${csvData[i][4]}, ${csvData[i][5]}, ${csvData[i][6]}, ${csvData[i][7]}, ${csvData[i][8]}, ${csvData[i][9]}, ${csvData[i][10]}, ${csvData[i][11]}, ${csvData[i][12]}, ${csvData[i][13]}, ${csvData[i][14]}, ${csvData[i][15]}, ${csvData[i][16]}, ${csvData[i][17]}, ${csvData[i][18]}, ${file}')`;
        //                 await console.log(sqlrun);
        //                 await sql.query(sqlrun);
        //             }
        //         }
        //         console.log(csvData);
        //     });
    }    


})();

sql.on('error', err => {
    console.log(err);
})