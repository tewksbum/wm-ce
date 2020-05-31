using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Linq;
using Google.Cloud.Diagnostics.AspNetCore;

namespace Wemade.Functions
{
    public static class ExcelParser
    {
        [FunctionName("ExcelParser")]
        public static async Task<IActionResult> Run( [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req, ILogger log)
        {

            string fileUrl = req.Query["fileUrl"];
            log.LogInformation($"processing {fileUrl}");
            var downloadRequest = WebRequest.Create(fileUrl);
            var downloadResponse = await downloadRequest.GetResponseAsync();
            Stream downloadStream = downloadResponse.GetResponseStream();

            List<List<string>> data = new List<List<string>>();

            string responseMessage = "";

            try
            {
                using (SpreadsheetDocument document = SpreadsheetDocument.Open(downloadStream, false))
                {
                    WorkbookPart workbook = document.WorkbookPart;
                    foreach (var worksheetPart in workbook.WorksheetParts)
                    {
                        Worksheet worksheet = worksheetPart.Worksheet;
                        foreach (var row in worksheet.GetFirstChild<SheetData>().Elements<Row>())
                        {
                            List<string> cells = new List<string>();
                            foreach (var cell in row.Elements<Cell>())
                            {
                                cells.Add(GetCellValue(cell));
                            }
                            data.Add(cells);
                        }
                    }
                    responseMessage = JsonConvert.SerializeObject(data);
                }
            }
            catch (Exception ex)
            {
                responseMessage = JsonConvert.SerializeObject(new {error = ex.Message, stack = ex.StackTrace });
                log.LogWarning($"error encountered: {responseMessage}");

            }

            log.LogWarning($"total memory used: {System.GC.GetTotalAllocatedBytes() / 1024 / 1024}MB" );


            return new OkObjectResult(responseMessage);
        }

        public static string GetCellValue(Cell cell)
        {
            if (cell == null)
                return "";
            if (cell.DataType == null)
                return cell.InnerText;

            string value = cell.InnerText;
            switch (cell.DataType.Value)
            {
                case CellValues.SharedString:
                    // For shared strings, look up the value in the shared strings table.
                    // Get worksheet from cell
                    OpenXmlElement parent = cell.Parent;
                    while (parent.Parent != null && parent.Parent != parent
                            && string.Compare(parent.LocalName, "worksheet", true) != 0)
                    {
                        parent = parent.Parent;
                    }
                    if (string.Compare(parent.LocalName, "worksheet", true) != 0)
                    {
                        throw new Exception("Unable to find parent worksheet.");
                    }

                    Worksheet ws = parent as Worksheet;
                    SpreadsheetDocument ssDoc = ws.WorksheetPart.OpenXmlPackage as SpreadsheetDocument;
                    SharedStringTablePart sstPart = ssDoc.WorkbookPart.GetPartsOfType<SharedStringTablePart>().FirstOrDefault();

                    // lookup value in shared string table
                    if (sstPart != null && sstPart.SharedStringTable != null)
                    {
                        value = sstPart.SharedStringTable.ElementAt(int.Parse(value)).InnerText;
                    }
                    break;

                //this case within a case is copied from msdn. 
                case CellValues.Boolean:
                    switch (value)
                    {
                        case "0":
                            value = "FALSE";
                            break;
                        default:
                            value = "TRUE";
                            break;
                    }
                    break;
            }
            return value;
        }
    }
}
