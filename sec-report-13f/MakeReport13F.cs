using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;

namespace MakeReport13F
{
    public static class MakeReport13F
    {
        public static readonly string StorageConnectionString = Environment.GetEnvironmentVariable("string_stsgsecreportsprodde");
        public static readonly int PageNumber = int.Parse(Environment.GetEnvironmentVariable("int_pagenumber_13f"));

        [FunctionName("Driver")]
        public static async Task RunOrchestrator([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var masterInput = context.GetInput<MasterInput>();

            var webIds = Helpers.FetchIds(masterInput.PageNumber, 0, log);

            if (webIds.Count > 0)
            {
                log.LogInformation($"Ids online: {webIds.Count}");

                var parallelTasks = new List<Task>();

                foreach (var webId in webIds)
                {
                    if (!masterInput.Ids.Contains(webId))
                    {
                        try
                        {
                            var task = context.CallActivityAsync("Worker", JsonConvert.SerializeObject(new RequestObject(webId)));
                            parallelTasks.Add(task);
                        }
                        catch (Exception ex)
                        {
                            log.LogError(ex.ToString());
                        }
                    }
                }

                log.LogInformation($"Unknown Ids: {parallelTasks.Count}");

                await Task.WhenAll(parallelTasks);
            }
        }

        [FunctionName("Worker")]
        public static async Task GenerateStorageQueueActivity([ActivityTrigger] string requestObject)
        {
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference("queue4requests-13f");
            await queue.CreateIfNotExistsAsync();

            var cloudQueueMessage = new CloudQueueMessage(requestObject);
            await queue.AddMessageAsync(cloudQueueMessage);
        }

        [FunctionName("Trigger")]
        public static async Task Run([TimerTrigger("0 */15 * * * *")] TimerInfo myTimer,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            log.LogInformation($"C# trigger function execution saterted at: {DateTime.Now}");

            int pageNumber = Helpers.GetPageNumber(log);

            int newPageNumber = pageNumber < PageNumber ? pageNumber + 1 : 1;

            Helpers.UpdatePageNumber(newPageNumber, log);

            var ids = Helpers.SelectReportIds(log);

            MasterInput masterInput = new MasterInput(ids, pageNumber);

            string instanceId = await starter.StartNewAsync("Driver", null, masterInput);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            log.LogInformation($"C# trigger function execution completed at: {DateTime.Now}");
        }

        [FunctionName("Listener")]
        public static void Write([QueueTrigger("queue4requests-13f")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");

            try
            {
                RequestObject requestObject = JsonConvert.DeserializeObject<RequestObject>(myQueueItem);

                string sqlInput = $"INSERT INTO [Sec].[QueuedReportIds]([ReportType],[ReportId]) VALUES ('13F','{requestObject.Id}')";

                SqlFunctions.CommitToDB(sqlInput, log);
            }
            catch (Exception ex)
            {
                log.LogError($"Failed to deserialize queue object. Exception: {ex}.");
            }
        }

        [FunctionName("PosionIdToDB")]
        public static void SafetyNet([QueueTrigger("queue4requests-13f-poison")] string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");

            try
            {
                RequestObject requestObject = JsonConvert.DeserializeObject<RequestObject>(myQueueItem);

                string sqlInput = $"INSERT INTO [Sec].[QueuedReportIds]([ReportType],[ReportId]) VALUES ('13F','{requestObject.Id}')";

                SqlFunctions.CommitToDB(sqlInput, log);
            }
            catch (Exception ex)
            {
                log.LogError($"Failed to deserialize queue object. Exception: {ex}.");
            }
        }

        [FunctionName("Maker")]
        public static void Execute([TimerTrigger("*/15 * * * * *", UseMonitor = false)] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# trigger function execution saterted at: {DateTime.Now}");

            string reportId = SqlFunctions.SelectId(log);

            log.LogInformation($"Processing report Id {reportId}");

            if (!string.IsNullOrEmpty(reportId))
            {
                Guid rowGuid = Guid.NewGuid();

                log.LogInformation($"Created guid {rowGuid} for report Id");

                HF hf = Report.GetDataFromReport(rowGuid, reportId);

                SqlFunctions.CommitReport(reportId, hf, log);

                SqlFunctions.DeleteId(reportId, log);
            }

            log.LogInformation($"C# trigger function execution completed at: {DateTime.Now}");
        }
    }
}
