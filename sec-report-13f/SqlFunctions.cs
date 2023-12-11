using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Logging;

namespace MakeReport13F
{
    public static class SqlFunctions
    {
        private static readonly string SqlConnectionString = Environment.GetEnvironmentVariable("string_sqldb_information").ToString();

        public static string SelectId(ILogger log)
        {
            string id = string.Empty;

            try
            {
                using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(@"SELECT TOP 1 [ReportId]
                                FROM [Sec].[QueuedReportIds]
                                WHERE [ReportType] = '13F'
                                ORDER BY [ReportId]");

                    string sql = sb.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        connection.Open();

                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                id = reader.GetString(0);
                            }
                        }
                        connection.Close();
                    }
                }

                log.LogInformation("SelectId succeded.");

            }
            catch (SqlException ex)
            {
                log.LogInformation($"SelectReportIds failed. Exception: {ex}");
            }

            return id;

        }

        public static void DeleteId(string id, ILogger log)
        {
            string sqlInput = $"DELETE FROM [Sec].[QueuedReportIds] WHERE [ReportType] = '13F' AND [ReportId] = '{id}'";

            CommitToDB(sqlInput, log);
        }

        public static bool CommitToDB(string sqlInput, ILogger log)
        {
            bool success = false;

            try
            {
                using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(sqlInput);

                    string sql = sb.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        connection.Open();
                        command.CommandTimeout = 600; //10 minutes
                        command.ExecuteReader();
                        connection.Close();
                    }
                }

                success = true;

                log.LogInformation("CommitToDB succeded.");

            }
            catch (SqlException ex)
            {
                log.LogError($"CommitToDB failed. Exception: {ex}");
                log.LogInformation(sqlInput);
            }

            return success;
        }

        public static bool CommitReport(string reportId, HF hf, ILogger log)
        {
            bool success;
            string sqlInput;

            if (hf != null)
            {
                sqlInput = "INSERT INTO [Sec].[Report13F]([RowGuid],[ReportId],[SubmissionType],[LiveTestFlag],[ConfirmingCopyFlag],[Cik],[Ccc],[PeriodOfReport],[Quarter],[Name],[IsAmendment],[Form13FFileNumber],[Signature],[SignatureDate],[OtherIncludedManagersCount],[TableEntryTotal],[TableValueTotal],[PublishedDate])"
                                + "VALUES " + hf.HFToSql();

                success = CommitToDB(sqlInput, log);

                if (success && hf.Positions.Count > 0)
                {
                    success = false;

                    List<List<string>> chunks = ChunkBy(hf.HFPositionsToSql(), 200);

                    foreach(List<string> chunk in chunks)
                    {
                        string sqlBatch = "INSERT INTO [Sec].[HFPositions]([ReportGuid],[ReportId],[NameOfIssuer],[TitleOfClass],[Cusip],[Value],[SshPrnamt],[SshPrnamtType],[InvestmentDiscretion],[Sole],[Shared],[None])"
                                    + "VALUES " + string.Join(",", chunk);
                        
                        success = CommitToDB(sqlBatch, log);

                        if (!success)
                        {
                            sqlInput = $"INSERT INTO [Sec].[ProblematicReportIds]([ReportType],[ReportId]) VALUES ('13F','{hf.ReportId}')";
                            CommitToDB(sqlInput, log);
                            break;
                        }
                    }
                }
            }
            else
            {
                sqlInput = $"INSERT INTO [Sec].[EmptyReportIds]([ReportType],[ReportId]) VALUES ('13F','{reportId}')";

                success = CommitToDB(sqlInput, log);

                log.LogInformation($"Processed id {reportId} without content.");
            }

            return success;
        }

        public static List<List<T>> ChunkBy<T>(List<T> source, int chunkSize)
        {
            return source
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / chunkSize)
                .Select(x => x.Select(v => v.Value).ToList())
                .ToList();
        }

    }
}
