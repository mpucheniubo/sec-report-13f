using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using Microsoft.Extensions.Logging;
using HtmlAgilityPack;
using System.Linq;
using System.Net;

namespace MakeReport13F
{
    public class Helpers
    {
        private static readonly string SqlConnectionString = Environment.GetEnvironmentVariable("string_sqldb_information").ToString();
        
        public static List<string> SelectReportIds(ILogger log)
        {
            List<string> ids = new List<string>();

            try
            {
                using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(@"SELECT [ReportId]
                                FROM [Sec].[Report13F]
                                UNION
                                SELECT [ReportId]
                                FROM [Sec].[EmptyReportIds]
                                WHERE [ReportType] = '13F'
                                UNION
                                SELECT [ReportId]
                                FROM [Sec].[ProblematicReportIds]
                                WHERE [ReportType] = '13F'
                                UNION
                                SELECT [ReportId]
                                FROM [Sec].[QueuedReportIds]
                                WHERE [ReportType] = '13F'
                                ");

                    string sql = sb.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        connection.Open();

                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ids.Add(reader.GetString(0));
                            }
                        }
                        connection.Close();
                    }
                }

            }
            catch (SqlException ex)
            {
                log.LogError($"SelectReportIds failed. Exception: {ex}");
            }

            return ids;

        }

        public static int GetPageNumber(ILogger log)
        {
            int pageNumber = 0;

            try
            {
                using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(@"SELECT TOP 1 [Value]
                                FROM [Sec].[RunningPageNumbers]
                                WHERE [ReportType] = '13F'
                                ORDER BY [RowUpdated] DESC");

                    string sql = sb.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        connection.Open();

                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                pageNumber = reader.GetInt32(0);
                            }
                        }
                        connection.Close();
                    }
                }

                log.LogInformation($"Fetched page number {pageNumber}.");

            }
            catch (SqlException ex)
            {
                log.LogError($"GetPageNumber failed. Exception: {ex}");
            }

            return pageNumber;
        }

        public static void UpdatePageNumber(int pageNumber, ILogger log)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append($"UPDATE [Sec].[RunningPageNumbers] SET [RowUpdated] = SYSUTCDATETIME(), [Value] = {pageNumber} WHERE [ReportType] = '13F'");

                    string sql = sb.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        connection.Open();
                        command.ExecuteReader();
                        connection.Close();
                    }
                }

                log.LogInformation($"Updated page number to {pageNumber}.");

            }
            catch (SqlException ex)
            {
                log.LogError($"UpdatePageNumber failed. Exception: {ex}");
            }
        }

        public static List<string> FetchIds(int pageNumber, int retry, ILogger log)
        {
            List<string> linkToDocuments = new List<string>();

            string searchURL = "https://sec.report/Document/Header/?formType=13F-HR&page=" + pageNumber.ToString();

            log.LogInformation(searchURL);

            try
            {
                var webSearch = new HtmlWeb();
                webSearch.PreRequest = delegate (HttpWebRequest webReq)
                {
                    webReq.Timeout = 120000; // 2 minutes
                    return true;
                };
                var docSearch = webSearch.Load(searchURL);

                var table = docSearch.DocumentNode.SelectSingleNode("//table[@class='table']");
                var htmlDoc = new HtmlDocument();
                htmlDoc.LoadHtml(table.OuterHtml);

                var linkList = htmlDoc.DocumentNode.SelectNodes("//a");

                if ((linkList == null) || (linkList.Count() == 0))
                {
                    log.LogWarning($"{searchURL} HAD ZERO LINKS");
                }
                else
                {
                    foreach (var link in linkList)
                    {
                        if (link.OuterHtml.Contains("Document"))
                        {
                            try
                            {
                                string[] webIdSplit = link.InnerHtml.Split('-');
                                int year = Convert.ToInt32(webIdSplit[1]);

                                if (year >= 15 && year <= 30)
                                {
                                    linkToDocuments.Add(link.InnerHtml);
                                }
                            }
                            catch(Exception ex)
                            {
                                log.LogError($"Reading Id {link.InnerHtml} failed. Ex: {ex}.");
                            }
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                log.LogError($"FetchIds failed. Exception: {ex}.");

                if(retry < 10)
                {
                    FetchIds(pageNumber, retry + 1, log);
                }
            }
            

            return linkToDocuments;
        }
    }
}
