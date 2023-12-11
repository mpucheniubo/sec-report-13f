using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using HtmlAgilityPack;

namespace MakeReport13F
{
    public class HF
    {
        public Guid RowGuid { get; set; }
        public string ReportId { get; set; }
        public string SubmissionType { get; set; }                    // nameOfIssuer                         e.g. APPLE INC
        public string LiveTestFlag { get; set; }                // titleOfClass                      e.g. COM (company)
        public string ConfirmingCopyFlag { get; set; }              // CUSIP                     e.g. 037833100
        public string Cik { get; set; }             // 11801                 e.g. 11801
        public string Ccc { get; set; }          // sshPrnamt                          e.g. 96607
        public DateTime PeriodOfReport { get; set; }              // sshPrnamtType                    e.g. SH
        public string Quarter
        {
            get
            {
                string year = "0000" + PeriodOfReport.Year.ToString();
                string quarter = ((PeriodOfReport.Month + 2) / 3).ToString();
                return year.Substring(year.Length - 4, 4) + "Q" + quarter;
            }
        }
        public string Name { get; set; }       // <name>SHEETS SMITH WEALTH MANAGEMENT</name>     => HEADGEFOND NAME           
        public bool IsAmendment { get; set; }          // Sole
        public string Form13FFileNumber { get; set; }        //Shared                   e.g. 0
        public string Signature { get; set; }
        public DateTime SignatureDate { get; set; }
        public long OtherIncludedManagersCount { get; set; }
        public long TableEntryTotal { get; set; }
        public long TableValueTotal { get; set; }
        public DateTime PublishedDate { get; set; }

        public List<HFPosition> Positions { get; set; }

        public HF(Guid rowGuid, string reportId)
        {
            RowGuid = rowGuid;
            ReportId = reportId;
            SubmissionType = string.Empty;
            LiveTestFlag = string.Empty;
            ConfirmingCopyFlag = string.Empty;
            Cik = string.Empty;
            Ccc = string.Empty;
            PeriodOfReport = DateTime.MinValue;
            Name = string.Empty;
            IsAmendment = false;
            Form13FFileNumber = string.Empty;
            Signature = string.Empty;
            SignatureDate = DateTime.MinValue;
            OtherIncludedManagersCount = 0;
            TableEntryTotal = 0;
            TableValueTotal = 0;
            PublishedDate = DateTime.MinValue;

            Positions = new List<HFPosition>();
        }

        public string HFToSql()
        {
            string toSql =  $"('{RowGuid}','{ReportId}','{SubmissionType}','{LiveTestFlag}','{ConfirmingCopyFlag}','{Cik}','{Ccc}','{PeriodOfReport.ToString("yyyy-MM-dd")}','{Quarter}','{Name}',{(IsAmendment ? 1 : 0)},'{Form13FFileNumber}','{Signature}','{SignatureDate.ToString("yyyy-MM-dd")}',{OtherIncludedManagersCount},{TableEntryTotal},{TableValueTotal},'{PublishedDate.ToString("yyyy-MM-dd HH:mm:ss")}')";

            return toSql;
        }

        public List<string> HFPositionsToSql()
        {
            List<string> sqlValues = new List<string>();

            foreach (HFPosition hFPosition in Positions)
            {
                sqlValues.Add(hFPosition.PositionToSql());
            }

            return sqlValues;
        }
    }

    public class HFPosition
    {
        public Guid ReportGuid { get; set; }
        public string ReportId { get; set; }
        public string NameOfIssuer { get; set; }                    // nameOfIssuer                         e.g. APPLE INC
        public string TitleOfClass { get; set; }                // titleOfClass                      e.g. COM (company)
        public string Cusip { get; set; }              // CUSIP                     e.g. 037833100
        public long Value { get; set; }             // 11801                 e.g. 11801
        public long SshPrnamt { get; set; }          // sshPrnamt                          e.g. 96607
        public string SshPrnamtType { get; set; }              // sshPrnamtType                    e.g. SH
        public string InvestmentDiscretion { get; set; }        // investmentDiscretion         e.g. SOLE                        
        public long Sole { get; set; }          // Sole
        public long Shared { get; set; }        //Shared                   e.g. 0
        public long None { get; set; }

        public HFPosition(Guid reportGuid, string reportId)
        {
            ReportGuid = reportGuid;
            ReportId = reportId;
            NameOfIssuer = string.Empty;
            TitleOfClass = string.Empty;
            Cusip = string.Empty;
            Value = 0;
            SshPrnamt = 0;
            SshPrnamtType = string.Empty;
            InvestmentDiscretion = string.Empty;
            Sole = 0;
            Shared = 0;
            None = 0;
        }

        public string PositionToSql()
        {
            string toSql = $"('{ReportGuid}','{ReportId}','{NameOfIssuer}','{TitleOfClass}','{Cusip}',{Value},{SshPrnamt},'{SshPrnamtType}','{InvestmentDiscretion}',{Sole},{Shared},{None})";

            return toSql;
        }
    }

    public static class Report
    {
        public static HF GetDataFromReport(Guid reportGuid, string id)
        {
            HF hf = null;

            var url = "https://sec.report/Document/" + id + "/#primary_doc.xml";
            var web = new HtmlWeb();
            var doc = web.Load(url);

            string strURLHeader = string.Empty;

            bool flag = false;

            try
            {
                strURLHeader = doc.DocumentNode.SelectSingleNode("//a[@title='primary_doc.xml']").GetAttributeValue("href", string.Empty);
            }
            catch
            {
                // too old: https://sec.report/Document/0000909012-09-000357/#primary_doc.xml     format from 2009
                // or https://sec.report/Document/0001641761-19-000008/#primary_doc.xml     other format
                strURLHeader = "https://sec.report/Document/" + id + "/primary_doc.xml";
                flag = true;
            }

            // submission date

            CultureInfo enUS = new CultureInfo("en-US");

            DateTime publishedDate = DateTime.MinValue;
            bool hasPublishedDate = false;
            try
            {
                hasPublishedDate = DateTime.TryParseExact(doc.DocumentNode.SelectSingleNode("//abbr[@class='published']").InnerHtml, "yyyy-MM-dd HH:mm:ss", enUS, DateTimeStyles.None, out publishedDate);
            }
            catch(Exception ex)
            {

            }

            string strURL = string.Empty;
            if (!flag)
            {
                strURL = doc.ParsedText;
                int ii = strURL.IndexOf("-table.xml");
                strURL = strURL.Substring(ii + 10);
                ii = strURL.IndexOf(">");
                strURL = strURL.Substring(ii + 1);
                ii = strURL.IndexOf("<");
                strURL = strURL.Substring(0, ii);
            }
            else
            {
                // Dann muss man noch mal extra den link zu den positionen suchen
                strURL = doc.ParsedText;
                int ii = strURL.IndexOf("INFORMATION TABL");
                if (ii != -1)
                {
                    strURL = strURL.Substring(ii + 10);
                    ii = strURL.IndexOf(">");
                    strURL = strURL.Substring(ii + 1);
                    ii = strURL.IndexOf("<");
                    strURL = strURL.Substring(0, ii).Replace(".html", ".xml");
                }
                else
                {
                    strURL = strURLHeader;
                }
            }

            if (strURL == "\n")
            {
                // manche sind echt hässlich ...
                strURL = doc.ParsedText;
                int ii = strURL.IndexOf("Complete submission text file");
                strURL = strURL.Substring(ii + 10);
                ii = strURL.IndexOf("sec.report/Document/" + id);
                strURL = strURL.Substring(ii + 10);
                ii = strURL.IndexOf(">");
                strURL = strURL.Substring(ii + 1);
                ii = strURL.IndexOf("<");
                strURL = strURL.Substring(0, ii).Replace(".html", ".xml");
            }

            string URLStringHeader = strURLHeader; // "https://sec.report/Document/0001398344-21-008475/primary_doc.xml";

            bool hasDataSet = true;

            DataSet dsHeader = new DataSet();
            try
            {
                dsHeader.ReadXml(URLStringHeader);
            }
            catch (System.Xml.XmlException e)
            {
                hasDataSet = false;
            }

            if (hasDataSet)
            {
                DataTable headerData = dsHeader.Tables[0];

                bool hasTableName = true;

                if (headerData.TableName == "Error")
                {
                    hasTableName = false;
                }

                if (hasTableName)
                {
                    DataTable filerInfo = new DataTable();
                    DataTable flags = new DataTable();
                    DataTable filer = new DataTable();
                    DataTable credentials = new DataTable();
                    DataTable formData = new DataTable();
                    DataTable coverPage = new DataTable();
                    DataTable filingManager = new DataTable();
                    DataTable address = new DataTable();
                    DataTable signatureBlock = new DataTable();
                    DataTable summaryPage = new DataTable();

                    bool hasSufficientData = true;
                    
                    if (dsHeader.Tables.Count == 13)
                    {
                        filerInfo = dsHeader.Tables[1];
                        filer = dsHeader.Tables[2];
                        credentials = dsHeader.Tables[3];
                        formData = dsHeader.Tables[4];
                        coverPage = dsHeader.Tables[5];
                        filingManager = dsHeader.Tables[6];
                        address = dsHeader.Tables[7];
                        signatureBlock = dsHeader.Tables[8];
                        summaryPage = dsHeader.Tables[9];
                    }
                    else if (dsHeader.Tables.Count == 15)
                    {
                        // this case is new - double check later if works
                        filerInfo = dsHeader.Tables[1];
                        flags = dsHeader.Tables[2];
                        filer = dsHeader.Tables[3];
                        credentials = dsHeader.Tables[4];
                        formData = dsHeader.Tables[5];
                        coverPage = dsHeader.Tables[6];
                        filingManager = dsHeader.Tables[7];
                        address = dsHeader.Tables[8];
                        signatureBlock = dsHeader.Tables[11];
                        summaryPage = dsHeader.Tables[12];
                    }
                    else if (dsHeader.Tables.Count > 10)
                    {
                        filerInfo = dsHeader.Tables[1];
                        flags = dsHeader.Tables[2];
                        filer = dsHeader.Tables[3];
                        credentials = dsHeader.Tables[4];
                        formData = dsHeader.Tables[5];
                        coverPage = dsHeader.Tables[6];
                        filingManager = dsHeader.Tables[7];
                        address = dsHeader.Tables[8];
                        signatureBlock = dsHeader.Tables[9];
                        summaryPage = dsHeader.Tables[10];
                    }
                    else if (dsHeader.Tables.Count == 10)
                    {
                        filerInfo = dsHeader.Tables[1];
                        filer = dsHeader.Tables[2];
                        credentials = dsHeader.Tables[3];
                        formData = dsHeader.Tables[4];
                        coverPage = dsHeader.Tables[5];
                        filingManager = dsHeader.Tables[6];
                        address = dsHeader.Tables[7];
                        signatureBlock = dsHeader.Tables[8];
                        summaryPage = dsHeader.Tables[9];
                    }
                    else
                    {
                        // e.g. https://www.sec.gov/Archives/edgar/data/1681822/000095012320007903/primary_doc.xml 
                        hasSufficientData = false;
                    }

                    if (hasSufficientData)
                    {
                        hf = new HF(reportGuid, id);

                        bool hasLength = true;

                        DateTime periodOfReport;
                        DateTime signatureDate = DateTime.MinValue;

                        hf.SubmissionType = headerData.Rows[0].ItemArray[0].ToString().Replace("'", "''");
                        hf.LiveTestFlag = filerInfo.Rows[0].ItemArray[0].ToString().Replace("'", "''");
                        if (flags.Rows.Count > 0)
                            hf.ConfirmingCopyFlag = flags.Rows[0].ItemArray[0].ToString().Replace("'", "''");
                        hf.Cik = credentials.Rows[0].ItemArray[0].ToString().Replace("'", "''");
                        if (credentials.Rows[0].ItemArray.Length > 1)
                            hf.Ccc = credentials.Rows[0].ItemArray[1].ToString().Replace("'", "''");

                        bool hasPeriodOfRepor = DateTime.TryParseExact(filerInfo.Rows[0].ItemArray[2].ToString(), "MM-dd-yyyy", enUS, DateTimeStyles.None, out periodOfReport);

                        if (coverPage.Rows[0].ItemArray.Length < 2)
                        {
                            hasLength = false;
                        }

                        if (hasLength && hasPeriodOfRepor)
                        {
                            hf.PeriodOfReport = periodOfReport;
                            hf.IsAmendment = Convert.ToBoolean(coverPage.Rows[0].ItemArray[1]);
                            if(coverPage.Rows[0].ItemArray.Length>4)
                                hf.Form13FFileNumber = coverPage.Rows[0].ItemArray[4].ToString().Replace("'", "''");
                            hf.Name = filingManager.Rows[0].ItemArray[0].ToString().Replace("'", "''");
                            hf.Signature = signatureBlock.Rows[0].ItemArray[0].ToString().Replace("'", "''");

                            bool hasSignatureDate = false;
                            
                            try
                            {
                                if (signatureBlock.Rows[0].ItemArray.Length > 6)
                                    hasSignatureDate = DateTime.TryParseExact(signatureBlock.Rows[0].ItemArray[6].ToString(), "MM-dd-yyyy", enUS, DateTimeStyles.None, out signatureDate);
                                if (hasSignatureDate)
                                {
                                    hf.SignatureDate = signatureDate;
                                }
                            }
                            catch (Exception ex)
                            {
                            }

                            bool hasCorrectFormat = true;

                            if (!(summaryPage.Rows[0].ItemArray[0].ToString() == string.Empty))
                            {
                                long temp;
                                bool isCorrentFormat = Int64.TryParse(summaryPage.Rows[0].ItemArray[0].ToString(), out temp);

                                if (isCorrentFormat && (summaryPage.Rows[0].ItemArray.Length > 2))
                                {
                                    hf.OtherIncludedManagersCount = Convert.ToInt64(summaryPage.Rows[0].ItemArray[0]);
                                    if (summaryPage.Rows[0].ItemArray[1].ToString().Contains("-"))
                                    {
                                        hasCorrectFormat = false;
                                    }
                                    else
                                    {
                                        hf.TableEntryTotal = Convert.ToInt64(summaryPage.Rows[0].ItemArray[1]);
                                        hf.TableValueTotal = Convert.ToInt64(summaryPage.Rows[0].ItemArray[2]);
                                    }
                                }
                                else
                                {
                                    hf.OtherIncludedManagersCount = 0;
                                    hf.TableEntryTotal = 0;
                                    hf.TableValueTotal = 0;
                                }
                            }
                            else
                            {
                                hf.OtherIncludedManagersCount = 0;
                                hf.TableEntryTotal = 0;
                                hf.TableValueTotal = 0;
                            }
                                

                            if (hasPublishedDate)
                            {
                                hf.PublishedDate = publishedDate;
                            }

                            string URLString = "https://sec.report/Document/" + id + "/" + strURL.Replace("\\n", "").Replace("/n", "");

                            DataSet ds = new DataSet();

                            

                            try
                            {
                                ds.ReadXml(URLString);
                            }
                            catch
                            {
                                // wrong format??
                                try
                                {
                                    ds.ReadXml("https://sec.report/Document/" + id + "/infotable.xml");
                                }
                                catch
                                {
                                    hasCorrectFormat = false;
                                }
                            }

                            if (hasCorrectFormat)
                            {
                                if (ds.Tables.Count > 1)
                                {
                                    DataTable infoTable = ds.Tables[0];
                                    DataTable shrsOrPrnAmt = ds.Tables[1];
                                    DataTable votingAuthority = ds.Tables[2];

                                    for (int i = 0; i < infoTable.Rows.Count; i++)
                                    {
                                        HFPosition position = new HFPosition(reportGuid, id);
                                        position.NameOfIssuer = infoTable.Rows[i].ItemArray[0].ToString().Replace("'", "''");
                                        position.TitleOfClass = infoTable.Rows[i].ItemArray[1].ToString().Replace("'", "''");
                                        position.Cusip = infoTable.Rows[i].ItemArray[2].ToString().Replace("'", "''");
                                        position.Value = (long)Math.Round(Convert.ToDouble(infoTable.Rows[i].ItemArray[3]));
                                        position.SshPrnamt = (long)Math.Round(Convert.ToDouble(infoTable.Rows[i].ItemArray[4]));
                                        position.InvestmentDiscretion = infoTable.Rows[i].ItemArray[5].ToString().Replace("'", "''");

                                        position.SshPrnamt = (long)Math.Round(Convert.ToDouble(shrsOrPrnAmt.Rows[i].ItemArray[0]));
                                        position.SshPrnamtType = shrsOrPrnAmt.Rows[i].ItemArray[1].ToString().Replace("'", "''");

                                        position.Sole = (long)Math.Round(Convert.ToDouble(votingAuthority.Rows[i].ItemArray[0]));
                                        position.Shared = (long)Math.Round(Convert.ToDouble(votingAuthority.Rows[i].ItemArray[1]));
                                        position.None = (long)Math.Round(Convert.ToDouble(votingAuthority.Rows[i].ItemArray[2]));

                                        hf.Positions.Add(position);
                                    }
                                }
                                else
                                {
                                    // no positions
                                    hasCorrectFormat = false;
                                }
                            }
                        }
                    }
                }
            }

            return hf;
        }
    }
}
