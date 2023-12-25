using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace NL_Results_Loader.Class
{
    class DatabaseManagement
    {
        MySqlConnection mysqlCon = new MySqlConnection(ConfigurationManager.ConnectionStrings["connection"].ConnectionString);


        public List<Scheduler> GetAllSchedules()
        {
            List<Scheduler> lstSchedules = new List<Scheduler>();
            try
            {
                if (mysqlCon.State == ConnectionState.Closed)
                {
                    mysqlCon.Open();
                }

                string sCommand = "SELECT ID, Name, Script, Parameters, Counter, Schedule, LastRunTime, NextRunTime, LastResult FROM scheduler_Loader order by ID";

                using (MySqlCommand sqlGetSchedules = new MySqlCommand(sCommand, mysqlCon))
                {
                    MySqlDataReader sqlReader = sqlGetSchedules.ExecuteReader();

                    if (sqlReader.HasRows)
                    {
                        while (sqlReader.Read())
                        {
                            Scheduler s = new Scheduler()
                            {
                                ID = Convert.ToInt32(sqlReader[0].ToString()),
                                Name = sqlReader[1].ToString(),
                                Script = sqlReader[2].ToString(),
                                Parameters = sqlReader[3].ToString(),
                                Counter = sqlReader[4].ToString(),
                                Schedule = sqlReader[5].ToString(),
                                LastResult = sqlReader[8].ToString()
                            };

                            if (sqlReader[6].ToString() != "")
                                s.LastRunTime = Convert.ToDateTime(sqlReader[6].ToString());
                            if (sqlReader[7].ToString() != "")
                                s.NextRunTime = Convert.ToDateTime(sqlReader[7].ToString());

                            lstSchedules.Add(s);
                        }
                    }
                    sqlReader.Close();
                }
            }
            catch (Exception exError)
            {
                throw new Exception("Error while GetApiHeaders " + exError.Message);
            }
            finally
            {
                if (mysqlCon.State == ConnectionState.Open)
                {
                    mysqlCon.Close();
                }
            }

            return lstSchedules;
        }

        public Scheduler GetScheduleByID(int scheduleID)
        {
            Scheduler schedule = null;
            try
            {
                MySqlCommand cmd = new MySqlCommand();
                cmd.Connection = mysqlCon;
                cmd.CommandText = "SELECT ID, Name, Script, Parameters, Counter, Schedule, LastRunTime, NextRunTime, LastResult FROM scheduler_Loader WHERE ID = " + scheduleID;
                mysqlCon.Open();

                var dr = cmd.ExecuteReader();

                if (dr.HasRows)
                {
                    dr.Read();
                    schedule = new Scheduler();
                    schedule.ID = Convert.ToInt32(dr[0].ToString());
                    schedule.Name = dr[1].ToString();
                    schedule.Script = dr[2].ToString();
                    schedule.Parameters = dr[3].ToString();
                    schedule.Counter = dr[4].ToString();
                    schedule.Schedule = dr[5].ToString();
                    if (dr[6].ToString() != "")
                        schedule.LastRunTime = Convert.ToDateTime(dr[6].ToString());
                    if (dr[7].ToString() != "")
                        schedule.NextRunTime = Convert.ToDateTime(dr[7].ToString());
                    schedule.LastResult = dr[8].ToString();

                }
                dr.Close();

            }
            catch (Exception e)
            {
                schedule = null;
                throw new Exception("Error while GetScheduleByID " + e.Message);

            }
            finally
            { mysqlCon.Close(); }

            return schedule;
        }

        public bool UpdateScheduler(Scheduler s)
        {
            bool success = false;
            try
            {
                Scheduler schedule = GetScheduleByID(s.ID);


                if (schedule != null)
                {
                    MySqlCommand cmd = new MySqlCommand();
                    cmd.Connection = mysqlCon;
                    mysqlCon.Open();

                    cmd.CommandText = "UPDATE scheduler_Loader SET Counter = '" + s.Counter + "',";


                    if (s.LastRunTime != null)
                        cmd.CommandText = cmd.CommandText + "LastRunTime = '" + s.LastRunTime?.ToString("yyyy-MM-dd HH:mm:ss") + "',";

                    if (s.NextRunTime != null)
                        cmd.CommandText = cmd.CommandText + " NextRunTime = '" + s.NextRunTime?.ToString("yyyy-MM-dd HH:mm:ss") + "',";

                    cmd.CommandText = cmd.CommandText + " LastResult = '" + s.LastResult + "' WHERE ID = " + s.ID;
                    cmd.ExecuteNonQuery();
                    success = true;
                }

            }
            catch (Exception e)
            {
                success = false;
                throw new Exception("Error while UpdateScheduler " + e.Message);

            }
            finally
            { mysqlCon.Close(); }
            return success;
        }

        /// <summary>
        /// Creates an HTTP Web Request with the API Details supplied
        /// Reads response given by Web Request
        /// Reads response as stream and converts it into JSON
        /// </summary>
        /// <param name="apiDetails">API Call details</param>
        /// <returns>JSON Response as string</returns>
        public string APIWebRequest(ApiDetails apiDetails)
        {
            try
            {
                if (!apiDetails.CertificateValidation)
                    System.Net.ServicePointManager.ServerCertificateValidationCallback = delegate { return true; };

                HttpWebRequest webRequest = WebRequest.Create(apiDetails.RequestURL) as HttpWebRequest;
                webRequest.Method = apiDetails.RequestMethod;
                webRequest.Host = apiDetails.Host;
                webRequest.ContentType = apiDetails.ContentType;
                webRequest.KeepAlive = apiDetails.KeepAlive;
                webRequest.Headers.Add(HttpRequestHeader.AcceptEncoding, apiDetails.AcceptEncoding);
                webRequest.Accept = apiDetails.Accept;
                webRequest.Headers.Add(HttpRequestHeader.AcceptLanguage, apiDetails.AcceptLanguage);
                webRequest.UserAgent = apiDetails.UserAgent;

                foreach (ApiHeaders h in apiDetails.ApiHeaders)
                {
                    webRequest.Headers.Add(h.HeaderName, h.HeaderValue);
                }

                //ReadingResponse
                HttpWebResponse WebResponse = (HttpWebResponse)webRequest.GetResponse();

                Stream responseStream = responseStream = WebResponse.GetResponseStream();
                if (WebResponse.ContentEncoding.ToLower().Contains("gzip"))
                    responseStream = new GZipStream(responseStream, CompressionMode.Decompress);
                else if (WebResponse.ContentEncoding.ToLower().Contains("deflate"))
                    responseStream = new DeflateStream(responseStream, CompressionMode.Decompress);

                StreamReader Reader = new StreamReader(responseStream, Encoding.Default);

                string json = Reader.ReadToEnd();

                WebResponse.Close();
                responseStream.Close();

                webRequest.GetResponse().Close();
                return json;
            }
            catch (Exception exError)
            {
                throw new Exception("Error APIWebRequest Sending/Receiving " + exError.Message);

            }


        }

        /// <summary>
        /// Gets Header Details from DB for a particular API call
        /// </summary>
        /// <param name="iApiDetailsPk">API call ID</param>
        /// <returns>List of API Headers</returns>
        public List<ApiHeaders> GetApiHeaders(int iApiDetailsPk)
        {
            List<ApiHeaders> apiHeaders = new List<ApiHeaders>();
            try
            {
                if (mysqlCon.State == ConnectionState.Closed)
                {
                    mysqlCon.Open();
                }

                string sCommand = "SELECT ApiHeadersSeqId, ApiDetailsSeqId, HeaderName, HeaderValue FROM ApiHeaders where ApiDetailsSeqId = " + iApiDetailsPk;

                using (MySqlCommand sqlGetHeaderDetails = new MySqlCommand(sCommand, mysqlCon))
                {
                    MySqlDataReader sqlReader = sqlGetHeaderDetails.ExecuteReader();

                    if (sqlReader.HasRows)
                    {
                        while (sqlReader.Read())
                        {
                            ApiHeaders h = new ApiHeaders()
                            {
                                ApiDetailsSeqId = sqlReader.GetInt32(0),
                                ApiHeaderSeqId = sqlReader.GetInt32(1),
                                HeaderName = sqlReader.GetString(2),
                                HeaderValue = sqlReader.GetString(3)
                            };

                            apiHeaders.Add(h);
                        }
                    }
                    sqlReader.Close();
                }
            }
            catch (Exception exError)
            {
                throw new Exception("Error while GetApiHeaders " + exError.Message);
            }
            finally
            {
                if (mysqlCon.State == ConnectionState.Open)
                {
                    mysqlCon.Close();
                }
            }

            return apiHeaders;
        }

        /// <summary>
        /// Gets API Details from DB for a given API Call
        /// </summary>
        /// <param name="sName">Name of API Call</param>
        /// <returns>API Details</returns>
        public ApiDetails GetApiDetailsByName(string sName)
        {
            ApiDetails apiDetails = null;
            try
            {
                if (mysqlCon.State == ConnectionState.Closed)
                {
                    mysqlCon.Open();
                }

                string sCommand = "SELECT ApiDetailsSeqId, Name, RequestURL, RequestMethod, Accept, AcceptLanguage, AcceptEncoding, UserAgent, Host, ContentType, KeepAlive, CertificateValidation FROM ApiDetails WHERE Name = '" + sName + "'";

                using (MySqlCommand sqlGetApiDetails = new MySqlCommand(sCommand, mysqlCon))
                {
                    MySqlDataReader sqlReader = sqlGetApiDetails.ExecuteReader();

                    if (sqlReader.HasRows)
                    {
                        while (sqlReader.Read())
                        {
                            apiDetails = new ApiDetails();
                            apiDetails.ApiDetailsSeqId = Convert.ToInt32(sqlReader[0].ToString());
                            apiDetails.Name = sqlReader[1].ToString();
                            apiDetails.RequestURL = sqlReader[2].ToString();
                            apiDetails.RequestMethod = sqlReader[3].ToString();
                            apiDetails.Accept = sqlReader[4].ToString();
                            apiDetails.AcceptLanguage = sqlReader[5].ToString();
                            apiDetails.AcceptEncoding = sqlReader[6].ToString();
                            apiDetails.UserAgent = sqlReader[7].ToString();
                            apiDetails.Host = sqlReader[8].ToString();
                            apiDetails.ContentType = sqlReader[9].ToString();
                            apiDetails.KeepAlive = Convert.ToBoolean(sqlReader[10].ToString());
                            apiDetails.CertificateValidation = Convert.ToBoolean(sqlReader[11].ToString());
                        }
                    }

                    sqlReader.Close();
                }
            }
            catch (Exception exError)
            {
                throw new Exception("Error while GetApiDetailsByName " + exError.Message);

            }
            finally
            {
                if (mysqlCon.State == ConnectionState.Open)
                {
                    mysqlCon.Close();
                }
            }

            if (apiDetails != null)
            {
                apiDetails.ApiHeaders = GetApiHeaders(apiDetails.ApiDetailsSeqId);
            }

            return apiDetails;
        }


        public bool InsertRecordKeno(DrawJSON draw, DrawJSON nextDraw)
        {
            try
            {
                if (mysqlCon.State == ConnectionState.Closed)
                {
                    mysqlCon.Open();
                }

                MySqlTransaction stInsertRecord;
                stInsertRecord = mysqlCon.BeginTransaction();

                using (MySqlCommand sqlUpdateLatestRecordTo0 = new MySqlCommand("UPDATE nl_stat.WebStatsFastKeno SET NextDrawNo = @num, NextDrawDateTime = @nextDatetime, NextDrawDate = @nextDate, NextDrawTime = @nextTime, LastDrawInd = 0 WHERE LastDrawInd = 1", mysqlCon, stInsertRecord))
                {
                    sqlUpdateLatestRecordTo0.Parameters.AddWithValue("@num", DBNull.Value);
                    sqlUpdateLatestRecordTo0.Parameters.AddWithValue("@nextDatetime", DBNull.Value);
                    sqlUpdateLatestRecordTo0.Parameters.AddWithValue("@nextDate", DBNull.Value);
                    sqlUpdateLatestRecordTo0.Parameters.AddWithValue("@nextTime", DBNull.Value);
                    sqlUpdateLatestRecordTo0.ExecuteNonQuery();
                }

                DateTime dtEpochConverted = ConvertEpochDateToDateTime(draw.closeTime);
                DateTime dtEpochConvertedNextDraw = ConvertEpochDateToDateTime(nextDraw.closeTime);

                using (MySqlCommand sqlInsertNewRecord = new MySqlCommand("INSERT INTO nl_stat.WebStatsFastKeno VALUES (@game, 1, @drawNo, @drawDateTime, @drawDate, @drawTime, @num1, @num2, @num3, @num4, @num5, @num6, @num7, @num8, @num9, @num10, @num11, @num12, @num13, @num14, @num15, @num16, @num17, @num18, @num19, @num20, @hiLo, @nextDrawNo, @nextDrawDateTime, @nextDrawDate, @nextDrawTime, @date)", mysqlCon, stInsertRecord))
                {
                    sqlInsertNewRecord.Parameters.AddWithValue("@game", "FastKeno");
                    sqlInsertNewRecord.Parameters.AddWithValue("@drawNo", draw.id);
                    sqlInsertNewRecord.Parameters.AddWithValue("@drawDateTime", dtEpochConverted);
                    sqlInsertNewRecord.Parameters.AddWithValue("@drawDate", dtEpochConverted.Date);
                    sqlInsertNewRecord.Parameters.AddWithValue("@drawTime", dtEpochConverted.TimeOfDay);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num1", draw.results[0].primary[0]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num2", draw.results[0].primary[1]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num3", draw.results[0].primary[2]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num4", draw.results[0].primary[3]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num5", draw.results[0].primary[4]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num6", draw.results[0].primary[5]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num7", draw.results[0].primary[6]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num8", draw.results[0].primary[7]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num9", draw.results[0].primary[8]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num10", draw.results[0].primary[9]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num11", draw.results[0].primary[10]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num12", draw.results[0].primary[11]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num13", draw.results[0].primary[12]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num14", draw.results[0].primary[13]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num15", draw.results[0].primary[14]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num16", draw.results[0].primary[15]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num17", draw.results[0].primary[16]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num18", draw.results[0].primary[17]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num19", draw.results[0].primary[18]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@num20", draw.results[0].primary[19]);
                    sqlInsertNewRecord.Parameters.AddWithValue("@hiLo", CalculateHiLow(draw));
                    sqlInsertNewRecord.Parameters.AddWithValue("@nextDrawNo", nextDraw.id);
                    sqlInsertNewRecord.Parameters.AddWithValue("@nextDrawDateTime", dtEpochConvertedNextDraw);
                    sqlInsertNewRecord.Parameters.AddWithValue("@nextDrawDate", dtEpochConvertedNextDraw.Date.ToString("yyyy-MM-dd"));
                    sqlInsertNewRecord.Parameters.AddWithValue("@nextDrawTime", dtEpochConvertedNextDraw.ToString("HH:mm:ss"));
                    sqlInsertNewRecord.Parameters.AddWithValue("@date", DateTime.Now);
                    sqlInsertNewRecord.ExecuteNonQuery();
                }

                using (MySqlCommand sqlRemoveLiveRecord = new MySqlCommand("DELETE FROM nl_stat.WebStatsFastKenoLive", mysqlCon, stInsertRecord))
                {
                    sqlRemoveLiveRecord.ExecuteNonQuery();
                }

                using (MySqlCommand sqlInsertLiveRecord = new MySqlCommand("INSERT INTO nl_stat.WebStatsFastKenoLive VALUES (@game, 1, @drawNo, @drawDateTime, @drawDate, @drawTime, @num1, @num2, @num3, @num4, @num5, @num6, @num7, @num8, @num9, @num10, @num11, @num12, @num13, @num14, @num15, @num16, @num17, @num18, @num19, @num20, @hiLo, @date)", mysqlCon, stInsertRecord))
                {
                    sqlInsertLiveRecord.Parameters.AddWithValue("@game", "FastKeno");
                    sqlInsertLiveRecord.Parameters.AddWithValue("@drawNo", draw.id);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@drawDateTime", dtEpochConverted);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@drawDate", dtEpochConverted.Date);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@drawTime", dtEpochConverted.TimeOfDay);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num1", draw.results[0].primary[0]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num2", draw.results[0].primary[1]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num3", draw.results[0].primary[2]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num4", draw.results[0].primary[3]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num5", draw.results[0].primary[4]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num6", draw.results[0].primary[5]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num7", draw.results[0].primary[6]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num8", draw.results[0].primary[7]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num9", draw.results[0].primary[8]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num10", draw.results[0].primary[9]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num11", draw.results[0].primary[10]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num12", draw.results[0].primary[11]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num13", draw.results[0].primary[12]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num14", draw.results[0].primary[13]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num15", draw.results[0].primary[14]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num16", draw.results[0].primary[15]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num17", draw.results[0].primary[16]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num18", draw.results[0].primary[17]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num19", draw.results[0].primary[18]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@num20", draw.results[0].primary[19]);
                    sqlInsertLiveRecord.Parameters.AddWithValue("@hiLo", CalculateHiLow(draw));
                    sqlInsertLiveRecord.Parameters.AddWithValue("@date", DateTime.Now);
                    sqlInsertLiveRecord.ExecuteNonQuery();
                }

                try
                    {
                        stInsertRecord.Commit();
                    }
                    catch (Exception exInnerException)
                    {
                        stInsertRecord.Rollback();
                        return false;
                    }

                return true;

            }
            catch (Exception exError)
            {
                return false;
            }
            finally
            {
                if (mysqlCon.State == ConnectionState.Open)
                {
                    mysqlCon.Close();
                }
            }
        }

        private static DateTime ConvertEpochDateToDateTime(long epochDateTime)
        {
            try
            {
                DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                epoch = epoch.AddMilliseconds(epochDateTime);
                epoch = TimeZoneInfo.ConvertTime(epoch, TimeZoneInfo.FindSystemTimeZoneById("Central European Standard Time"));

                return epoch;
            }
            catch (Exception ex)
            {
                throw new Exception("Error ConvertEpochDateToDateTime " + ex.Message);
            }
        }

        protected string CalculateHiLow(DrawJSON draw)
        {
            string[] sNums = draw.results[0].primary;
            int[] iNums = Array.ConvertAll(sNums, int.Parse);

            int iCountLess41 = 0; int iCountGreater40 = 0;

            for(int iCount = 0; iCount < iNums.Length; iCount++)
            {
                if((iNums[iCount] >= 1) && (iNums[iCount] <= 40))
                {
                    iCountLess41++;
                }

                if ((iNums[iCount] >= 41) && (iNums[iCount] <= 80))
                {
                    iCountGreater40++;
                }
            }

            if(iCountLess41 > 12)
            {
                return "LO";
            }
            else if (iCountGreater40 > 12)
            {
                return "HI";
            }
            else
            {
                return "NONE";
            }
        }
    }

    public class Scheduler
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public string Script { get; set; }
        public string Parameters { get; set; }
        public string Counter { get; set; }
        public string Schedule { get; set; }
        public DateTime? LastRunTime { get; set; }
        public DateTime? NextRunTime { get; set; }
        public string LastResult { get; set; }
    }

    public class ApiDetails
    {

        public int ApiDetailsSeqId { get; set; }
        public string Name { get; set; }
        public string RequestURL { get; set; }
        public string RequestMethod { get; set; }
        public string Accept { get; set; }
        public string AcceptLanguage { get; set; }
        public string AcceptEncoding { get; set; }
        public string UserAgent { get; set; }
        public string Host { get; set; }
        public string ContentType { get; set; }
        public bool KeepAlive { get; set; }
        public bool CertificateValidation { get; set; }
        public List<ApiHeaders> ApiHeaders { get; set; }
    }

    public class ApiHeaders
    {
        public int ApiHeaderSeqId { get; set; }
        public int ApiDetailsSeqId { get; set; }
        public string HeaderName { get; set; }
        public string HeaderValue { get; set; }
    }
}
