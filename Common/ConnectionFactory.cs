using Dedup.ViewModels;
using Npgsql;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;

namespace Dedup.Common
{
    public class ConnectionFactory : IDisposable
    {
        public IDbConnection DbConnection { get; set; }

        public ConnectionFactory() { }

        public ConnectionFactory(string pDatabaseUrl)
        {
            if (pDatabaseUrl.StartsWith("postgres://"))
                DbConnection = new NpgsqlConnection(GetPGConnectionStringFromUrl(pDatabaseUrl));
            else
                DbConnection = new NpgsqlConnection(pDatabaseUrl);

            if (DbConnection.State == ConnectionState.Broken || DbConnection.State == ConnectionState.Closed)
            {
                DbConnection.Open();
            }
        }

        public ConnectionFactory(string pDatabaseUrl, DatabaseType pDatabaseType)
        {
            if (pDatabaseType == DatabaseType.Azure_SQL)
            {
                if (!pDatabaseUrl.ToLower().Contains("persist security info"))
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "Persist Security Info=False");
                }
                if (!pDatabaseUrl.ToLower().Contains("multipleactiveresultsets"))
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "MultipleActiveResultSets=True");
                }
                if (pDatabaseUrl.Contains("database.windows.net"))
                {
                    if (!pDatabaseUrl.ToLower().Contains("encrypt"))
                    {
                        pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "Encrypt=True");
                    }
                    if (!pDatabaseUrl.ToLower().Contains("trustservercertificate"))
                    {
                        pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "TrustServerCertificate=True");
                    }
                }
                if (!pDatabaseUrl.ToLower().Contains("connection timeout"))
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "Connection Timeout=300");
                }
                if (!pDatabaseUrl.ToLower().Contains("integrated security"))
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "Integrated Security=False");
                }
                pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "Pooling=True;Max Pool Size=50;Min Pool Size=20;");

                DbConnection = new SqlConnection(pDatabaseUrl);
            }
            else
            {
                if (pDatabaseUrl.StartsWith("postgres://"))
                    DbConnection = new NpgsqlConnection(GetPGConnectionStringFromUrl(pDatabaseUrl));
                else
                    DbConnection = new NpgsqlConnection(pDatabaseUrl);
            }

            if (DbConnection.State == ConnectionState.Broken || DbConnection.State == ConnectionState.Closed)
            {
                DbConnection.Open();
            }
        }

        public void OpenConnection()
        {
            if (DbConnection.State == ConnectionState.Broken || DbConnection.State == ConnectionState.Closed)
            {
                DbConnection.Open();
            }
        }

        private bool _disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (DbConnection != null)
                    {
                        if (DbConnection.State == ConnectionState.Open)
                        {
                            DbConnection.Close();
                        }
                        DbConnection.Dispose();
                    }
                    DbConnection = null;
                }
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~ConnectionFactory()
        {
            Dispose(false);
        }

        public static string GetSqlConnectionStringFromUrl(string pDatabaseUrl, bool pIsPooling = false)
        {
            if (!string.IsNullOrEmpty(pDatabaseUrl))
            {
                if (!pDatabaseUrl.ToLower().Contains("persist security info"))
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "Persist Security Info=False");
                }
                if (!pDatabaseUrl.ToLower().Contains("multipleactiveresultsets"))
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "MultipleActiveResultSets=True");
                }
                if (!pDatabaseUrl.ToLower().Contains("encrypt"))
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "Encrypt=True");
                }
                if (!pDatabaseUrl.ToLower().Contains("trustservercertificate"))
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "TrustServerCertificate=True");
                }
                if (!pDatabaseUrl.ToLower().Contains("connection timeout"))
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "Connection Timeout=100");
                }
                if (!pDatabaseUrl.ToLower().Contains("integrated security"))
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "Integrated Security=False");
                }

                if (pIsPooling)
                {
                    pDatabaseUrl = string.Format("{0}{1}{2};", pDatabaseUrl, (pDatabaseUrl.EndsWith(";") ? "" : ";"), "Pooling=True;Max Pool Size=50;Min Pool Size=20;");
                }
            }

            return pDatabaseUrl;
        }

        //public static string GetPGConnectionStringFromUrl(string pDatabaseUrl, bool pIsPooling = false)
        //{
        //    if (!string.IsNullOrEmpty(pDatabaseUrl))
        //    {
        //        string strConn = Regex.Replace(pDatabaseUrl, "postgres://", "", RegexOptions.IgnoreCase);
        //        string[] conStrParts = strConn.Split(new char[] { '/', ':', '?' });
        //        conStrParts = conStrParts.Where(x => !string.IsNullOrEmpty(x)).ToArray();
        //        if (conStrParts.Count() == 4)
        //        {
        //            if (pIsPooling)
        //            {
        //                return $"Host={conStrParts[1].Substring(conStrParts[1].LastIndexOf("@") + 1)};Port={conStrParts[2]};Database={conStrParts[3]};User ID={conStrParts[0]};Password={conStrParts[1].Substring(0, conStrParts[1].LastIndexOf("@"))};sslmode=Require;Trust Server Certificate=true;Pooling=true;MinPoolSize=1;MaxPoolSize=30;No Reset On Close=true;Timeout=1000;Command Timeout=0;";
        //            }
        //            else
        //            {
        //                return $"Host={conStrParts[1].Substring(conStrParts[1].LastIndexOf("@") + 1)};Port={conStrParts[2]};Database={conStrParts[3]};User ID={conStrParts[0]};Password={conStrParts[1].Substring(0, conStrParts[1].LastIndexOf("@"))};sslmode=Require;Trust Server Certificate=true;Pooling=false;No Reset On Close=true;Timeout=1000;Command Timeout=0;";
        //            }
        //        }
        //    }

        //    return string.Empty;
        //}

        public static string GetPGConnectionStringFromUrl(string pDatabaseUrl, bool pIsPooling = false)
        {
            if (!string.IsNullOrEmpty(pDatabaseUrl))
            {
                string conStrParts = pDatabaseUrl.Replace("//", "");
                string[] strConn = conStrParts.Split(new char[] { '/', ':', '?' });
                strConn = strConn.Where(x => !string.IsNullOrEmpty(x)).Select(x => x.Trim()).ToArray();
                if (pIsPooling)
                {
                    return string.Format("Host={0};Port={1};Database={2};User ID={3};Password={4};sslmode=Require;Trust Server Certificate=true;Pooling=true;MinPoolSize=1;MaxPoolSize=30;No Reset On Close=true;Timeout=25;Command Timeout=0;", strConn[2].Substring(strConn[2].LastIndexOf("@") + 1), strConn[3], strConn[4], strConn[1], strConn[2].Substring(0, strConn[2].LastIndexOf("@")));
                }
                else
                {
                    return string.Format("Host={0};Port={1};Database={2};User ID={3};Password={4};sslmode=Require;Trust Server Certificate=true;Pooling=false;No Reset On Close=true;Timeout=25;Command Timeout=0;", strConn[2].Substring(strConn[2].LastIndexOf("@") + 1), strConn[3], strConn[4], strConn[1], strConn[2].Substring(0, strConn[2].LastIndexOf("@")));
                }
            }

            return string.Empty;
        }

        public static string GetUserIdFromPGConnectionUrl(string pDatabaseUrl)
        {
            if (!string.IsNullOrEmpty(pDatabaseUrl))
            {
                string strConn = Regex.Replace(pDatabaseUrl, "postgres://", "", RegexOptions.IgnoreCase);
                string[] conStrParts = strConn.Split(new char[] { '/', ':', '?' });
                conStrParts = conStrParts.Where(x => !string.IsNullOrEmpty(x)).ToArray();
                if (conStrParts.Count() == 4)
                {
                    return conStrParts[0];
                }
            }

            return null;
        }
    }
}
