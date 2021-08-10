using Dapper;
using Hangfire;
using Dedup.Common;
using Dedup.Repositories;
using Dedup.ViewModels;
using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Dedup.Models;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Dedup.Services
{
    public sealed class JobScheduler
    {
        public static JobScheduler Instance { get { return HangfireServiceInstance.Instance; } }

        private class HangfireServiceInstance
        {
            static HangfireServiceInstance()
            {
            }

            internal static readonly JobScheduler Instance = new JobScheduler();
        }

 
        public void DeleteJob(string ccid, int connectorId, string jobId, ScheduleType scheduleType)
        {
            if (!string.IsNullOrEmpty(ccid) && connectorId > 0)
            {
                try
                {
                    bool isDeleted = false;
                    if (scheduleType != ScheduleType.MANUAL_SYNC)
                    {
                        //Create new connection to schedule database
                        using (ConnectionFactory connectionFactory = new ConnectionFactory(ConfigVars.Instance.hangfireConnectionString))
                        {
                            StringBuilder sb = new StringBuilder();
                            string hangFireSchema = ConfigVars.Instance.herokuAddonAppName;
                            sb.Append($"SELECT h1.* FROM \"{hangFireSchema}\".hash h1");
                            sb.Append($" JOIN (SELECT key FROM \"{hangFireSchema}\".hash WHERE value LIKE '%{ccid}%' AND (value::json->>'Arguments')::json->>1='{connectorId}' AND REPLACE((value::json->>'Arguments')::json->>2,'\"','')='{ccid}') h2 ON h1.key=h2.key");
                            sb.Append(" WHERE h1.field='CreatedAt' OR (h1.field='LastJobId' AND h1.value<>'')");
                            var recrecurringJobs = connectionFactory.DbConnection.Query<dynamic>(sb.ToString()).ToArray();
                            if (recrecurringJobs != null)
                            {
                                for (int i = 0; i < recrecurringJobs.Length; ++i)
                                {
                                    if (recrecurringJobs[i].field.Trim() == "LastJobId")
                                    {
                                        DeleteJob(recrecurringJobs[i].value.Trim());
                                        Console.WriteLine("DeleteJob=>{0}:{1} deleted", recrecurringJobs[i].key.Trim(), recrecurringJobs[i].value.Trim());
                                    }

                                    try
                                    {
                                        RecurringJob.RemoveIfExists(recrecurringJobs[i].key.Trim().Replace("recurring-job:", ""));
                                        Console.WriteLine("RecurringJob=>{0} deleted", recrecurringJobs[i].key.Trim().Replace("recurring-job:", ""));
                                        isDeleted = true;
                                    }
                                    catch (Exception exception)
                                    {
                                        Console.WriteLine("Error: {0}", exception.Message);
                                    }
                                }
                            }
                        }
                    }

                    if (!string.IsNullOrEmpty(jobId) && !isDeleted)
                    {
                        DeleteJob(jobId);
                        Console.WriteLine("DeleteJob=>{0} deleted", jobId);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: {0}", ex.Message);
                }
            }
        }

        public void DeleteJob(string jobId)
        {
            if (!string.IsNullOrEmpty(jobId))
            {
                try
                {
                    RecurringJob.RemoveIfExists(jobId);
                    Console.WriteLine("RecurringJob=>{0} deleted", jobId);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: {0}", ex.Message);
                }

                try
                {
                    BackgroundJob.Delete(jobId);
                    Console.WriteLine("BackgroundJob=>{0} deleted", jobId);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: {0}", ex.Message);
                }
            }
        }

        /// <summary>
        /// Method: ScheduleJob
        /// Description: It is used to schedule background sync process for connector
        /// </summary>
        /// <param name="connector"></param>
        /// <param name="id"></param>
        /// <returns>status as int</returns>
        public async Task<int> ScheduleJob(string ccid, int id, DatabaseType connectorType, ScheduleType scheduleType, int? customScheduleInMinutes = 1200)
        {
            var syncStatus = 1;
            try
            {
                if (string.IsNullOrEmpty(ccid) || id == 0)
                    return 0;

                IJobCancellationToken token = JobCancellationToken.Null;
                //token = new JobCancellationToken(true);
                var jobKey = string.Empty;
                if (scheduleType != ScheduleType.MANUAL_SYNC)// && scheduleType != ScheduleType.STREAMING_SYNC)
                    jobKey = Math.Abs(Guid.NewGuid().ToInt()).ToString();
                if (DatabaseType.Azure_SQL == connectorType)
                {
                    //switch (scheduleType)
                    //{
                    //    case ScheduleType.EVERY_15_MINS:
                    //        RecurringJob.AddOrUpdate<DERepository>(jobKey, service => service.AddDataRowsToSqlConnector(token, id, ccid), Cron.MinuteInterval(15), null, "critical");
                    //        break;
                    //    case ScheduleType.CUSTOM:
                    //        RecurringJob.AddOrUpdate<DERepository>(jobKey, service => service.AddDataRowsToSqlConnector(token, id, ccid), Cron.MinuteInterval(customScheduleInMinutes), null, "critical");
                    //        break;
                    //    case ScheduleType.EVERY_60_MINS:
                    //        RecurringJob.AddOrUpdate<DERepository>(jobKey, service => service.AddDataRowsToSqlConnector(token, id, ccid), Cron.Hourly, null, "critical");
                    //        break;
                    //    case ScheduleType.ONCE_DAILY:
                    //        RecurringJob.AddOrUpdate<DERepository>(jobKey, service => service.AddDataRowsToSqlConnector(token, id, ccid), Cron.Daily, null, "critical");
                    //        break;
                    //    case ScheduleType.TWICE_DAILY:
                    //        RecurringJob.AddOrUpdate<DERepository>(jobKey, service => service.AddDataRowsToSqlConnector(token, id, ccid), Cron.HourInterval(11), null, "critical");
                    //        break;
                    //    case ScheduleType.TWICE_WEEKLY:
                    //        RecurringJob.AddOrUpdate<DERepository>(jobKey, service => service.AddDataRowsToSqlConnector(token, id, ccid), Cron.DayInterval(2), null, "critical");
                    //        break;
                    //    case ScheduleType.ONCE_WEEKLY:
                    //        RecurringJob.AddOrUpdate<DERepository>(jobKey, (service) => service.AddDataRowsToSqlConnector(token, id, ccid), Cron.Weekly, null, "critical");
                    //        break;
                    //    case ScheduleType.MANUAL_SYNC:
                    //        jobKey = BackgroundJob.Enqueue<DERepository>(service => service.AddDataRowsToSqlConnector(token, id, ccid));
                    //        break;
                    //    case ScheduleType.STREAMING_SYNC:
                    //        jobKey = string.Empty;
                    //        break;
                    //}
                }
                else
                {
                    switch (scheduleType)
                    {
                        case ScheduleType.EVERY_15_MINS:
                            RecurringJob.AddOrUpdate<ISyncRepository>(jobKey, service => service.DeDupRowsFromDatabaseTable(token, id, ccid), DedupCron.MinuteInterval(15), TimeZoneInfo.Local, Constants.JOB_QUEUE_NAME);
                            break;
                        case ScheduleType.CUSTOM:
                            RecurringJob.AddOrUpdate<ISyncRepository>(jobKey, service => service.DeDupRowsFromDatabaseTable(token, id, ccid), DedupCron.MinuteInterval((int)customScheduleInMinutes), TimeZoneInfo.Local, Constants.JOB_QUEUE_NAME);
                            break;
                        case ScheduleType.EVERY_60_MINS:
                            RecurringJob.AddOrUpdate<ISyncRepository>(jobKey, service => service.DeDupRowsFromDatabaseTable(token, id, ccid), DedupCron.Hourly, TimeZoneInfo.Local, Constants.JOB_QUEUE_NAME);
                            break;
                        case ScheduleType.ONCE_DAILY:
                            RecurringJob.AddOrUpdate<ISyncRepository>(jobKey, service => service.DeDupRowsFromDatabaseTable(token, id, ccid), DedupCron.Daily, TimeZoneInfo.Local, Constants.JOB_QUEUE_NAME);
                            break;
                        case ScheduleType.TWICE_DAILY:
                            RecurringJob.AddOrUpdate<ISyncRepository>(jobKey, service => service.DeDupRowsFromDatabaseTable(token, id, ccid), DedupCron.HourInterval(11), TimeZoneInfo.Local, Constants.JOB_QUEUE_NAME);
                            break;
                        case ScheduleType.TWICE_WEEKLY:
                            RecurringJob.AddOrUpdate<ISyncRepository>(jobKey, service => service.DeDupRowsFromDatabaseTable(token, id, ccid), DedupCron.DayInterval(2), TimeZoneInfo.Local, Constants.JOB_QUEUE_NAME);
                            break;
                        case ScheduleType.ONCE_WEEKLY:
                            RecurringJob.AddOrUpdate<ISyncRepository>(jobKey, (service) => service.DeDupRowsFromDatabaseTable(token, id, ccid), DedupCron.Weekly, TimeZoneInfo.Local, Constants.JOB_QUEUE_NAME);
                            break;
                        case ScheduleType.MANUAL_SYNC:
                            jobKey = BackgroundJob.Enqueue<ISyncRepository>(service => service.DeDupRowsFromDatabaseTable(token, id, ccid));
                            break;
                            //case ScheduleType.STREAMING_SYNC:
                            //    jobKey = string.Empty;
                            //    break;
                    }
                }

                if (!string.IsNullOrEmpty(jobKey))
                {
                    SyncRepository.UpdateSyncInfo(id: id, ccid: ccid, jobid: jobKey);
                }
            }
            catch (Exception ex)
            {
                syncStatus = 0;
                Console.WriteLine("Error: {0}", ex.Message);
            }
            return await Task.FromResult(syncStatus);
        }

        public Task<string> IsScheduledJob(string ccid, int connectorId, ScheduleType scheduleType, int? customScheduleInMinutes = 1200)
        {
            string recurringJobId = string.Empty;
            if (!string.IsNullOrEmpty(ccid) && connectorId > 0)
            {
                try
                {
                    string cronExpr = string.Empty;
                    switch (scheduleType)
                    {
                        case ScheduleType.EVERY_15_MINS:
                            cronExpr = DedupCron.MinuteInterval(15);
                            break;
                        case ScheduleType.EVERY_60_MINS:
                            cronExpr = DedupCron.Hourly();
                            break;
                        case ScheduleType.CUSTOM:
                            cronExpr = DedupCron.MinuteInterval((int)customScheduleInMinutes);
                            break;
                        case ScheduleType.ONCE_DAILY:
                            cronExpr = DedupCron.Daily();
                            break;
                        case ScheduleType.TWICE_DAILY:
                            cronExpr = DedupCron.HourInterval(11);
                            break;
                        case ScheduleType.TWICE_WEEKLY:
                            cronExpr = DedupCron.DayInterval(2);
                            break;
                        case ScheduleType.ONCE_WEEKLY:
                            cronExpr = DedupCron.Weekly();
                            break;
                    }

                    //Create new connection to schedule database
                    using (ConnectionFactory connectionFactory = new ConnectionFactory(ConfigVars.Instance.hangfireConnectionString))
                    {
                        StringBuilder sb = new StringBuilder();
                        string hangFireSchema = ConfigVars.Instance.herokuAddonAppName;
                        sb.Append($"SELECT h1.* FROM \"{hangFireSchema}\".hash h1");
                        sb.Append($" JOIN (SELECT key FROM \"{hangFireSchema}\".hash WHERE value LIKE '%{ccid}%' AND (value::json->>'Arguments')::json->>1='{connectorId}' AND REPLACE((value::json->>'Arguments')::json->>2,'\"','')='{ccid}') h2 ON h1.key=h2.key");
                        sb.Append(" WHERE h1.field='CreatedAt' OR h1.field='Cron' OR (h1.field='LastJobId' AND h1.value<>'')");
                        var recrecurringJobs = connectionFactory.DbConnection.Query<dynamic>(sb.ToString()).ToArray();
                        if (recrecurringJobs != null)
                        {
                            //get scheduled jobId
                            if (recrecurringJobs.Where(p => p.field.Trim() == "Cron" && p.value.Trim() == cronExpr.Trim()).Count() > 0)
                            {
                                var jobIds = recrecurringJobs.Where(p => p.field.Trim() == "Cron" && p.value.Trim() == cronExpr.Trim()).Select(p => ((string)p.key).Trim()).ToArray();
                                recurringJobId = recrecurringJobs.Where(p => jobIds.Contains(((string)p.key).Trim()) && p.field.Trim() == "CreatedAt").OrderByDescending(p => Convert.ToDateTime(p.value.Trim())).FirstOrDefault().key.Trim();
                            }

                            List<string> deletedIds = new List<string>();
                            for (int i = 0; i < recrecurringJobs.Length; ++i)
                            {
                                if (!recurringJobId.Equals(recrecurringJobs[i].key.Trim(), StringComparison.OrdinalIgnoreCase))
                                {
                                    if (recrecurringJobs[i].field.Trim() == "LastJobId")
                                    {
                                        try
                                        {
                                            RecurringJob.RemoveIfExists(recrecurringJobs[i].value.Trim());
                                            Console.WriteLine("RecurringJob=>Expired {0}:{1} deleted", recrecurringJobs[i].key.Trim(), recrecurringJobs[i].value.Trim());
                                        }
                                        catch (Exception exception)
                                        {
                                            Console.WriteLine("Error: {0}", exception.Message);
                                        }

                                        try
                                        {
                                            BackgroundJob.Delete(recrecurringJobs[i].value.Trim());
                                            Console.WriteLine("BackgroundJob=>Expired {0}:{1} deleted", recrecurringJobs[i].key.Trim(), recrecurringJobs[i].value.Trim());
                                        }
                                        catch (Exception exception)
                                        {
                                            Console.WriteLine("Error: {0}", exception.Message);
                                        }
                                    }

                                    try
                                    {
                                        if (!deletedIds.Contains(recrecurringJobs[i].key.Trim()))
                                        {
                                            RecurringJob.RemoveIfExists(recrecurringJobs[i].key.Trim().Replace("recurring-job:", ""));
                                            Console.WriteLine("RecurringJob=>Expired {0} deleted", recrecurringJobs[i].key.Trim());
                                            deletedIds.Add(recrecurringJobs[i].key.Trim());
                                        }
                                    }
                                    catch (Exception exception)
                                    {
                                        Console.WriteLine("Error: {0}", exception.Message);
                                    }
                                }
                            }
                            deletedIds.Clear();
                            deletedIds = null;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: {0}", ex.Message);
                }
            }

            return Task.FromResult(recurringJobId);
        }

        /// <summary>
        /// Method: StopSyncJob
        /// Description: It is used to stop background job by ccid, connectorId, jobId and scheduleType
        /// </summary>
        /// <param name="ccid"></param>
        /// <param name="connectorId"></param>
        /// <param name="jobId"></param>
        /// <param name="scheduleType"></param>
        public Task StopSyncJob(string ccid, int connectorId, string jobId, ScheduleType scheduleType)
        {
            if (!string.IsNullOrEmpty(ccid) && connectorId > 0)
            {
                try
                {
                    bool isDeleted = false;
                    if (scheduleType != ScheduleType.MANUAL_SYNC)
                    {
                        //Create new connection to schedule database
                        using (ConnectionFactory connectionFactory = new ConnectionFactory(ConfigVars.Instance.hangfireConnectionString))
                        {
                            StringBuilder sb = new StringBuilder();
                            string hangFireSchema = ConfigVars.Instance.herokuAddonAppName;
                            sb.Append($"SELECT h1.* FROM \"{hangFireSchema}\".hash h1");
                            sb.Append($" JOIN (SELECT key FROM \"{hangFireSchema}\".hash WHERE value LIKE '%{ccid}%' AND (value::json->>'Arguments')::json->>1='{connectorId}' AND REPLACE((value::json->>'Arguments')::json->>2,'\"','')='{ccid}') h2 ON h1.key=h2.key");
                            sb.Append(" WHERE (h1.field='LastJobId' AND h1.value<>'')");
                            var recrecurringJobs = connectionFactory.DbConnection.Query<dynamic>(sb.ToString()).ToArray();
                            if (recrecurringJobs != null)
                            {
                                for (int i = 0; i < recrecurringJobs.Length; ++i)
                                {
                                    if (recrecurringJobs[i].field.Trim() == "LastJobId")
                                    {
                                        try
                                        {
                                            BackgroundJob.Delete(recrecurringJobs[i].value.Trim());
                                            Console.WriteLine("BackgroundJob=>{0}:{1} stopped", recrecurringJobs[i].key.Trim(), recrecurringJobs[i].value.Trim());
                                            isDeleted = true;
                                        }
                                        catch (Exception exception)
                                        {
                                            Console.WriteLine("Error: {0}", exception.Message);
                                        }

                                        try
                                        {
                                            RecurringJob.RemoveIfExists(recrecurringJobs[i].value.Trim());
                                            Console.WriteLine("RecurringJob=>{0}:{1} stopped", recrecurringJobs[i].key.Trim(), recrecurringJobs[i].value.Trim());
                                            isDeleted = true;
                                        }
                                        catch (Exception ex)
                                        {
                                            Console.WriteLine("Error: {0}", ex.Message);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (!string.IsNullOrEmpty(jobId) && !isDeleted)
                    {
                        DeleteJob(jobId);
                        Console.WriteLine("DeleteJob=>{0} deleted", jobId);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: {0}", ex.Message);
                }
            }

            return Task.CompletedTask;
        }

        public async Task ResetScheduledJobs()
        {
            try
            {
                //cancel token
               var cancellationToken = (new CancellationTokenSource()).Token;
                await Task.Run(async () =>
                {
                    Connectors[] connectors = null;
                    Console.WriteLine("RSJ:Get connectors starts");
                    //get sync connectors
                    using (ConnectionFactory connectionFactory = new ConnectionFactory(ConfigVars.Instance.connectionString))
                    {
                        StringBuilder sb = new StringBuilder();
                        sb.Append(string.Format("SELECT c.* FROM \"{0}\".\"Connectors\" c", Constants.ADDON_DB_DEFAULT_SCHEMA));
                        sb.Append(string.Format(" LEFT JOIN \"{0}\".\"Resources\" r ON c.ccid=r.uuid", Constants.ADDON_DB_DEFAULT_SCHEMA));
                        //sb.Append(" WHERE r.plan NOT IN (" + $"{string.Join(",", ConfigVars.Instance.addonPrivatePlanLevels.Select(p => $"'{p}'").ToArray())}" + ");");
                        connectors = connectionFactory.DbConnection.Query<Connectors>(sb.ToString()).ToArray();
                    }
                    Console.WriteLine("RSJ:Get connectors ended");
                    if (connectors != null && connectors.Length > 0)
                    {
                        Console.WriteLine("RSJ:Connectors Count: {0}", connectors.Length);
                        for (int i = 0; i < connectors.Length; ++i)
                        {
                            //Cancel current task if cancel requested (eg: when system getting shutdown)
                            if (cancellationToken != null && cancellationToken.IsCancellationRequested)
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                return;
                            }

                            var isRecurringJobNeedRestart = false;
                            var connector = connectors[i];
                            if (connector.sync_status == 1)
                            {
                                //one time schedule job delete
                                if (!string.IsNullOrEmpty(connector.job_id) && (connector.schedule_type == ScheduleType.MANUAL_SYNC))
                                {
                                    //delete old jobs
                                    DeleteJob(connector.job_id);
                                }

                                //set sync status to failed{3}
                                var connectorLogs = new ConnectorLogs()
                                {
                                    sync_ended_at = DateTime.UtcNow,
                                    sync_logs = new List<string>() {
                                        RestSharp.Extensions.MonoHttp.HttpUtility.UrlEncode($"Records synced: {connector.sync_count} {Environment.NewLine}"),
                                        RestSharp.Extensions.MonoHttp.HttpUtility.UrlEncode($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff zzz} [{Microsoft.Extensions.Logging.LogLevel.Information}]: {"Restarted sync after Heroku Dyno Restart, no action needed on your part"} {Environment.NewLine}")
                                    }
                                };

                                //Update job status
                                Console.WriteLine("RSJ:Update connector sync status starts");
                                if (connector.schedule_type == ScheduleType.MANUAL_SYNC)
                                {
                                    SyncRepository.UpdateSyncInfo(id: connector.connector_id, ccid: connector.ccid, status: 9, connectorLogs: connectorLogs);

                                    Console.WriteLine("RSJ:Newly schedule job manual sync:{0}-{1}", connector.ccid, connector.connector_id);
                                    await ScheduleJob(connector.ccid, connector.connector_id, JsonConvert.DeserializeObject<DatabaseConfig>(connector.dest_config_json).databaseType, connector.schedule_type, connector.custom_schedule_in_minutes).ConfigureAwait(false);
                                }
                                else
                                {
                                    isRecurringJobNeedRestart = true;
                                    SyncRepository.UpdateSyncInfo(id: connector.connector_id, ccid: connector.ccid, status: 9, connectorLogs: connectorLogs);
                                }
                                Console.WriteLine("RSJ:Update connector sync status ended");
                            }

                            if (connector.schedule_type != ScheduleType.MANUAL_SYNC)
                            {
                                string recurringJobId = await IsScheduledJob(connector.ccid, connector.connector_id, connector.schedule_type, connector.custom_schedule_in_minutes).ConfigureAwait(false);
                                Console.WriteLine("RSJ:Recurring Job ID:{0}", recurringJobId);
                                Console.WriteLine("RSJ:isRecurringJobNeedRestart:{0}", isRecurringJobNeedRestart);
                                if (string.IsNullOrEmpty(recurringJobId))
                                {
                                    Console.WriteLine("RSJ:Newly schedule job:{0}-{1}", connector.ccid, connector.connector_id);
                                    await ScheduleJob(connector.ccid, connector.connector_id, JsonConvert.DeserializeObject<DatabaseConfig>(connector.dest_config_json).databaseType, connector.schedule_type, connector.custom_schedule_in_minutes).ConfigureAwait(false);
                                }
                                else if (isRecurringJobNeedRestart)
                                {
                                    Console.WriteLine("RSJ:scheduled job triggered immediately:{0}", recurringJobId);
                                    RecurringJob.Trigger(recurringJobId.Replace("recurring-job:", ""));
                                }
                            }
                        }
                    }

                }, cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error {0}", ex.Message);
            }
        }
    }
}
