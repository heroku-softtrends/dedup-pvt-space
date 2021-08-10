using Hangfire.Client;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.States;
using Dedup.Repositories;
using Dedup.ViewModels;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Dedup.HangfireFilters
{
    /// <summary>
    /// Filter: JobsFilter
    /// Description: It is used to track the process state of schedule jobs.
    /// </summary>
    public class JobsFilter : JobFilterAttribute, IClientFilter, IServerFilter, IElectStateFilter
    {
        /// <summary>
        /// Action: OnCreating
        /// Description: It is used to check the current connector sync is already going or not.
        /// If not then allow to proceed else cancel the schedule. This is the entry filter for all sync task.
        /// Sync status 1 means pending/ongoing
        /// </summary>
        /// <param name="context"></param>
        void IClientFilter.OnCreating(CreatingContext context)
        {
            Console.WriteLine(string.Format("Creating a job based on method `{0}`", context.Job.Method.Name));
            var ccid = context.Job.Args.ElementAt(2) as string;
            int connectorId = (int)context.Job.Args.ElementAt(1);
            if (!string.IsNullOrEmpty(ccid) && connectorId > 0)
            {
                //Check connector status. If it is 1 then cancel the new schedule
                if (SyncRepository.GetSyncStatus(ccid: ccid, connectorId: connectorId) == 1)
                {
                    Console.WriteLine(string.Format("Creating job `{0}` has been canceled", context.Job.Method.Name));
                    context.Canceled = true;
                }
            }
        }

        /// <summary>
        /// Action: OnCreated
        /// Description: It is the second filter to update the sync status to 1 for all jobs validated by previous filter. But jobs are not to be started yet.
        /// Sync status 1 means pending/ongoing.
        /// </summary>
        /// <param name="context"></param>
        void IClientFilter.OnCreated(CreatedContext context)
        {
            if (context.Canceled == false || context.Exception == null)
            {
                Console.WriteLine(string.Format("Job is based on method `{0}` has been created with id `{1}`", context.Job.Method.Name, context.BackgroundJob?.Id));
                var ccid = context.Job.Args.ElementAt(2) as string;
                int connectorId = (int)context.Job.Args.ElementAt(1);
                string jobId = context.BackgroundJob?.Id;
                if (!string.IsNullOrEmpty(ccid) && connectorId > 0)
                {
                    //set sync status to progress{1}.
                    var connectorLogs = new ConnectorLogs()
                    {
                        sync_started_at = DateTime.UtcNow,
                        sync_ended_at = null,
                        sync_logs = new List<string>()
                    };

                    SyncRepository.UpdateSyncInfo(id: connectorId, ccid: ccid, status: 1, count: 0, jobid: jobId, connectorLogs: connectorLogs, totaluniquecount: 0, sync_updated_count: 0, deduped_count: 0, total_records_count:0,dedup_process_count:0);
                }
            }
        }

        /// <summary>
        /// Action: OnPerforming
        /// Description: It is the third filter to cancel any jobs if sync status is not 1 while they are performing.
        /// </summary>
        /// <param name="context"></param>
        void IServerFilter.OnPerforming(PerformingContext context)
        {
            Console.WriteLine(string.Format("Job `{0}` has been performing", context.BackgroundJob?.Id));
            if (context.Canceled == false)
            {
                var ccid = context.BackgroundJob.Job.Args.ElementAt(2) as string;
                int connectorId = (int)context.BackgroundJob.Job.Args.ElementAt(1);
                if (!string.IsNullOrEmpty(ccid) && connectorId > 0)
                {
                    //Check connector status. If it is not 1 then cancel it
                    if (SyncRepository.GetSyncStatus(ccid: ccid, connectorId: connectorId) != 1)
                    {
                        Console.WriteLine(string.Format("Performing job `{0}` has been canceled", context.Job.Method.Name));
                        context.Canceled = true;
                    }
                }
            }
        }

        /// <summary>
        /// Action: OnPerformed
        /// Description: It is the final filter to used for updating sync status to 2.
        /// Sync status 2 means completed.
        /// </summary>
        /// <param name="context"></param>
        void IServerFilter.OnPerformed(PerformedContext context)
        {
            if (context.Canceled == false && context.Exception == null)
            {
                Console.WriteLine(string.Format("Job `{0}` has been performed", context.BackgroundJob?.Id));
                var ccid = context.BackgroundJob.Job.Args.ElementAt(2) as string;
                int connectorId = (int)context.BackgroundJob.Job.Args.ElementAt(1);
                if (!string.IsNullOrEmpty(ccid) && connectorId > 0)
                {
                    if (GC.GetTotalMemory(false) >= 67108864)
                    {
                        Console.WriteLine($"GC.Generation: 2, max allocated memory: {GC.GetTotalMemory(false)}");
                        GC.Collect(2);
                        GC.WaitForPendingFinalizers();
                        GC.Collect(2);
                        Console.WriteLine($"Max allocated memory after GC.Collect: {GC.GetTotalMemory(false)}");
                    }

                    if (GC.GetTotalMemory(false) >= 33554432)
                    {
                        Console.WriteLine($"GC.Generation: 1, max allocated memory: {GC.GetTotalMemory(false)}");
                        GC.Collect(1);
                        GC.WaitForPendingFinalizers();
                        GC.Collect(1);
                        Console.WriteLine($"Max allocated memory after GC.Collect: {GC.GetTotalMemory(false)}");
                    }

                    if (GC.GetTotalMemory(false) >= 20971520)
                    {
                        Console.WriteLine($"GC.Generation: 0, max allocated memory: {GC.GetTotalMemory(false)}");
                        GC.Collect();
                        GC.WaitForPendingFinalizers();
                        GC.Collect();
                        Console.WriteLine($"Max allocated memory after GC.Collect: {GC.GetTotalMemory(false)}");
                    }

                    //set sync status to completed{2}
                    var connectorLogs = new ConnectorLogs()
                    {
                        sync_ended_at = DateTime.UtcNow,
                        sync_logs = new List<string>()
                    };

                    SyncRepository.UpdateSyncInfo(id: connectorId, ccid: ccid, status: 2, connectorLogs: connectorLogs);
                }
            }
        }

        /// <summary>
        /// Action: OnStateElection
        /// Description: It is the error filter to capture any error occur while performing the data sync. If any error occured then it will update sync status to 3.
        /// Sync status 3 means failed.
        /// </summary>
        /// <param name="context"></param>
        void IElectStateFilter.OnStateElection(ElectStateContext context)
        {
            Console.WriteLine(string.Format("Job `{0}` has been changed to state `{1}`", context.BackgroundJob?.Id, context.CandidateState.Name));
            //Get current state
            var failedState = context.CandidateState as FailedState;
            if (failedState != null)
            {
                Console.WriteLine(string.Format("Job `{0}` has been failed due to an exception `{1}`", context.BackgroundJob.Id, failedState.Exception));
                var ccid = context.BackgroundJob.Job.Args.ElementAt(2) as string;
                int connectorId = (int)context.BackgroundJob.Job.Args.ElementAt(1);
                if (!string.IsNullOrEmpty(ccid) && connectorId > 0)
                {
                    if (GC.GetTotalMemory(false) >= 67108864)
                    {
                        Console.WriteLine($"GC.Generation: 2, max allocated memory: {GC.GetTotalMemory(false)}");
                        GC.Collect(2);
                        GC.WaitForPendingFinalizers();
                        GC.Collect(2);
                        Console.WriteLine($"Max allocated memory after GC.Collect: {GC.GetTotalMemory(false)}");
                    }

                    if (GC.GetTotalMemory(false) >= 33554432)
                    {
                        Console.WriteLine($"GC.Generation: 1, max allocated memory: {GC.GetTotalMemory(false)}");
                        GC.Collect(1);
                        GC.WaitForPendingFinalizers();
                        GC.Collect(1);
                        Console.WriteLine($"Max allocated memory after GC.Collect: {GC.GetTotalMemory(false)}");
                    }

                    if (GC.GetTotalMemory(false) >= 20971520)
                    {
                        Console.WriteLine($"GC.Generation: 0, max allocated memory: {GC.GetTotalMemory(false)}");
                        GC.Collect();
                        GC.WaitForPendingFinalizers();
                        GC.Collect();
                        Console.WriteLine($"Max allocated memory after GC.Collect: {GC.GetTotalMemory(false)}");
                    }

                    //set sync status to failed{3}
                    var connectorLogs = new ConnectorLogs()
                    {
                        sync_ended_at = DateTime.UtcNow,
                        sync_logs = new List<string>() { HttpUtility.UrlEncode($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff zzz} [{LogLevel.Error}]: {failedState.Exception} {Environment.NewLine}") }
                    };

                    SyncRepository.UpdateSyncInfo(id: connectorId, ccid: ccid, status: 3, connectorLogs: connectorLogs);
                }
            }
        }
    }
}
