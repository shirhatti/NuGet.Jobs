using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NuGet.Jobs.Common;
using System.Diagnostics;

namespace GenerateSearchRankingsJob
{
    internal class Job : JobBase
    {
        private static readonly int DefaultRankingCount = 250;
        private static readonly string DefaultContainerName = "ng-search-data";
        private static readonly string ReportName = "rankings.v1.json";
        private static readonly JobEventSource JobEventSourceLog = JobEventSource.Log;

        public SqlConnectionStringBuilder WarehouseConnection { get; set; }
        public int? RankingCount { get; set; }
        public CloudStorageAccount Destination { get; set; }
        public string DestinationContainerName { get; set; }
        public CloudBlobContainer DestinationContainer { get; set; }

        public string OutputDirectory { get; set; }
        public override bool Init(IDictionary<string, string> jobArgsDictionary)
        {
            try
            {
                // Init member variables
                Destination = CloudStorageAccount.Parse(JobConfigManager.GetArgument(jobArgsDictionary,
                            JobArgumentNames.PrimaryStorageAccount,
                            EnvironmentVariableKeys.StoragePrimary));
                DestinationContainerName = JobConfigManager.TryGetArgument(jobArgsDictionary, JobArgumentNames.DestinationContainerName);
                DestinationContainer = Destination.CreateCloudBlobClient().GetContainerReference(
                   String.IsNullOrEmpty(DestinationContainerName) ? DefaultContainerName : DestinationContainerName);
                RankingCount = JobConfigManager.TryGetIntArgument(jobArgsDictionary, JobArgumentNames.RankingCount) ?? DefaultRankingCount;
                OutputDirectory = JobConfigManager.TryGetArgument(jobArgsDictionary, JobArgumentNames.OutputDirectory);
                WarehouseConnection = new SqlConnectionStringBuilder(JobConfigManager.GetArgument(jobArgsDictionary,
                            JobArgumentNames.SourceDatabase,
                            EnvironmentVariableKeys.SqlWarehouse));
                return true;
            }
            catch (Exception ex)
            {
                Trace.TraceError(ex.ToString());
            }
            return false;
        }

        public override async Task<bool> Run()
        {
            string destination = String.IsNullOrEmpty(OutputDirectory) ?
              (Destination.Credentials.AccountName + "/" + DestinationContainer.Name) :
              OutputDirectory;
            if (String.IsNullOrEmpty(destination))
            {
                throw new Exception("WarehouseJob_NoDestinationAvailable");
            }
            JobEventSourceLog.GeneratingSearchRankingReport(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog, destination);
            // Gather overall rankings
            JObject report = new JObject();
            JobEventSourceLog.GatheringOverallRankings(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog);
            var overallData = await GatherOverallRankings();
            report.Add("Rank", overallData);
            JobEventSourceLog.GatheredOverallRankings(overallData.Count);

            // Get project types
            JobEventSourceLog.GettingAvailableProjectTypes(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog);
            var projectTypes = await GetProjectTypes();
            JobEventSourceLog.GotAvailableProjectTypes(projectTypes.Count);

            // Gather data by project type
            int count = 0;
            JobEventSourceLog.GatheringProjectTypeRankings(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog);
            foreach (var projectType in projectTypes)
            {
                JobEventSourceLog.GatheringProjectTypeRanking(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog, projectType);
                var data = await GatherProjectTypeRanking(projectType);
                report.Add(projectType, data);
                JobEventSourceLog.GatheredProjectTypeRanking(data.Count, projectType);
                count += data.Count;
            }
            JobEventSourceLog.GatheredProjectTypeRankings(count);
            // Write the JSON blob
            await WriteReport(report, ReportName, JobEventSourceLog.WritingReportBlob, JobEventSourceLog.WroteReportBlob, Formatting.Indented);

            return true;
        }

        private async Task<JArray> GatherOverallRankings()
        {
            using (var connection = await WarehouseConnection.ConnectTo())
            {
                // Get the script
                var script = await ResourceHelpers.ReadResourceFile("GenerateSearchRankingsJob.Scripts.SearchRanking_Overall.sql", typeof(Job).Assembly);

                // Execute it and return the results
                return new JArray(
                    (await connection.QueryWithRetryAsync<SearchRankingEntry>(script, new { RankingCount }, commandTimeout: 120))
                        .Select(e => e.PackageId));
            }
        }

        private async Task<IList<string>> GetProjectTypes()
        {
            using (var connection = await WarehouseConnection.ConnectTo())
            {
                // Execute the query and return the results
                return (await connection.QueryAsync<string>("SELECT ProjectTypes FROM Dimension_Project")).ToList();
            }
        }

        private async Task<JArray> GatherProjectTypeRanking(string projectType)
        {
            using (var connection = await WarehouseConnection.ConnectTo())
            {
                // Get the script
                var script = await ResourceHelpers.ReadResourceFile("GenerateSearchRankingsJob.Scripts.SearchRanking_ByProjectType.sql", typeof(Job).Assembly);

                // Execute it and return the results
                return new JArray(
                    (await connection.QueryWithRetryAsync<SearchRankingEntry>(script, new { RankingCount, ProjectGuid = projectType }, commandTimeout: 120))
                        .Select(e => e.PackageId));
            }
        }

        private async Task WriteReport(JObject report, string name, Action<string> onWriting, Action<string> onWritten, Formatting formatting)
        {
            if (!String.IsNullOrEmpty(OutputDirectory))
            {
                await WriteToFile(report, name, onWriting, onWritten, formatting);
            }
            else
            {
                await DestinationContainer.CreateIfNotExistsAsync();
                await WriteToBlob(report, name, onWriting, onWritten, formatting);
            }
        }

        private async Task WriteToFile(JObject report, string name, Action<string> onWriting, Action<string> onWritten, Formatting formatting)
        {
            string fullPath = Path.Combine(OutputDirectory, name);
            string parentDir = Path.GetDirectoryName(fullPath);
            onWriting(fullPath);
            
            if (!Directory.Exists(parentDir))
            {
                Directory.CreateDirectory(parentDir);
            }
            if (File.Exists(fullPath))
            {
                File.Delete(fullPath);
            }
            using (var writer = new StreamWriter(File.OpenWrite(fullPath)))
            {
                await writer.WriteAsync(report.ToString(formatting));
            }
            
            onWritten(fullPath);
        }

        private async Task WriteToBlob(JObject report, string name, Action<string> onWriting, Action<string> onWritten, Formatting formatting)
        {
            var blob = DestinationContainer.GetBlockBlobReference(name);
            onWriting(blob.Uri.AbsoluteUri);
            blob.Properties.ContentType = "application/json";
            await blob.UploadTextAsync(report.ToString(formatting));

            onWritten(blob.Uri.AbsoluteUri);
        }
    }

    public class SearchRankingEntry
    {
        public string PackageId { get; set; }
        public int Downloads { get; set; }
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-GenerateSearchRankings")]
    public class JobEventSource : EventSource
    {
        public static readonly JobEventSource Log = new JobEventSource();
        private JobEventSource() { }

        [Event(
            eventId: 1,
            Message = "Gathering Overall Rankings from {0}/{1}...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.GatheringOverallRankings)]
        public void GatheringOverallRankings(string dbServer, string db) { WriteEvent(1, dbServer, db); }

        [Event(
            eventId: 2,
            Message = "Gathered {0} rows of data.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.GatheringOverallRankings)]
        public void GatheredOverallRankings(int rows) { WriteEvent(2, rows); }

        [Event(
            eventId: 3,
            Message = "Gathering Project Type Rankings from {0}/{1}...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.GatheringProjectTypeRankings)]
        public void GatheringProjectTypeRankings(string dbServer, string db) { WriteEvent(3, dbServer, db); }

        [Event(
            eventId: 4,
            Message = "Gathered {0} rows of data for all project types.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.GatheringProjectTypeRankings)]
        public void GatheredProjectTypeRankings(int rows) { WriteEvent(4, rows); }

        [Event(
            eventId: 5,
            Message = "Gathering Project Type Rankings for '{2}' from {0}/{1}...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.GatheringProjectTypeRanking)]
        public void GatheringProjectTypeRanking(string dbServer, string db, string projectType) { WriteEvent(5, dbServer, db, projectType); }

        [Event(
            eventId: 6,
            Message = "Gathered {0} rows of data for project type '{1}'.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.GatheringProjectTypeRanking)]
        public void GatheredProjectTypeRanking(int rows, string projectType) { WriteEvent(6, rows, projectType); }

        [Event(
            eventId: 7,
            Message = "Writing report to {0}.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.WritingReportBlob)]
        public void WritingReportBlob(string uri) { WriteEvent(7, uri); }

        [Event(
            eventId: 8,
            Message = "Wrote report to {0}.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.WritingReportBlob)]
        public void WroteReportBlob(string uri) { WriteEvent(8, uri); }

        [Event(
            eventId: 9,
            Message = "Getting Project Types from {0}/{1}...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.GettingAvailableProjectTypes)]
        public void GettingAvailableProjectTypes(string dbServer, string db) { WriteEvent(9, dbServer, db); }

        [Event(
            eventId: 10,
            Message = "Got {0} project types",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.GettingAvailableProjectTypes)]
        public void GotAvailableProjectTypes(int rows) { WriteEvent(10, rows); }

        [Event(
            eventId: 11,
            Message = "Generating Search Ranking Report from {0}/{1} to {2}.",
            Level = EventLevel.Informational)]
        public void GeneratingSearchRankingReport(string dbServer, string db, string destinaton) { WriteEvent(11, dbServer, db, destinaton); }

        [Event(
            eventId: 12,
            Message = "Query timed out, retrying",
            Level = EventLevel.Informational)]
        public void Retrying() { WriteEvent(12); }

        public static class Tasks
        {
            public const EventTask GatheringOverallRankings = (EventTask)0x01;
            public const EventTask GatheringProjectTypeRankings = (EventTask)0x02;
            public const EventTask GatheringProjectTypeRanking = (EventTask)0x03;
            public const EventTask WritingReportBlob = (EventTask)0x04;
            public const EventTask GettingAvailableProjectTypes = (EventTask)0x05;
        }
    }
}
