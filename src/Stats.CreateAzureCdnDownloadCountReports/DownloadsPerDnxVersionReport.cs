// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Stats.CreateAzureCdnDownloadCountReports
{
    class DownloadsPerDnxVersionReport : ReportBase
    {
        private const string _storedProcedureName = "[dbo].[SelectTotalDownloadCountsPerDnxVersion]";
        private const int _defaultCommandTimeout = 1800; // 30 minutes max
        internal const string ReportName = "dnx.v1.json";

        public DownloadsPerDnxVersionReport(CloudStorageAccount cloudStorageAccount, string statisticsContainerName, SqlConnectionStringBuilder statisticsDatabase, SqlConnectionStringBuilder galleryDatabase)
            : base(new[] { new StorageContainerTarget(cloudStorageAccount, statisticsContainerName) }, statisticsDatabase, galleryDatabase)
        {
        }

        public async Task Run()
        {
            // Gather download count data from statistics warehouse
            IReadOnlyCollection<DnxDownloadCountData> data;
            Trace.TraceInformation("Gathering Dnx Download Counts from {0}/{1}...", StatisticsDatabase.DataSource, StatisticsDatabase.InitialCatalog);
            using (var connection = await StatisticsDatabase.ConnectTo())
            using (var transaction = connection.BeginTransaction(IsolationLevel.Snapshot))
            {
                data = (await connection.QueryWithRetryAsync<DnxDownloadCountData>(
                    _storedProcedureName, commandType: CommandType.StoredProcedure, transaction: transaction, commandTimeout: _defaultCommandTimeout)).ToList();
            }

            Trace.TraceInformation("Gathered {0} rows of data.", data.Count);

            if(data.Any())
            {
                var reportText = JsonConvert.SerializeObject(data, Formatting.None);
                foreach (var storageContainerTarget in Targets)
                {
                    try
                    {
                        var targetBlobContainer = await GetBlobContainer(storageContainerTarget);
                        var blob = targetBlobContainer.GetBlockBlobReference(ReportName);
                        Trace.TraceInformation("Writing report to {0}", blob.Uri.AbsoluteUri);
                        blob.Properties.ContentType = "application/json";
                        await blob.UploadTextAsync(reportText);
                        Trace.TraceInformation("Wrote report to {0}", blob.Uri.AbsoluteUri);
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceError("Error writing report to storage account {0}, container {1}. {2} {3}",
                            storageContainerTarget.StorageAccount.Credentials.AccountName,
                            storageContainerTarget.ContainerName, ex.Message, ex.StackTrace);
                    }
                }
            }
        }
    }
}
