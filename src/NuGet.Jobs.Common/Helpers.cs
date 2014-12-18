using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Jobs.Common
{
    public static class StorageHelpers
    {
        private static readonly string PackageBackupsDirectory = "packages";
        private static readonly string PackageBlobNameFormat = "{0}.{1}.nupkg";
        private static readonly string PackageBackupBlobNameFormat = PackageBackupsDirectory + "/{0}/{1}/{2}.nupkg";
        private const string ContentTypeJson = "application/json";

        public static string GetPackageBlobName(string id, string version)
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                PackageBlobNameFormat,
                id,
                version).ToLowerInvariant();
        }

        public static string GetPackageBackupBlobName(string id, string version, string hash)
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                PackageBackupBlobNameFormat,
                id.ToLowerInvariant(),
                version.ToLowerInvariant(),
                WebUtility.UrlEncode(hash));
        }

        public static CloudBlobDirectory GetBlobDirectory(CloudStorageAccount account, string path)
        {
            var client = account.CreateCloudBlobClient();
            client.DefaultRequestOptions = new BlobRequestOptions()
            {
                ServerTimeout = TimeSpan.FromMinutes(5)
            };

            string[] segments = path.Split('/');
            string containerName;
            string prefix;

            if (segments.Length < 2)
            {
                // No "/" segments, so the path is a container and the catalog is at the root...
                containerName = path;
                prefix = String.Empty;
            }
            else
            {
                // Found "/" segments, but we need to get the first segment to use as the container...
                containerName = segments[0];
                prefix = String.Join("/", segments.Skip(1)) + "/";
            }

            var container = client.GetContainerReference(containerName);
            var dir = container.GetDirectoryReference(prefix);
            return dir;
        }

        public static async Task UploadJsonBlob(CloudBlobContainer container, string blobName, string content)
        {
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            blob.Properties.ContentType = ContentTypeJson;
            await blob.UploadTextAsync(content);
        }
    }

    public static class ArgCheck
    {
        public static void Require(string value, string name)
        {
            if (String.IsNullOrEmpty(value))
            {
                throw new ArgumentNullException(name);
            }
        }

        public static void Require(object value, string name)
        {
            if (value == null)
            {
                throw new ArgumentNullException(name);
            }
        }
    }

    public static class ResourceHelpers
    {
        public static Task<string> ReadResourceFile(string name)
        {
            return ReadResourceFile(name, typeof(ResourceHelpers).Assembly);
        }

        public static async Task<string> ReadResourceFile(string name, Assembly asm)
        {
            using (var stream = asm.GetManifestResourceStream(name))
            using (var reader = new StreamReader(stream))
            {
                return await reader.ReadToEndAsync();
            }
        }
    }
}

namespace System.Data.SqlClient
{
    public static class SqlConnectionStringBuilderExtensions
    {
        public static Task<SqlConnection> ConnectTo(this SqlConnectionStringBuilder self)
        {
            return ConnectTo(self.ConnectionString);
        }

        private static async Task<SqlConnection> ConnectTo(string connection)
        {
            var c = new SqlConnection(connection);
            await c.OpenAsync().ConfigureAwait(continueOnCapturedContext: false);
            return c;
        }
    }
}

namespace Dapper
{
    public static class DapperExtensions
    {
        public static Task ExecuteAsync(this SqlConnection connection, string sql)
        {
            SqlCommand cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandType = CommandType.Text;
            return cmd.ExecuteNonQueryAsync();
        }

        public static async Task<IEnumerable<T>> QueryWithRetryAsync<T>(
            this SqlConnection connection,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null,
            int maxRetries = 10,
            Action onRetry = null)
        {
            for (int attempt = 0; attempt < maxRetries; attempt++)
            {
                try
                {
                    return await connection.QueryAsync<T>(sql, param, transaction, commandTimeout, commandType);
                }
                catch (SqlException ex)
                {
                    switch (ex.Number)
                    {
                        case -2:   // Client Timeout
                        case 701:  // Out of Memory
                        case 1204: // Lock Issue 
                        case 1205: // >>> Deadlock Victim
                        case 1222: // Lock Request Timeout
                        case 8645: // Timeout waiting for memory resource 
                        case 8651: // Low memory condition 
                            // Ignore
                            if (attempt < (maxRetries - 1))
                            {
                                if (onRetry != null)
                                {
                                    onRetry();
                                }
                            }
                            else
                            {
                                throw;
                            }
                            break;
                        default:
                            throw;
                    }
                }
            }
            throw new Exception("Unknown error! Should have thrown the final timeout!");
        }
    }
}



