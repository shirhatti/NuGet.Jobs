// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Stats.CreateAzureCdnDownloadCountReports
{
    public class DnxDownloadCountData
    {

        public string DnxVersion { get; set; }
        public string OperatingSystem { get; set; }
        public string FileName { get; set; }
        public long TotalDownloadCount { get; set; }
    }
}