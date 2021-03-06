@echo OFF
	
cd bin

:Top
	echo "Starting job - #{Jobs.stats.createazurecdndownloadcountreports.Title}"
	
	set NUGETJOBS_STORAGE_PRIMARY=#{Jobs.stats.createazurecdndownloadcountreports.Storage.Primary}

	title #{Jobs.stats.createazurecdndownloadcountreports.Title}

	start /w stats.createazurecdndownloadcountreports.exe -AzureCdnCloudStorageAccount "#{Jobs.stats.createazurecdndownloadcountreports.AzureCdn.CloudStorageAccount}" -AzureCdnCloudStorageContainerName "#{Jobs.stats.createazurecdndownloadcountreports.AzureCdn.CloudStorageContainerName}" -DataStorageAccount "#{Jobs.stats.createazurecdndownloadcountreports.DataStorageAccount}" -DataContainerName "#{Jobs.stats.createazurecdndownloadcountreports.DataStorageContainerName}" -StatisticsDatabase "#{Jobs.stats.createazurecdndownloadcountreports.StatisticsDatabase}" -SourceDatabase "#{Jobs.stats.createazurecdndownloadcountreports.SourceDatabase}" -InstrumentationKey "#{Jobs.stats.createazurecdndownloadcountreports.InstrumentationKey}" -verbose true -interval #{Jobs.stats.createazurecdndownloadcountreports.Interval}

	echo "Finished #{Jobs.stats.createazurecdndownloadcountreports.Title}"

	goto Top