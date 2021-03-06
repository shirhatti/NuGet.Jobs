@echo OFF
	
cd bin

:Top
	echo "Starting job - #{Jobs.stats.aggregatecdndownloadsingallery.Title}"
	
	set NUGETJOBS_STORAGE_PRIMARY=#{Jobs.stats.aggregatecdndownloadsingallery.Storage.Primary}

	title #{Jobs.stats.aggregatecdndownloadsingallery.Title}

	start /w stats.aggregatecdndownloadsingallery.exe -StatisticsDatabase "#{Jobs.stats.aggregatecdndownloadsingallery.StatisticsDatabase}" -DestinationDatabase "#{Jobs.stats.aggregatecdndownloadsingallery.DestinationDatabase}" -verbose true -interval #{Jobs.stats.aggregatecdndownloadsingallery.Interval}

	echo "Finished #{Jobs.stats.aggregatecdndownloadsingallery.Title}"

	goto Top
