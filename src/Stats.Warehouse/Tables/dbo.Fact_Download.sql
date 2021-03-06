﻿CREATE TABLE [dbo].[Fact_Download] (
	[Id]							UNIQUEIDENTIFIER NOT NULL DEFAULT newid(),
    [Dimension_Package_Id]			INT NOT NULL,
    [Dimension_Date_Id]				INT NOT NULL,
    [Dimension_Time_Id]				INT NOT NULL,
    [Dimension_Operation_Id]		INT NOT NULL,
    [Dimension_Client_Id]			INT NOT NULL,
    [Dimension_Platform_Id]			INT NOT NULL,
    [Fact_UserAgent_Id]				INT NOT NULL,
    [Fact_LogFileName_Id]			INT NOT NULL,
    [Fact_EdgeServer_IpAddress_Id]	INT NOT NULL,
    [DownloadCount]					INT NULL,
    [Timestamp] DATETIME NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_Fact_Download] PRIMARY KEY CLUSTERED ([Id]) WITH (STATISTICS_NORECOMPUTE = ON)
);
GO
CREATE NONCLUSTERED INDEX [Fact_Download_NCI_TimestampDesc]
    ON [dbo].[Fact_Download]([Timestamp] DESC)
    INCLUDE([Dimension_Date_Id], [Dimension_Package_Id], [Dimension_Client_Id], [DownloadCount]) WITH (STATISTICS_NORECOMPUTE = ON);
GO
CREATE NONCLUSTERED INDEX [Fact_Download_NCI_DownloadCount]
    ON [dbo].[Fact_Download]([DownloadCount] ASC) WITH (STATISTICS_NORECOMPUTE = ON);
GO
CREATE NONCLUSTERED INDEX [Fact_Download_NCI_Package_Id]
    ON [dbo].[Fact_Download]([Dimension_Package_Id] ASC)
    INCLUDE([Dimension_Client_Id], [Dimension_Date_Id], [Dimension_Operation_Id], [DownloadCount], [Timestamp]) WITH (STATISTICS_NORECOMPUTE = ON);
GO
CREATE NONCLUSTERED INDEX [Fact_Download_NCI_Date_Time]
    ON [dbo].[Fact_Download]([Dimension_Date_Id] ASC, [Timestamp])
    INCLUDE([Dimension_Package_Id], [Dimension_Client_Id], [DownloadCount]) WITH (STATISTICS_NORECOMPUTE = ON);
GO
CREATE NONCLUSTERED INDEX [Fact_Download_NCI_UserAgent]
    ON [dbo].[Fact_Download] ([Fact_UserAgent_Id])
	INCLUDE ([Dimension_Client_Id], [DownloadCount]) WITH (ONLINE = ON)
GO
CREATE NONCLUSTERED INDEX [Fact_Download_NCI_LogFileName]
    ON [dbo].[Fact_Download] ([Fact_LogFileName_Id])
	INCLUDE ([DownloadCount]) WITH (ONLINE = ON)
GO
CREATE NONCLUSTERED INDEX [Fact_Download_NCI_EdgeServer_IpAddress]
    ON [dbo].[Fact_Download] ([Fact_EdgeServer_IpAddress_Id])
	INCLUDE ([DownloadCount]) WITH (ONLINE = ON)
GO
CREATE NONCLUSTERED INDEX [Fact_Download_NCI_Client]
    ON [dbo].[Fact_Download] ([Dimension_Client_Id])
	INCLUDE ([Fact_UserAgent_Id]) WITH (ONLINE = ON)
GO