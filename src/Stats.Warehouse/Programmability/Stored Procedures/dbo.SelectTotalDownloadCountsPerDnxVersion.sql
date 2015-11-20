CREATE PROCEDURE [dbo].[SelectTotalDownloadCountsPerDnxVersion]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ReportGenerationTime DATETIME = GETDATE()
    DECLARE @Cursor DATETIME = (SELECT ISNULL(MAX([Position]), @ReportGenerationTime) FROM [dbo].[Cursors] (NOLOCK) WHERE [Name] = 'GetDirtyPackageId')

    SELECT  T.[DnxVersion],
            T.[OperatingSystem],
			T.[FileName],
            SUM(ISNULL(F.[DownloadCount], 0)) AS [TotalDownloadCount]
    FROM    [dbo].[Fact_Dnx_Download] (NOLOCK) AS F

    INNER JOIN  [dbo].[Dimension_Dnx] AS T (NOLOCK)
    ON      T.[Id] = F.[Dimension_Dnx_Id]

    WHERE       F.[Timestamp] <= @Cursor

    GROUP BY    T.[DnxVersion],
                T.[OperatingSystem],
				T.[FileName]
END