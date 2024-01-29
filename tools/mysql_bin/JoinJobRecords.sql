USE clientdb;

-- ------------------------------------------------------------------------------
-- Procedure for converting EventRecords + BlahdRecords into JobRecords
DROP PROCEDURE IF EXISTS JoinJobRecords1;
DELIMITER //
CREATE PROCEDURE JoinJobRecords1()
BEGIN
    DECLARE procstart DATETIME;
    DECLARE apeldn INT;

    SET procstart = NOW();
    SET apeldn = DNLookup("apelclient");

    REPLACE INTO JobRecords (
      UpdateTime,
      SiteID,                -- Foreign key
      SubmitHostID,          -- Foreign key
      MachineNameID,         -- Foreign key
      QueueID,               -- Foreign key
      LocalJobId,
      LocalUserId,
      GlobalUserNameID,      -- Foreign key
      FQAN,
      VOID,                  -- Foreign key
      VOGroupID,             -- Foreign key
      VORoleID,              -- Foreign key
      WallDuration,
      CpuDuration,
      Processors,
      NodeCount,
      StartTime,
      EndTime,
      InfrastructureDescription,
      InfrastructureType,
      MemoryReal,
      MemoryVirtual,
      ServiceLevelType,
      ServiceLevel,
      PublisherDNID,
      EndYear,
      EndMonth)
-- as long as we are not joining records from external databases we can do it with
-- raw IDs
    SELECT
        procstart,                                         -- JobRecord.UpdateTime
        EventRecords.SiteID as SiteID,                     -- JobRecord.Site
        BlahdRecords.CEID as SubmitHostID,                 -- JobRecord.SubmitHost
        EventRecords.MachineNameID as MachineNameID,       -- JobRecord.MachineName
        EventRecords.QueueID as QueueID,                   -- JobRecord.Queue
        EventRecords.JobName as LocalJobId,                -- JobRecord.LocalJobId
        EventRecords.LocalUserID as LocalUserId,           -- JobRecord.LocalUserId
        BlahdRecords.GlobalUserNameID as GlobalUserNameID, -- JobRecord.GlobalUserName
        BlahdRecords.FQAN as FQAN,
        BlahdRecords.VOID as VOID,                         -- JobRecord.VOID
        BlahdRecords.VOGroupID as VOGroupID,               -- JobRecord.VOGroup
        BlahdRecords.VORoleID as VORoleID,                 -- JobRecord.VORole
        EventRecords.WallDuration as WallDuration,         -- JobRecord.WallDuration
        EventRecords.CpuDuration as CpuDuration,           -- JobRecord.CpuDuration
        EventRecords.Processors as Processors,             -- JobRecord.Processors
        EventRecords.NodeCount as NodeCount,               -- JobRecord.NodeCount
        EventRecords.StartTime as StartTime,               -- JobRecord.StartTime
        EventRecords.EndTime as EndTime,                   -- JobRecord.EndTime
        EventRecords.Infrastructure as InfrastructureDescription,     -- JobRecord.Infrastructure
        "grid",                                            -- JobRecord.InfrastructureType
        EventRecords.MemoryReal as MemoryReal,             -- JobRecord.MemoryReal
        EventRecords.MemoryVirtual as MemoryVirtual,       -- JobRecord.MemoryVirtual
        SpecRecords.ServiceLevelType as ServiceLevelType,
        SpecRecords.ServiceLevel as ServiceLevel,
        apeldn,                            -- JobRecords.PublisherDN
        YEAR(EventRecords.EndTime),
        MONTH(EventRecords.EndTime)
    FROM SpecRecords
    INNER JOIN EventRecords ON ((SpecRecords.StopTime > EventRecords.EndTime
                             OR
                             SpecRecords.StopTime IS NULL)
                             AND
                             SpecRecords.StartTime <= EventRecords.EndTime)
                             AND SpecRecords.SiteID = EventRecords.SiteID
    INNER JOIN BlahdRecords ON BlahdRecords.ValidFrom <= EventRecords.EndTime AND
                             BlahdRecords.ValidUntil > EventRecords.EndTime AND
                             EventRecords.SiteID = BlahdRecords.SiteID AND EventRecords.JobName = BlahdRecords.LrmsId
                             AND SpecRecords.SiteID = BlahdRecords.SiteID AND SpecRecords.CEID = BlahdRecords.CEID
    WHERE EventRecords.Status != 2       -- all events which are not already grid jobs
        AND BlahdRecords.Processed != 1; -- all blahd which haven't been touched

    UPDATE EventRecords, JobRecords
    SET Status = 2
    WHERE EventRecords.MachineNameID = JobRecords.MachineNameID
        AND EventRecords.JobName = JobRecords.LocalJobId
        AND EventRecords.EndTime = JobRecords.EndTime
        AND JobRecords.UpdateTime >= procstart;

    UPDATE BlahdRecords, JobRecords
    SET BlahdRecords.Processed = 1
    WHERE BlahdRecords.LrmsId = JobRecords.LocalJobId
        AND BlahdRecords.CEID = JobRecords.SubmitHostID
        AND BlahdRecords.ValidUntil > JobRecords.EndTime
        AND JobRecords.UpdateTime >= procstart;

END //
DELIMITER ;
