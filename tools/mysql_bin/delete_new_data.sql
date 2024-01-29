USE clientdb;

DELETE FROM BlahdRecords WHERE ValidUntil > '2020-03-01 00:00:00';
DELETE FROM SpecRecords; -- These will be refreshed to suit the new data
DELETE FROM EventRecords WHERE EndTime > '2020-03-01 00:00:00';
DELETE FROM JobRecords WHERE EndTime > '2020-03-01 00:00:00';
