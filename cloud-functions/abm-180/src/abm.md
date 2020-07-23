ABM at a high level is a distributor of the pipeline result

## Distribution Mechanisms ##
- database inserts/updates
- pub/sub
- Generate a file and optionally send it somewhere
    - SFTP
    - storage bucket
- API call
    - RESTful

## Architecture ##
- abm-180 receives all the records to be distributed
- abm-180 uses datastore or mysql for storage, all records being passed through are stored 
    - data storage is partitioned by owner aka sponsor and type (people, house, household)
    - an immutable key is assigned to each record
    - all golden ids are attached to the immutable key
        - we might need to keep track of which ones are expired
    - any source system record id will also be attached to the immutable key
        - identified by source system name and source system id
- abm-180 distributes records through pubsub topics
    - record may be distributed one at a time or use a windowed write
    - record(s) are distributed with write instructions
    - receiver will have all the information it needs to perform the task in the message
- direct receivers
    - File writer (abm-output-file)
    - RESTful writer (abm-output-rest)
    - MySQL writer (abm-output-mysql)
    - Datastore writer (abm-output-datastore)
- indirect receivers
    - SFTP writer

## Configurations ##
- Configuration can be stored in storage bucket or SQL or Datastore
- Distribution settings are stored as configuration
- Default distribution can be overriden by owner specific distribution requirement
- Defailt distribution can also be suppressed


## Changes required for 360 and 720
    - pub the new golden record with the expired golden id together