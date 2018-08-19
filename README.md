# Photo Finish

## Ingestion

Two possibilities

 * Scan manually organized directories and create hard-links where we like them to be
 * Offer ingestion directories where files are only kept during ingestion

Process:
 * When ingestion is started
   * Per file
      * note original file-name relative to ingestion dir
      * calculate primary check-sum
      * check if file is already in storage
        * if no, create hard-link to store
        * if yes, do nothing
      * create initial metadata blob
      
Metadata:
 * Metadata is append only, i.e. we just record events that someone (person) or something (software) has added metadata
 * Merging metadata from different sources means just merging events (maybe sorting by creation date of metadata unit) 
 * Use either manual or automatic conflict resolution to figure out conflicts (can come from multi-user or multi-device or multi-software
   deployments)       

Questions
 * Automatic or manual ingestion?
  