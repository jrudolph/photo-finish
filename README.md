# Photo Finish - A media storage and metadata repository experiment

A central archive for personal media files.

## Guiding principles

 * Data in the the repository is immutable
 * Data is stored in a content-addressable way leading to automatic deduplication
 * Metadata for the stored data can be stored in the repository or next to it in a simple enough format to allow access
   to metadata in the future
 * Metadata can be either entered manually (ownership data, comments, ratings, etc.) or
   be extracted by pluggable metadata extractors (file metadata like EXIF, basic file information, contents of file containers,
   keyframes of videos, text content of PDFs, any summary of file you might want to query)
 * Metadata Processes can enumerate all the existing metadata and do any kind of aggregation of the total data and
   provide access to it
 * Complete processing might take a long time, so it needs to be incremental