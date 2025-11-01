# Database Check list

1. [x] Update DB to support multiple SST files, even though at this point there will only ever be one. will need something in memory representing all files that is constructed on start up. Get logic should be updated to go through all files until it finds key

   - reminder even though all this exists we will have no code that creates multiple files. So there will only be 1 file

2. [x] Create in-memory BTree that flushes contents to disk (in-order) once certain size is reached

   - flush should play well with step 1 on restart
   - do not worry about data in memory for crashes
   - flush will be blocking as part of set or delete

3. [x] Create WAL for in-memory BTree

   - all sets and deletes should record into WAL
   - on BTree flush WAL should be cleared
   - BTree should be constructed with contents of WAL on start up

4. [x] Update GET logic to be smarter

   - once we start writing contents from in-memory table to disk. All contents on sst files will be ordered. Meaning we can update our GET behaviour to move onto next file once we have passed a key that would be later then it on file
   - Like if we are looking for Brad in file. We would start from beginning Alice -> Allison -> Bary -> Brett. Once we hit Brett we know that Brad must not be in this file as it would have been before Brett. So we can move onto next file

5. Bloom filter

   - implement generation and utilization of bloom filters to speed up look ups.
   - bloom filter files will be generated on flush

6, Index files

- currently we start at beginning of SST files and continue till we find the key or pass it (refer to step 4). We should introduce index files that store small fraction of contents on files so that we can start at certain offsets in the file

6. [] Look into multiple file compaction

   - background process that will compact multiple files together into single file
   - will
