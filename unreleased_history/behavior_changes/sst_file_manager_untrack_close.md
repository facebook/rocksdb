DB::Close now untracks files in SstFileManager, making avaialble any space used
by them. Prior to this change they would be orphaned until the DB is re-opened.
