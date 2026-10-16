Added `FileSystem::IsPreallocationSupported()`. A `FileSystem` that never preallocates file space can return false so that DB open skips reopening and truncating the last WAL file.
