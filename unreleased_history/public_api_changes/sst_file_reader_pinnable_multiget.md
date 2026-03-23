Added `SstFileReader::MultiGet` overload that accepts `std::vector<PinnableSlice>*`, enabling zero-copy reads when the underlying `TableReader` supports pinning.
