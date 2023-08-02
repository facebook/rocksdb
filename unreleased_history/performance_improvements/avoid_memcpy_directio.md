In case of direct_io, if buffer passed by callee is already aligned, RandomAccessFileRead::Read will avoid realloacting a new buffer, reducing memcpy and use already passed aligned buffer.
