Add kFSPrefetch to FSSupportedOps enum to allow file systems to indicate prefetch support capability, avoiding unnecessary prefetch system calls on file systems that don't support them.
