Custom implementations of `TablePropertiesCollectorFactory` may now return a `nullptr` collector to decline processing a file, reducing callback overheads in such cases.
