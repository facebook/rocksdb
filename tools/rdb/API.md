# JavaScript API

## DBWrapper

### Constructor

NOTE: Error currently does not properly get thrown if an existing column family
is not specified. The constructor will still execute properly, but then any
operations on the database will segfault. This bug will be fixed in a future
update to the shell.

    # Open a new or existing RocksDB database.
	#
	# db_name         (string)   - Location of the database (inside the
	#                              `/tmp` directory).
	# column_families (string[]) - Names of additional column families
	#                              beyond the default. If there are no other
	#                              column families, this argument can be
    #                              left off.
	#
	# Throws an error if an existing column family is not specified, or if
	# a specified column family does not exist in the database.
	#
	# Returns a new database object.
	RDB(db_name, column_families = [])

### Get

	# Get the value of a given key.
	#
	# key           (string) - Which key to get the value of.
	# column_family (string) - Which column family to check for the key.
	#                          This argument can be left off for the default
	#                          column family
	#
	# Returns the value (string) that is associated with the given key if
	# one exists, or null otherwise.
	db_obj.get(key, column_family = { default })

### Put

	# Associate a value with a key.
	#
	# key           (string) - Which key to associate the value with.
	# value         (string) - The value to associate with the key.
	# column_family (string) - Which column family to put the key-value pair
	#                          in. This argument can be left off for the
	#                          default column family.
	#
	# Returns true if the key-value pair was successfully stored in the
	# database, or false otherwise.
	db_obj.put(key, value, column_family = { default })

### Delete

	# Delete a value associated with a given key.
    #
	# key           (string) - Which key to delete the value of..
	# column_family (string) - Which column family to check for the key.
	#                          This argument can be left off for the default
	#                          column family
	#
	# Returns true if an error occured while trying to delete the key in
	# the database, or false otherwise. Note that this is NOT the same as
	# whether a value was deleted; in the case of a specified key not having
	# a value, this will still return true. Use the `get` method prior to
	# this method to check if a value existed before the call to `delete`.
	db_obj.delete(key, column_family = { default })

### Dump

    # Print out all the key-value pairs in a given column family of the
    # database.
	#
	# column_family (string) - Which column family to dump the pairs from.
	#                          This argument can be left off for the default
	#                          column family.
	#
	# Returns true if the keys were successfully read from the database, or
	# false otherwise.
	db_obj.dump(column_family = { default })

### WriteBatch

	# Execute an atomic batch of writes (i.e. puts and deletes) to the
    # database.
	#
	# cf_batches (BatchObject[]; see below) - Put and Delete writes grouped
	#                                         by column family to execute
	#                                         atomically.
	#
	# Returns true if the argument array was well-formed and was
	# successfully written to the database, or false otherwise.
	db_obj.writeBatch(cf_batches)

### CreateColumnFamily

	# Create a new column familiy for the database.
	#
	# column_family_name (string) - Name of the new column family.
	#
	# Returns true if the new column family was successfully created, or
	# false otherwise.
	db_obj.createColumnFamily(column_family_name)

### CompactRange

***Will document this later***

### Close

	# Close an a database and free the memory associated with it.
	#
	# Return null.
		# db_obj.close()


## BatchObject

***Will document this later***
