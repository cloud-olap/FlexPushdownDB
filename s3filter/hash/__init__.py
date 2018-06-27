"""What we're trying to do here is incur less traffic while running a query between the local system and the
remote store.

Locally, we have a list of keys that we want to retrieve. To reduce inbound traffic we only want to load those
records that match the list of keys we have, but we don't want to send all those keys to the remote store as
this will add a lot of outbound traffic.

To get around these two problems we create a bloom filter of the records we want to load and then convert that
bloom filter into a SQL predicate that we can 'push down' to s3. We then use the bloom filter to load only
the records we want, without needing to send the list of record keys to s3.

To do this we need to do two things, first to create and represent a bloom filter in SQL, and then use that
bloom filter as a predicate in a where clause.

Normally construction of a bloom filter would mean creation of an empty bit array. Adding values to the bloom
filter would work by hashing the value k times. The resulting hashes would indicate which bit to set in the bit
array.

The tricky part is how to push this down to s3 (which only has a limited set of operators to work with). To represent
this in SQL, we represent the bloom filter bit array as a string and the hash functions as universal hash functions
which can be calculated with simple arithmetic operators. The presence of a bit can then be tested using a substring
function on the bit array string with the starting pos set to the hash calculated on the column being filtered.

"""
