# fsdatastore
fsdatastore implements (a subset of) the legacy App Engine datastore api ("google.golang.org/appengine/datastore") using native Firestore.

Known differences from the original datastore api:
  - All integer ids are converted to strings (e.g. id 555 and name "555" are the same).
  - No namespace support
  - No cursor support
  - No AllocateIDs.
  - Incomplete keys get a random 20 character name instead of an integer id.
  - Unindexed field tags are ignored. This must be configured using the firestore console.
  - Queries run using CollectionGroups to match datastore behaviour as closely as possible.
    You may need to tweak your indexes.

It is possible to export your database from datastore and import it into firestore.
However, this process has some undesirable side effects:

  - Numeric ids are converted to "__id1234__" instead of "1234"
  - Nested structs are not stored as maps.
  - Unindexed string fields are converted to maps. Instead of just "foo", you get
    `{"__type__": "__legacy_meaning_value__", "meaning": 15, "value": "foo"}`

Use the CopyDatastore dataflow to copy your data between projects in the format expected by this library.


