# KVDB 

## Commands
- database create \[database-path\] \[number of elements - upper bound\]
- database get \[database-path\] \[key\] 
- database set \[database-path\] \[key\] \[value\]
- database del \[database-path\] \[key\] 
- database ts \[database-path\] \[key\] 

## General Design
The database system is based on a single file that is initialized with database create. The DB uses linear probing hashing
with lazy page deletion. It also uses advisory locks to handle concurrency. 

## Limitations
- Since the DB uses static hashing, it needs to be resized which is currently not handled. This can be fixed by running another thread and building
a shadow file to replace the original file. 
- Rolling back and crash recovery is not handled, any crash in any of the processes can make the DB inconsistent.
- Lazy page deletion can cause performance issues when closer to DB capacity.
- keys and values must have positive length and have a maximum size of 100.
- keys and values must use ASCII characters.
