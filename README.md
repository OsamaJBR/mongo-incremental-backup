# mongo-incremental-backup

Having issue with mongo-dump / mongo-restore tools ? You don't want bussniess to access live databases direct ? Then, migrate data incrementaly and periodically with this script.

#### To use tool you should have a uniqe field in your collection (who does not have !) and a time field that you can depend on to update data.

# How it works 
* Check max value of the time_field you have in the destination mongo (Backup Server).
* Get all updated/inserted document after that time from the source.
* Insert/Update all documents in the destination server.

# Example
```
$ python mongo-backup.py --src mongodb://localhost/ --des mongodb://172.17.0.7/ --db customers --collection orders --uid order_id --update-field timestamp -u admin -p password@123
```
