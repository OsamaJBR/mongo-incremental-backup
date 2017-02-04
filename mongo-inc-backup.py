#!/usr/bin/python 
import pymongo
import argparse

def send_alert(message):
    #TODO
    print message 
    return 0

def mongoConnect(host,dbname,collection):
    client = pymongo.MongoClient(host)    
    if args.username and args.password : 
        client.chat.authenticate(username, password)
    db_conn = client[dbname]
    db_coll = db_conn[collection]
    return db_coll

def sync(db,collection,unique_id,update_field):
    dest_collection = mongoConnect(host=args.mongo_dest,dbname=db,collection=collection)
    try:
        last_updated_at = dest_collection.find_one(sort=[(update_field,pymongo.DESCENDING)],projection={update_field: True},limit=1)
        if not last_updated_at : last_updated_at = 0
        else : last_updated_at = int(last_updated_at[update_field])
	print 'last_update_at = %s' %last_updated_at
    except Exception as e:
        send_alert(message='[Error] Failed to get last update_field from backup server \n %s' %str(e))
        exit(2)
    
    bulk=None
    source_collection = mongoConnect(host=args.mongo_source,dbname=db,collection=collection)
    for i,doc in enumerate(source_collection.find({update_field : { "$gte" : last_updated_at } }) ) :
	if not bulk : bulk = dest_collection.initialize_ordered_bulk_op()
	bulk.find({unique_id: doc[unique_id]}).upsert().replace_one(doc)
        if i%1000 == 999:
            try:
		print 'executing bulk ..'
                bulk.execute()
            except Exception as e:
                send_alert(message='[Error] Failed to execute bulk on destination server \n %s' %str(e))
                exit(1)
	    bulk = None
    if bulk:
	try: bulk.execute()
	except Exception as e:
	    send_alert(message='[Error] Failed to execute final bulk on destination server \n %s' %str(e))
	    exit(3)

if __name__ == "__main__":
    # args
    parser = argparse.ArgumentParser()
    parser.add_argument('-u',action='store',dest='username')
    parser.add_argument('-p',action='store',dest='password')
    parser.add_argument('--uid',action='store',dest='unique_id')
    parser.add_argument('--update-field',action='store',dest='update_field')
    parser.add_argument('--src',action='store',dest='mongo_source')
    parser.add_argument('--des',action='store',dest='mongo_dest')
    parser.add_argument('--collection',action='store',dest='collection')
    parser.add_argument('--db',action='store',dest='dbname')
    args = parser.parse_args()
    sync(db=args.dbname,collection=args.collection,unique_id=args.unique_id,update_field=args.update_field)
