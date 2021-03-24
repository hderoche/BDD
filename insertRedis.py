import redis

host = 'localhost'
port = 6379
db = 0

r = redis.Redis(host=host, port=port, db=db)

class Document:
    def __init__(self, id, event_type, occuredOn, version, graph_id, nature, object_nature, path):
        self.id = id
        self.ids = [id]
        self.event_type = event_type
        self.occuredOn = occuredOn
        self.version = version
        self.graph_id = graph_id
        self.nature = nature
        self.object_name = object_name
        self.path = path
        self.values = dict({
            "id": self.id, 
            "event_type": self.event_type, 
            "occuredOn": self.occuredOn, 
            "version": self.version,
            "graph_id": self.graph_id,
            "object_name": self.object_name, 
            "path": self.path
        })
class Stats:
    def __init__(self):
        self.status = dict({
            "received": 0, 
            "verified": 0,
            "processed": 0,
            "remedied": 0,
            "consumed": 0,
            "rejected":0,
            "to_be_purged": 0,
            "purged": 0
        })
        self.integrity = 0
        self.last_doc_id = 0
    
    def count_by_status(self, doc):
        if self.last_doc_id == doc.id:
            return
        self.last_doc_id = doc.id
        path = doc.path
        if "RECEIVED" in path:
            if "VERIFIED" in path:
                if "PROCESSED" in path:
                    if "CONSUMED" in path:
                        if "TO_BE_PURGED" in path:
                            if "PURGED" in path:
                                self.integrity += 1
                                addAll()
                                self.rejected -= 1
                    elif "REJECTED" in path:
                        if "TO_BE_PURGED" in path:
                            if "PURGED" in path:
                                self.integrity += 1
                                addAll()
                                self.consumed -= 1
                        elif "REMEDIED" in path:
                            # loop back on consumed
                    else:
                        self.status.consumed += 1 
                else:
                    self.status.processed += 1
            else:
                self.status.verified += 1
        else:
            self.status.received += 1                       
        self.save()
        return
        
    def save(self):
        r.hmset('stats:' + str(1), self.status)
        r.set('stats:integrity', self.integrity)

    def addAll(self):
        self.status.received += 1 
        self.status.verified += 1
        self.status.processed += 1
        self.status.consumed += 1
        self.status.rejected += 1
        self.status.to_be_purged += 1
        self.status.purged += 1