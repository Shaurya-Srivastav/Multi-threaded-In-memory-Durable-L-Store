from lstore.index import Index
import threading

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns  


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.num_columns = num_columns
        self.key = key
        self.page_directory = {}  
        self.rid_to_versions = {}  
        self.index = Index(self)  
        self.next_rid = 0  

    def merge_base_tail(self):
        for rid, versions in self.rid_to_versions.items():
            if len(versions) > 1:
                self.rid_to_versions[rid][0] = versions[-1]
                versions.clear()
                versions.append(self.rid_to_versions[rid][0])

    def start_background_merge(self):
        merge_thread = threading.Thread(target=self.merge_base_tail)
        merge_thread.start()

    def get_new_rid(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def insert_record(self, record):
        rid = self.get_new_rid()
        self.rid_to_versions[rid] = [record]  
        self.index.pk_index[record[self.key]] = rid 
        return rid

    def get_latest_version(self, rid):
        if rid in self.rid_to_versions:
            return self.rid_to_versions[rid][-1]  
        return None