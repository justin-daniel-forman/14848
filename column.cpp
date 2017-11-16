/*
 *  Keeps track of attributes in our NoSQL Database
 *
 */
class Column {

private:

    MemTable mtable;
    std::dequeue <SSTable> sst_list;

    int  dump_mtable(void);
    void compact_sst(void);

public:
    Column(string attr_name, comp_opt_t compression);
    string read(string key);
    string write(string key, string value);
    int del(string key);

};


/*
 *  Representation of our in-memory append-only SSTable
 */
class MemTable {

private:
    char  byte_array[PAGE_SIZE];
    int   curr_offset;
    SSIndex index;

public:
    void write(string key, string value);
    string read(string key, int offset);
    char *get_byte_array(void);

};


/*
 *  Representation of the index for an SSTable to be kept in memory
 */
typedef struct {

    int  offset;
    int  len;
    bool valid;

} index_entry_t;

class SSIndex {

private:
    BloomFilter bf; //FIXME: Is Bloom filter necessary since Log(n) lookup is pretty fast?
    std::map<string, index_entry_t> index;
    int curr_offset;

public:

    index_entry_t *lookup_key(string key) {
        std::map<string, index_entry_t> it = index.find[key];
        if(it != index.end()) {
            return it;
        } else {
            return NULL;
        }
    };

    int add_key(string key, string value) {
        index_entry_t *old_entry;
        index_entry_t new_entry;

        if((old_entry = lookup_key(key)) == NULL) {
            new_entry.offset = curr_offset;
            new_entry.length = strlen(value);
            new_entry.value = value;
            curr_offset += new_entry.length;
            index.insert(std::pair<string, index_entry_t>(key, new_entry));

        } else {
            old_entry.value = value;

        }

    };

    int delete_key(string key) {
        //find the entry in the map and unflip the valid bit

    };


};


/*
 *  Representation of an SST to be kept on disk
 */
class SSTable {

private:
    //FIXME: Keep open file descriptor?
    string _filename;
    SSIndex index;

public:
    //Reads the associated value if key is mapped
    int read(string key, string *result);

    //Called on the newer one with a pointer to the older one
    bool merge_older_table(SSTable *oldtable);

    SSTable(string filename, SSIndex index, char *data_array) {
        //write the data array to specified filename
        //Save index internally
    }

    destroy() {
        //delete _filename to free up disk space
        //Free the index here?
        //implies we are done with this table
    }

}


/*
 *  Bloom filter for checking membership in a set
 */
class BloomFilter {

private:
    int _size;
    int *bf;

    int h0(string k);
    int h1(string k);
    int h2(string k);

public:
    bool hit(string key);
    void insert(string key);

    BloomFilter(int size) {
        _size = size;
        bf = malloc(sizeof(int) * _size);
    }

    ~BloomFilter() {
        free(bf);
    }
};
