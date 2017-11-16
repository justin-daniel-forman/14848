/*
 *  Keeps track of attributes in our NoSQL Database
 *
 */
class Column {

private:

    MemTable mtable;

    //SSTs that have been previously written
    //Keep all of the indexes in memory
    SSTable sst_list[5];
    SSIndex disk_index_list[5];

    int  dump_mtable(SSTable *sst);
    void compact_sst();

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

public:
    index_entry_t lookup_key(string key);
    int add_key(string key, string value);
    int delete_key(string key);

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
    bool merge_table(SSTable input);

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
