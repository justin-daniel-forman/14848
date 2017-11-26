#ifndef SSTABLE_H
#define SSTABLE_H

#include <string>
#include <map>

/*
 *  Entry in the index that represents the metadata for the k->v pair
 */
struct index_entry_t {

    int  offset;
    int  len;
    bool valid;

};

/*
 *  Representation of an SST to be kept on disk
 */
class SSTable {

private:
    std::string _name;
    std::string _filename;
    long _file_len;
    int _compression_type;

    std::map<std::string, index_entry_t*> _index;
    std::map<std::string, index_entry_t*>::iterator _iter;

public:
    SSTable(std::string name,
            std::map<std::string, std::string> memtable,
            int compress_opt = 0);
    ~SSTable(void);

    std::string read(std::string);
    bool peek(std::string);
    void invalidate(std::string);
    long get_file_len();

    int merge_into_table(SSTable, long);
    int append_data_block(std::string,
                          std::map<std::string, index_entry_t*>,
                          int);


};

#endif /* SSTABLE_H */
