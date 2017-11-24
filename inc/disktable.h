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

    bool _valid; //Hacky error handling for now

    std::map<std::string, index_entry_t*> _index;
    std::map<std::string, index_entry_t*>::iterator _iter;

public:
    SSTable(std::string name, std::map<std::string, std::string> memtable);
    ~SSTable(void);

    //Reads the associated value if key is mapped
    std::string read(std::string);
    void invalidate(std::string); //set 'valid=false' on the entry

    //Called on the newer one with a pointer to the older one
    int merge_older_table(SSTable*);

};

#endif /* SSTABLE_H */
