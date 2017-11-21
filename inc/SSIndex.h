#ifndef SSINDEX_H
#define SSINDEX_H

#include <string>
#include <map>

#include "BloomFilter.h"

/*
 *  Entry in the index that represents the metadata for the k->v pair
 */
typedef struct {

    int  offset;
    int  len;
    bool valid;

} index_entry_t;


/*
 *  Representation of the index for an SSTable to be kept in memory
 */
class SSIndex {

private:

    std::string _name;
    std::map<std::string, index_entry_t*> _index;
    std::map<std::string, index_entry_t*>::iterator _iter;

public:

    SSIndex(std::string);
    ~SSIndex(void);

    index_entry_t *lookup(std::string);
    void map(std::string, int, int);
    void erase(std::string); //Actually remove the key from this map
    void invalidate(std::string); //set 'valid=false' on the entry
    int  merge_newer_index(SSIndex*, int);

};

#endif /* SSINDEX_H */
