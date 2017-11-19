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
    std::map<std::string, index_entry_t*> _index;
    std::map<std::string, index_entry_t*>::iterator _iter;

public:

    SSIndex(void);
    ~SSIndex(void);

    index_entry_t *lookup_key(std::string);
    void map_key(std::string, int, int);
    void erase_key(std::string); //Actually remove the key from this map
    void invalidate_key(std::string); //set 'valid=false' on the entry


};

#endif /* SSINDEX_H */
