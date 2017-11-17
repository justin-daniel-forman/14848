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
    BloomFilter *_bf; //FIXME: Is Bloom filter necessary since Log(n) lookup is pretty fast?
    std::map<std::string, index_entry_t> _index;
    int _curr_offset;

public:

    SSIndex(void);
    ~SSIndex(void);

    index_entry_t *lookup_key(std::string);
    int add_key(std::string, std::string);
    int delete_key(std::string);


};

#endif /* SSINDEX_H */
