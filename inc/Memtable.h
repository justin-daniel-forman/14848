#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <string>

#include "SSIndex.h"

#define PAGE_SIZE 4096

/*
 *  Representation of our in-memory append-only SSTable
 */
class Memtable {

private:
    char    _byte_array[PAGE_SIZE];
    int     _curr_offset;
    SSIndex *_index;

public:

    Memtable(void);
    ~Memtable(void);

    void write(std::string key, std::string value);
    std::string read(std::string key, int offset);
    char *get_byte_array(void);

};

#endif /* MEMTABLE_H */
