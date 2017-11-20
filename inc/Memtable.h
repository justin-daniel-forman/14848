#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <string>

#include "SSIndex.h"

#define PAGE_SIZE 4096

/*
 *  Memtable implementation
 */
class Memtable {

private:
    std::string _data;
    SSIndex *_index;

public:

    Memtable(void);
    ~Memtable(void);

    int write(std::string key, std::string value);
    std::string read(std::string key);
    void del(std::string key);
    const char *get_data(void);
    SSIndex *get_index(void);

};

#endif /* MEMTABLE_H */
