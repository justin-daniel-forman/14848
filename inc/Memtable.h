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

    std::string read(std::string);
    void write(std::string, std::string);
    void del(std::string);

};

#endif /* MEMTABLE_H */
