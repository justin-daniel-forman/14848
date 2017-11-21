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
    std::string _name;
    std::string _data;
    SSIndex *_index;

public:

    Memtable(std::string);
    ~Memtable(void);

    std::string read(std::string);
    int write(std::string, std::string);
    void del(std::string);
    const char *get_data(void);
    SSIndex *get_index(void);

};

#endif /* MEMTABLE_H */
