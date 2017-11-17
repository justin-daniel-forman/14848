#ifndef COLUMN_H
#define COLUMN_H

#include <deque>
#include <string>

#include "Memtable.h"
#include "SSTable.h"

/*
 *  Keeps track of attributes in our NoSQL Database
 *
 */
class Column {

private:

    Memtable *_mtable;
    std::deque <SSTable> _sst_list;

    int  dump_mtable(void);
    void compact_sst(void);

public:
    Column(int);
    ~Column();

    std::string read(std::string);
    std::string write(std::string, std::string);
    int del(std::string);

};

#endif
