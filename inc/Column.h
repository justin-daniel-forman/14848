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

    std::string _name;
    Memtable *_mtable;
    std::deque <SSTable> _sst_list;

    int  dump_mtable(void);
    void compact_sst(void);

public:
    Column(std::string, int);
    ~Column();

    std::string read(std::string);
    void write(std::string, std::string);
    void del(std::string);

};

#endif
