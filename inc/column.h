#ifndef COLUMN_H
#define COLUMN_H

#include <deque>
#include <string>
#include <mutex>

#include "memtable.h"
#include "disktable.h"

/*
 *  Keeps track of attributes in our NoSQL Database
 *
 */
class Column {

private:

    std::string _name;
    int _compression;
    int _uuid;

    Memtable *_mtable;
    std::mutex _mtable_lock;

    //FIXME add bloomfilter so we don't have to iter thru each for reads?
    std::deque <Memtable*> _ronly_list;
    std::mutex _ronly_lock;

    //FIXME add bloomfilter so we don't have to iter thru each for reads?
    std::deque <SSTable*> _sst_list;
    std::mutex _sst_lock;

    int  dump_mtable(void);
    void compact_sst(void);

public:
    Column(std::string, int = 0);
    ~Column();

    std::string read(std::string);
    void write(std::string, std::string);
    void del(std::string);

};

#endif
