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
    int _compression_opt;

    //Single writeable memtable
    std::mutex _mtable_lock;
    Memtable *_mtable;

    //RONLY memtables
    std::mutex _tables_lock;
    long _table_uid;
    std::map <long, Memtable*> _tables_map;

    //SSTs
    std::mutex _sst_lock;
    long _sst_uid;
    std::map <long, SSTable*> _sst_map;

    //Background cleanup functions
    void  dump_table_to_disk(void);
    void compact_sst(SSTable*, SSTable*);

public:
    Column(std::string, int = 0);
    ~Column();

    std::string read(std::string);
    void write(std::string, std::string);
    void del(std::string);

};

#endif
