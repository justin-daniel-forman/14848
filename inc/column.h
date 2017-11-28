#ifndef COLUMN_H
#define COLUMN_H

#include <deque>
#include <string>
#include <mutex>
#include <thread>
#include <vector>

#include "memtable.h"
#include "disktable.h"

struct Compact_Worker {

    long newer_uid;
    long older_uid;

};

struct Dump_Container {

    long table_uid;
    long sst_uid;
    std::map <std::string, std::string> raw_map;
    SSTable *new_sst;
};

struct Compact_Container {
    long t0_uid;
    long t1_uid;
    long next_uid;
    SSTable *t0;
    SSTable *t1;
    SSTable *next_table;
};

/*
 *  Keeps track of attributes in our NoSQL Database
 *
 */
class Column {

private:

    std::string _name;
    int _compression_opt;
    std::thread dumper;
    std::thread compactor;

    std::deque <std::thread> _cworkers;
    std::vector <std::thread> _dworkers;
    std::thread _cm;
    std::thread _dm;
    bool _cleanup;

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

    //Background processes and methods
    void Dump_Master();
    void Compact_Master();
    void dump_map_to_disk(Dump_Container*);
    void compact_tables(Compact_Container*);

public:
    Column(std::string, int = 0);
    ~Column();

    std::string read(std::string);
    void write(std::string, std::string);
    void del(std::string);

};

#endif
