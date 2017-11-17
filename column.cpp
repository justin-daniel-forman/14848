/***
 *
 *  column.cpp
 *
 *  Contains definitions for class functions needed to store a column
 *  in our NoSQL database.
 *
 *  Authors: Justin Forman, Mark Wuebbens
 *
 ***/

#include <iostream>
#include <cstdlib>

#include "inc/Column.h"
#include "inc/SSTable.h"
#include "inc/Memtable.h"

using namespace std;

/*****************************************************************************
 *                                  COLUMN                                   *
 *****************************************************************************/

Column::Column (int comp_opt) {
    _mtable = new Memtable;
    cout << "We created a column" << std::endl;
}


Column::~Column() {
    cout << "STARTING column destructor" << std::endl;
    delete _mtable;
}


std::string Column::read (std::string key) {
    cout << "Hello from Col" << std::endl;
    return "foo";
}


/*****************************************************************************
 *                                  SSTable                                  *
 *****************************************************************************/

SSTable::SSTable(std::string filename, SSIndex *index, char *data_array) {
    return;
}


SSTable::~SSTable(void) {
    return;
}


/*****************************************************************************
 *                                  MemTable                                 *
 *****************************************************************************/
Memtable::Memtable(void) {
    cout << "Creating new Memtable" << std::endl;

    _index = new SSIndex;
    return;
}


Memtable::~Memtable(void) {
    cout << "Deleting memtable" << std::endl;

    delete _index;
    return;

}


/*****************************************************************************
 *                                  SSIndex                                  *
 *****************************************************************************/
SSIndex::SSIndex(void) {
    cout << "Creating new Index" << std::endl;

    _bf = new BloomFilter(1);
    return;
}


SSIndex::~SSIndex(void) {
    cout << "Deleting index" << std::endl;
    delete _bf;
    return;
}


index_entry_t* SSIndex::lookup_key (string key) {
//    std::map<string, index_entry_t> it = _index.find[key];
//    if(it != _index.end()) {
//        return it;
//    } else {
//        return NULL;
//    }
//
    return NULL;
}


int SSIndex::add_key (string key, string value) {
//    index_entry_t *old_entry;
//    index_entry_t new_entry;
//
//    if((old_entry = lookup_key(key)) == NULL) {
//        new_entry.offset = curr_offset;
//        new_entry.length = strlen(value);
//        new_entry.value = value;
//        curr_offset += new_entry.length;
//        index.insert(std::pair<string, index_entry_t>(key, new_entry));
//
//    } else {
//        old_entry.value = value;
//
//    }
    return 0;
}


/*****************************************************************************
 *                                  BloomFilter                              *
 *****************************************************************************/
BloomFilter::BloomFilter(int size) {
    cout << "Creating new BloomFilter" << std::endl;

    _size = size;
    _bf   = (int*) std::malloc(sizeof(int) * size);
    return;
}


BloomFilter::~BloomFilter() {
    std::free(_bf);
    return;
}
