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
    cout << "Creating new Column" << std::endl;
}


Column::~Column() {
    delete _mtable;
}


string Column::read (string key) {
    cout << "Hello from Col read" << std::endl;
    return "foo";
}

void Column::write (string key, string value) {
    cout << "Hello from Col write" << std::endl;
    return "foo";
}

void Column::del (string key) {
    cout << "Hello from Col del" << std::endl;
    return "foo";
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

    delete _index;
    return;

}

void Memtable::write (string key, string value) {

    int curr_offset = _data.length();
    int next_offset = curr_offset + value.length();

    if (next_offset <= PAGE_SIZE) {

        //Write the value to our byte array
        _data.append(value);

        //Add the key to the index
        _index->map_key(key, curr_offset, value.length());

    } else {
        //hmmmmm... spill to disk?
        std::cout << "implement me...\n";
    }

    cout << "Just wrote [" << key << ":" << value << "] to memtable!\n";
}

string Memtable::read (string key) {

    string ret;
    index_entry_t *entry = _index->lookup_key(key);

    if (entry && entry->valid) {

        ret.assign(_data, entry->offset, entry->len);

    }

    cout << "Just read [" << key << ":" << ret << "] from memtable!\n";
    return ret;

}

//Does not hard remove anything. Merely suggests the data is removed
//by setting the valid bit in the index to false
void Memtable::del (string key) {

    cout << "Deleting this guy: " << key << " from memtable!\n";
    _index->invalidate_key(key);

}
/*****************************************************************************
 *                                  SSIndex                                  *
 *****************************************************************************/
SSIndex::SSIndex(void) {
    cout << "Creating new Index" << std::endl;
    return;
}

SSIndex::~SSIndex(void) {
    return;
}

/*
 * SSIndex.lookup(key)
 */
index_entry_t* SSIndex::lookup_key (string key) {

    _iter = _index.find(key);
    if (_iter != _index.end()) {
        return _iter->second;
    }
    return NULL;
}

/*
 * SSIndex.erase(key)
 */
void SSIndex::erase (string key) {
    //FIXME - free the index_entry_t?
    _index.erase(key);
}

/*
 * SSIndex.invalidate(key)
 */
void SSIndex::invalidate (string key) {

    _iter = _index.find(key);

    if (_iter != _index.end()) {
        //Key is mapped! Invalidate it
        _iter->second->valid = false;
    }
}

/*
 * SSIndex.map(key, offset, len)
 */
void SSIndex::map (string key, int offset, int length) {

    _iter = _index.find(key);

    if (_iter != _index.end()) {
        //Key already mapped, update it
        _iter->second->offset = offset;
        _iter->second->len = length;
        _iter->second->valid = true;

    } else {
        //Make a new map entry
        index_entry_t *new_entry = new index_entry_t;
        new_entry->offset = offset;
        new_entry->len = length;
        new_entry->valid = true;

        //Insert it
        _index.insert(
            std::map<string, index_entry_t*>::value_type(key, new_entry)
        );
    }
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
