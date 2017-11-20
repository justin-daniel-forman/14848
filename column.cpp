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
#include <fstream>
#include <string.h>

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


std::string Column::read (std::string key) {
    cout << "Hello from Col read" << std::endl;
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

const char* Memtable::get_data(void) {
    return _data.c_str();
}

SSIndex* Memtable::get_index(void) {
    return _index;
}


int Memtable::write (string key, string value) {

    int curr_offset = _data.length();
    int next_offset = curr_offset + value.length();

    if (next_offset <= PAGE_SIZE) {

        //Write the value to our byte array
        _data.append(value);

        //Add the key to the index
        _index->map_key(key, curr_offset, value.length());

        cout << "Just wrote [" << key << ":" << value << "] to memtable!\n";
        return 0;


    } else {
        return -1;
    }

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

index_entry_t* SSIndex::lookup_key (string key) {

    _iter = _index.find(key);
    if (_iter != _index.end()) {
        return _iter->second;
    }
    return NULL;
}

void SSIndex::erase_key (string key) {
    _index.erase(key);
}

void SSIndex::invalidate_key (string key) {

    _iter = _index.find(key);

    if (_iter != _index.end()) {
        //Key is mapped! Invalidate it
        _iter->second->valid = false;
    }
}

void SSIndex::map_key (string key, int offset, int length) {

    _iter = _index.find(key);

    if (_iter != _index.end()) {
        //Key already mapped, update its offset
        _iter->second->offset = offset;
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

SSTable::SSTable(const char *filename, SSIndex *index, const char *data) {

    ofstream fp;

    //SST inherits old Memtable index and data
    _index    = index;
    memcpy(_filename, filename, sizeof(_filename));

    //On error, set filename to be ""
    fp.open(filename, std::ofstream::binary);
    if(!fp.is_open()) {
        cout << filename << " cannot be opened, ERROR" << std::endl;
        memset(_filename, '\0', sizeof(_filename));
    }

    fp.write(data, strlen(data));
    fp.close();

    //Error handling happens after closing the file. A flag will have been
    //set if the write was unsuccessful
    if(!fp) {
        cout << filename << " could not be written to, ERROR" << std::endl;
        memset(_filename, '\0', sizeof(_filename));
    }

    return;
}


SSTable::~SSTable(void) {
    delete _index;
    return;
}


int SSTable::read(std::string key, char *result) {

    ifstream fp;
    index_entry_t *entry = _index->lookup_key(key);

    //Data is in SSTable
    if (entry && entry->valid) {

        fp.open(_filename);
        fp.seekg(entry->offset, fp.beg);
        fp.read(result, entry->len);
        fp.close();

        if(!fp) {
            cout << "Something went wrong reading from SST" << std::endl;
            return -1;
        }

    //Data is not in SSTable
    } else {
        return -1;
    }

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
