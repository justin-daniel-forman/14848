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

        cout << "Just wrote [" << key << ":" << value << "] " << "size: " << value.length() <<  " to memtable!\n";
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
        _iter->second->offset = offset;
        _iter->second->len = length;
        _iter->second->valid = true;

    } else {
        cout << "Make a new entry" << std::endl;

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

/***
 *
 *  Call this function on the older index to loop through and insert each
 *  of its entries one by one into the newer index
 *
 ***/
int SSIndex::merge_newer_index(SSIndex *new_index, int new_size) {
    //insert each of my keys into the newer index
    index_entry_t *old_entry;
    index_entry_t *new_entry;
    string key;
    _iter = _index.begin();

    while(_iter != _index.end()) {
        old_entry = _iter->second;
        key = _iter->first;

        //Only insert into new index if key was never seen
        new_entry = new_index->lookup_key(key);
        if(new_entry == NULL) {
            cout << "inserting " << key << " into new index" << std::endl;
            cout << "old len: " << old_entry->len << std::endl;
            new_index->map_key((&key)->c_str(), old_entry->offset + new_size, old_entry->len);
        }

        _iter++;
    }

    return 0;
}


/*****************************************************************************
 *                                  SSTable                                  *
 *****************************************************************************/

/***
 *
 *  Constructor for SSTable
 *
 *  We create an index by passing in pointers to the index and data. The index
 *  should not be deleted, but the data is copied, so it is fine to dispose
 *  of that after this constructor is called.
 *
 *  filename:   The name of the location on disk to store the data
 *  index:      The index created while constructing the Memtable
 *  data:       A byte array of data to store on disk
 *
 *
 ***/
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
    _size = strlen(data);
    fp.close();

    //Error handling happens after closing the file. A flag will have been
    //set if the write was unsuccessful
    if(!fp) {
        cout << filename << " could not be written to, ERROR" << std::endl;
        memset(_filename, '\0', sizeof(_filename));
    }

    return;
}

/***
 *
 *  Destructor for SSTable
 *
 ***/
SSTable::~SSTable(void) {
    delete _index;
    return;
}


/***
 *
 *  Reads a value from the SST on disk
 *
 *  Seeks immediately to the location in the file by looking up the key in
 *  the index before opening the file.
 *
 ***/
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

/***
 *
 *  Exposes the index of the SSTable for merging purposes
 *
 ***/
SSIndex* SSTable::get_index(void) {
    return _index;
}

/***
 *
 *  Exposes the filename of the SSTable for merging purposes
 *
 ***/
char* SSTable::get_filename(void) {
    return _filename;
}

/***
 *
 *  Merges two SSTables together into one file on disk
 *
 *  This involves merging the two indices together and then just writing
 *  both of the data arrays to disk consecutively and adjusting the offsets.
 *
 ***/
int SSTable::merge_older_table(SSTable *old_table) {

    ofstream op;
    ifstream ip;
    SSIndex  *old_index = old_table->get_index();

    //Merge the two indices together and update offset of every old index entry
    old_index->merge_newer_index(_index, _size);

    op.open(_filename, std::fstream::app);
    ip.open(old_table->get_filename(), std::ofstream::binary);

    if(!ip.is_open() || !op.is_open()) {
        return -1;
    }

    op << ip.rdbuf();
    op.close();
    ip.close();

    if(!ip || !op) {
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
