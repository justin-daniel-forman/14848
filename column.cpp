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

Column::Column (string name, int comp_opt) {
    _name = name;
    _mtable = new Memtable(name.append("_mtable"));
    cout << "Creating " << _name << std::endl;
}


Column::~Column() {
    delete _mtable;
}


string Column::read (string key) {

    string ret = "foo";
    cout <<  _name << " read (" << key << ":" << ret << ")\n";
    return ret;
}

void Column::write (string key, string value) {
}

void Column::del (string key) {
}



/*****************************************************************************
 *                                  MemTable                                 *
 *****************************************************************************/

Memtable::Memtable(string name) {

    _name = name;
    _index = new SSIndex(name.append("_index"));

    cout << "Creating " << _name << std::endl;
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
        _index->map(key, curr_offset, value.length());

        cout << _name << " wrote [" << key << ":" << value << "]\n";
        return 0;


    } else {
        return -1;
    }

}

string Memtable::read (string key) {

    string ret;
    index_entry_t *entry = _index->lookup(key);

    if (entry && entry->valid) {

        ret.assign(_data, entry->offset, entry->len);

    }

    cout << _name << " read [" << key << ":" << ret << "]\n";
    return ret;

}

//Does not hard remove anything. Merely suggests the data is removed
//by setting the valid bit in the index to false
void Memtable::del (string key) {

    cout << _name << " deleted: " << key << std::endl;
    _index->invalidate(key);

}

/*****************************************************************************
 *                                  SSIndex                                  *
 *****************************************************************************/

SSIndex::SSIndex(string name) {

    _name = name;

    cout << "Creating " << _name << std::endl;
    return;
}

SSIndex::~SSIndex(void) {
    return;
}

/*
 * SSIndex.lookup(key)
 */
index_entry_t* SSIndex::lookup (string key) {

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
        //Remap existing entry
        _iter->second->offset = offset;
        _iter->second->len = length;
        _iter->second->valid = true;
        cout << _name << " remapped: " << key << std::endl;

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
        cout << _name << " mapped: " << key << std::endl;
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
    index_entry_t *my_entry;
    index_entry_t *new_entry;
    string my_key;
    _iter = _index.begin();

    while(_iter != _index.end()) {
        my_entry = _iter->second;
        my_key = _iter->first;

        //Only insert into new index if key was never seen
        new_entry = new_index->lookup(my_key);
        if(new_entry == NULL) {
            cout << "inserting " << my_key << " into new index" << std::endl;
            new_index->map(my_key,
                           my_entry->offset + new_size,
                           my_entry->len);
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
 *  uuid:   The name of the location on disk to store the data
 *  index:      The index created while constructing the Memtable
 *  data:       A string of data to store on disk
 *
 *
 ***/
SSTable::SSTable(string uuid, SSIndex *index, string data) {

    ofstream fp;

    //SST inherits old Memtable index and data
    _index = index;
    _name = uuid;
    _filename = uuid;
    _size = data.length();
    _valid = false; //Hacky error handling for now

    cout << "Creating " << _name << std::endl;

    //Open the on disk file
    fp.open(_name.c_str());
    if(fp.is_open()) {

        //Write the data
        fp.write(data.c_str(), data.length());
        if(fp.good()) {

            //Close the file for now
            fp.close();
            if (fp.good()) {
                //SUCCESS!
                _valid = true;

            } else {
                //Closing error
                cout << _filename << " cannot be closed, ERROR" << std::endl;
            }
        } else {
            //Writing error
            cout << _filename << " cannot be written, ERROR" << std::endl;
        }
    } else {
        //Opening error
        cout << _filename << " cannot be opened, ERROR" << std::endl;
    }
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
string SSTable::read(std::string key) {

    string ret;
    ifstream fp(_filename.c_str());
    index_entry_t *entry = _index->lookup(key);
    int offset, len;

    //Data is in SSTable
    if (entry && entry->valid) {

        offset = entry->offset;
        len = entry->len;

        ret.assign(ios::beg + offset, ios::beg + offset + len);

        if(!fp.good()) {
            cout << "Error reading from SST " << _name << std::endl;
            ret.clear();
        }
    }

    return ret;
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
string SSTable::get_filename(void) {
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

    op.open(_filename.c_str(), std::fstream::app);
    ip.open(old_table->get_filename().c_str(), std::ofstream::binary);

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
