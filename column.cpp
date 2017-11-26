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

//#define NDEBUG
#include <cassert>

#include "inc/column.h"

using namespace std;

/*****************************************************************************
 *                                  COLUMN                                   *
 *****************************************************************************/

Column::Column (string name, int comp_opt) {
    _uuid = 0;
    _name = name;
    _compression = comp_opt;

    _mtable = new Memtable("mtable_" + _name + to_string(_uuid++));

    cout << "Creating " << _name << std::endl;
}


Column::~Column() {
    delete _mtable;

    _ronly_list.clear();
}


string Column::read (string key) {
    string ret;

    if (ret.empty()) {
        //Read from memtable
        lock_guard<mutex> lock(_mtable_lock);
        ret = _mtable->read(key);

    } if (ret.empty()) {
        //Read from secondary RONLY memtables
        lock_guard<mutex> lock(_ronly_lock);

        for (auto& ronly_iter: _ronly_list) {

            ret = ronly_iter->read(key);
            if (!ret.empty()) {
                return ret;
            }
        }

    } if (ret.empty()) {
        //Step thru the SST stack and try there
        lock_guard<mutex> lock(_sst_lock);

        for (auto& sst_iter: _sst_list) {

            ret = sst_iter->read(key);
            if (!ret.empty()) {
                return ret;
            }
        }
    }

    return ret;
}

void Column::write (string key, string value) {

    int err;

    lock_guard<mutex> mem_table_lock(_mtable_lock);

    err = _mtable->write(key, value);

    if (err) { //This memtable is full...

        lock_guard<mutex> ronly_table_lock(_ronly_lock);


        //Move the full table to the ronly reference stack
        _ronly_list.push_front(_mtable);

        //Make new memtable
        _mtable = new Memtable("mtable_" + _name + to_string(_uuid++));

        //Try the write again
        err = _mtable->write(key, value);
        assert(!err);

        if (_ronly_list.size() > 10) {

            Memtable *oldest_table = _ronly_list.back();
            _ronly_list.pop_back();

            lock_guard<mutex> sst_lock(_sst_lock);
            SSTable *new_table = new SSTable(
                                    "sst_" + _name + to_string(_uuid++),
                                    oldest_table->get_map(),
                                    0);

            _sst_list.push_front(new_table);

        }
    }

}

//Blindly delete or invalidate ALL mappings to this particular key
void Column::del (string key) {

    //Delete from memtable
    _mtable_lock.lock();
    _mtable->del(key);
    _mtable_lock.unlock();

    //Delete from RONLY tables
    _ronly_lock.lock();
    for (auto& ronly_iter: _ronly_list) {

        ronly_iter->del(key);
    }
    _ronly_lock.unlock();

    //Invalidate in all SSTables
    _sst_lock.lock();
    for (auto& sst_iter: _sst_list) {
        //Blindly invalidate for all SSTs
        sst_iter->invalidate(key);
    }
    _sst_lock.unlock();

}

/*****************************************************************************
 *                                  MemTable                                 *
 *****************************************************************************/

Memtable::Memtable(string name) {

    _name = name;
    _size = 0;

    cout << "Creating " << _name << std::endl;
    return;
}


Memtable::~Memtable(void) {

    return;

}

int Memtable::write (string key, string value) {

    //FIXME - revisit
    if (_size + value.length() <= PAGE_SIZE) {

        _map[key] = value;
        _size += value.length();
        return 0;

    } else {
        //Fail out, let the column deal with it
        return -1;
    }

}

string Memtable::read (string key) {

    string ret;

    _iter = _map.find(key);

    if (_iter != _map.end()) {

        ret = _iter->second;

    }
    return ret;

}

void Memtable::del (string key) {

    _iter = _map.find(key);
    if (_iter != _map.end()) {

        _size -= _iter->second.length();
        _map.erase(key);
    }
}

map<string, string> Memtable::get_map(void) {
    return _map;
}
/*****************************************************************************
 *                                  SSTable                                  *
 *****************************************************************************/

/***
 *
 *  Constructor for SSTable
 *
 *  uuid: The name of the location on disk to store the data
 *  map:  A <key:value> map inherited from a memtable
 *
 ***/
SSTable::SSTable(string uuid,
                 map<string, string> data_map,
                 int compress_opt) {

    ofstream outfile;
    string key, key_header, data;
    long length, offset;

    //SST inherits a Memtables map
    _name = uuid;
    _filename = uuid;
    _file_len = 0;
    _compression_type = compress_opt;

    cout << "Creating " << _name << std::endl;

    //Open the file as append only
    outfile.open(_name.c_str(), ofstream::trunc);

    if(outfile.is_open()) {

        //Write each entry to the file, save a mapping in our index
        for (auto& iter: data_map) {

            key.assign(iter.first);
            key_header.assign(key + "\n");
            data.assign(iter.second + "\n");
            length = data.length();

            //Write the key header
            outfile.write(key_header.c_str(), key_header.length());
            //Calculate offset value for our index
            offset = outfile.tellp();
            //Write the data
            outfile.write(data.c_str(), data.length());

            //Map to our index
            index_entry_t *new_entry = new index_entry_t;
            new_entry->offset = offset;
            new_entry->len = length;
            new_entry->valid = true;
            _index[key] = new_entry;

            if(!outfile.good()) {
                //OOPS, write error
                cout <<"ERROR: writing: " << key << "to " << _name <<std::endl;
            }
        }

        //Save the file length
        _file_len = outfile.tellp();

        //Close the file
        outfile.close();
        if (!outfile.good()) {
            //OOPS, closing error
            cout << "ERROR: " << _filename << " cannot be closed" << std::endl;
        }
    } else {
        //Opening error
        cout << "ERROR: " << _filename << " cannot be opened" << std::endl;
        return;
    }

    cout << _filename << " created successfully!" << std::endl;
}

/***
 *
 *  Destructor for SSTable
 *
 ***/
SSTable::~SSTable(void) {

    return;
}

/*
 *
 */
void SSTable::invalidate(string key) {

    _iter = _index.find(key);
    if (_iter != _index.end()) {
        _iter->second->valid = false;
    }
}

/*
 *
 */
long SSTable::get_file_len() {
    return _file_len;
}

/***
 *
 *  Reads a value from the SST on disk
 *
 *  Seeks immediately to the location in the file by looking up the key in
 *  the index before opening the file.
 *
 ***/
string SSTable::read(string key) {

    string ret;
    long offset;

    ifstream infile(_filename.c_str());
    _iter = _index.find(key);

    if (_iter != _index.end()) {

        index_entry_t *entry = _iter->second;

        //Data is in SSTable
        if (entry->valid) {

            offset = entry->offset;

            infile.seekg(offset);

            std::getline(infile, ret);

            if(!infile.good()) {
                cout <<"ERROR: Reading "<<key<<" from "<<_name<<std::endl;
                //ret.clear();
            }
        }
    }

    return ret;
}

bool SSTable::peek(string key) {

    _iter = _index.find(key);

    return (_iter != _index.end());

}
/***
 *
 *  Merges two SSTables together into one file on disk
 *
 ***/
int SSTable::merge_into_table(SSTable new_table, long table_offset) {

    string new_block, data_key, data;
    long rel_offset, local_offset, data_len;
    map<string, index_entry_t*> new_map;

    ifstream infile(_filename.c_str());

    for (_iter = _index.begin(); _iter != _index.end(); _iter++) {

        //  key is not already in new table ---- key has valid entry
        if ((!new_table.peek(_iter->first)) and (_iter->second->valid)) {
            //Found an entry to add!
            data_key = _iter->first;
            data_len = _iter->second->len;
            local_offset = _iter->second->offset;

            //Read data from local file
            infile.seekg(local_offset);
            std::getline(infile, data);

            //Write to data block
            new_block.append(data_key + "\n");
            rel_offset = new_block.length();
            new_block.append(data + "\n");

            //Add entry to map
            index_entry_t *new_entry = new index_entry_t;
            new_entry->offset = table_offset + rel_offset;
            new_entry->len = data_len;
            new_entry->valid = true;
            new_map[data_key] = new_entry;

        }
    }

    //Instruct the newer table to add the new made block of data
    return new_table.append_data_block(new_block, new_map, _compression_type);
}

int SSTable::append_data_block(string data,
                               map<string, index_entry_t*> new_index,
                               int compression_opt) {

    ofstream outfile;
    long data_length = data.length();

    if (compression_opt != _compression_type) {
        cout <<"ERROR: cant merge data with different compression type!\n";
        return -1;
    }

    outfile.open(_name.c_str(), ofstream::app);
    outfile.write(data.c_str(), data_length);
    outfile.close();

    _file_len = outfile.tellp();

    if (!outfile.good()) {
        //FIXME - Actually clean up........
        cout <<"ERROR: could not append data block for " << _name<<std::endl;
        return -1;

    }

    _index.insert(new_index.begin(), new_index.end());

    return 0;
}
/*****************************************************************************
 *                                  BloomFilter                              *
 *****************************************************************************/
//BloomFilter::BloomFilter(int size) {
//    cout << "Creating new BloomFilter" << std::endl;
//
//    _size = size;
//    _bf   = (int*) std::malloc(sizeof(int) * size);
//    return;
//}
//
//
//BloomFilter::~BloomFilter() {
//    std::free(_bf);
//    return;
//}

/*****************************************************************************
 *                                  SSIndex                                  *
 *****************************************************************************/

//SSIndex::SSIndex(string name) {
//
//    _name = name;
//
//    cout << "Creating " << _name << std::endl;
//    return;
//}
//
//SSIndex::~SSIndex(void) {
//    return;
//}
//
///*
// * SSIndex.lookup(key)
// */
//index_entry_t* SSIndex::lookup (string key) {
//
//    _iter = _index.find(key);
//    if (_iter != _index.end()) {
//        return _iter->second;
//    }
//    return NULL;
//}
//
///*
// * SSIndex.erase(key)
// */
//void SSIndex::erase (string key) {
//    //FIXME - free the index_entry_t?
//    _index.erase(key);
//}
//
///*
// * SSIndex.invalidate(key)
// */
//void SSIndex::invalidate (string key) {
//
//    _iter = _index.find(key);
//
//    if (_iter != _index.end()) {
//        //Key is mapped! Invalidate it
//        _iter->second->valid = false;
//    }
//}
//
///*
// * SSIndex.map(key, offset, len)
// */
//void SSIndex::map (string key, int offset, int length) {
//
//    _iter = _index.find(key);
//
//    if (_iter != _index.end()) {
//        //Remap existing entry
//        _iter->second->offset = offset;
//        _iter->second->len = length;
//        _iter->second->valid = true;
//        cout << _name << " remapped: " << key << std::endl;
//
//    } else {
//        //Make a new map entry
//        index_entry_t *new_entry = new index_entry_t;
//        new_entry->offset = offset;
//        new_entry->len = length;
//        new_entry->valid = true;
//
//        //Insert it
//        _index.insert(
//            std::map<string, index_entry_t*>::value_type(key, new_entry)
//        );
//        cout << _name << " mapped: " << key << std::endl;
//    }
//}

/***
 *
 *  Consumes the index called with this function by adding any non-overlapping
 *  key:value pairs to this index
 *
 ***/
//int SSIndex::consume_index(SSIndex *old_index) {
//    //insert each of my keys into the newer index
//    index_entry_t *my_entry;
//    index_entry_t *new_entry;
//    string my_key;
//    _iter = _index.begin();
//
//    while(_iter != _index.end()) {
//        my_entry = _iter->second;
//        my_key = _iter->first;
//
//        //Only insert into new index if key was never seen
//        new_entry = new_index->lookup(my_key);
//        if(new_entry == NULL) {
//            cout << "inserting " << my_key << " into new index" << std::endl;
//            new_index->map(my_key,
//                           my_entry->offset + new_size,
//                           my_entry->len);
//        }
//
//        _iter++;
//    }
//
//    return 0;
//}


