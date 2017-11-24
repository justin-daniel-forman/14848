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

#include "inc/column.h"

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
    //FIXME
    //delete _mtable;
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

//int Column::merge_sst(SSTable *curr_table, SSTable *old_table) {
//
//    SSIndex old_index = old_table->get_index();
//    string old_file_name = old_table->get_filename();
//    
//}



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

    if (_size + value.length() <= PAGE_SIZE) {

        _map[key] = value;

        cout << _name << " wrote [" << key << ":" << value << "]\n";
        return 0;


    } else {
        //FIXME - implement still
        cout << "STIL NEED TO IMPLEMENT SPILL TO DISK...\n";
        return -1;
    }

}

string Memtable::read (string key) {

    string ret;

    _iter = _map.find(key);

    if (_iter != _map.end()) {

        ret = _iter->second;
        cout << _name << " read [" << key << ":" << ret << "]\n";

    }
    return ret;

}

//Does not hard remove anything. Merely suggests the data is removed
//by setting the valid bit in the index to false
void Memtable::del (string key) {

    if (_map.erase(key)) {
        cout << _name << " deleted: " << key << std::endl;
    }

}

std::map<std::string, std::string> Memtable::get_map(void) {
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
SSTable::SSTable(string uuid, map<string, string> map) {

    ofstream outfile;

    //SST inherits a Memtables map
    _name = uuid;
    _filename = uuid;

    string key, key_header, data;
    long length, offset;
    cout << "Creating " << _name << std::endl;

    //Open the file as append only
    outfile.open(_name.c_str(), ofstream::trunc);

    if(outfile.is_open()) {

        //Write each entry to the file, save a mapping in our index
        for (auto& iter: map) {

            key.assign(iter.first);
            key_header.assign("<" + key + ">");
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
    long offset;

    ifstream infile(_filename.c_str());
    _iter = _index.find(key);

    if (_iter != _index.end()) {

        index_entry_t *entry = _iter->second;

        //Data is in SSTable
        if (entry->valid) {

            offset = entry->offset;

            infile.seekg(offset);

            //char *buf = new char[len];
            //infile.readsome(ret.buf, len);
            //ret.assign(buf);
            //delete buf;

            std::getline(infile, ret);

            if(!infile.good()) {
                cout <<"ERROR: Reading "<<key<<" from "<<_name<<std::endl;
                //ret.clear();
            }
        }
    }

    return ret;
}


/***
 *
 *  Merges two SSTables together into one file on disk
 *
 *  This involves merging the two indices together and then just writing
 *  both of the data arrays to disk consecutively and adjusting the offsets.
 *
 ***/
//int SSTable::merge_older_table(SSTable *old_table) {
//
//    ofstream op;
//    ifstream ip;
//    SSIndex  *old_index = old_table->get_index();
//
//    //Merge the two indices together and update offset of every old index entry
//    old_index->merge_newer_index(_index, _size);
//
//    op.open(_filename.c_str(), std::fstream::app);
//    ip.open(old_table->get_filename().c_str(), std::ofstream::binary);
//
//    if(!ip.is_open() || !op.is_open()) {
//        return -1;
//    }
//
//    op << ip.rdbuf();
//    op.close();
//    ip.close();
//
//    if(!ip || !op) {
//        return -1;
//    }
//
//    return 0;
//}

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


