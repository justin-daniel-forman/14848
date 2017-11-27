/***
 *
 *  column.cpp
 *
 *  Contains definitions for class functions needed to store a column
 *  in our NoSQL database.
 *
 *  Authors: Mark Wuebbens
 *
 ***/

#include <iostream>
#include <cstdlib>
#include <fstream>
#include <chrono>
#include <vector>

//#define NDEBUG
#include <cassert>

#include "inc/column.h"
#include "inc/bloomfilter.h"

using namespace std;

/*****************************************************************************
 *                                  COLUMN                                   *
 *****************************************************************************/

Column::Column (string name, int comp_opt) {

    _name = name;
    _compression_opt = comp_opt;
    _table_uid = 0;
    _sst_uid = 0;

    long table_id = _table_uid++;

    _mtable = new Memtable(_name+"_mtable"+to_string(table_id), table_id);

    //Spawn off background threads to handle dumping to disk and compaction
    //dumper = thread (&Column::Dump_Master, this).detach();
    thread (&Column::Dump_Master, this).detach();
    //thread compact_master (&Column::Compact_Master, this);

    cout << "Created " << _name << std::endl;
}


Column::~Column() {
    delete _mtable;

    //FIXME - moar better
    //dumper.join();
    //_tables_map.clear();
}


string Column::read (string key) {
    int not_found;
    string ret;

    unique_lock<mutex> MEM_LOCK(_mtable_lock, std::defer_lock);
    unique_lock<mutex> TABLES_LOCK(_tables_lock, std::defer_lock);
    unique_lock<mutex> SST_LOCK(_sst_lock, std::defer_lock);

    //Read from memtable
    MEM_LOCK.lock();
    not_found = _mtable->read(key, &ret);
    if (!not_found) return ret;
    MEM_LOCK.unlock();

    //Read from secondary RONLY memtables
    TABLES_LOCK.lock();
    for (auto& table_iter: _tables_map) {

        not_found = table_iter.second->read(key, &ret);
        if (!not_found) return ret;
    }
    TABLES_LOCK.unlock();

    //Read from the on disk SSTs
    SST_LOCK.lock();
    for (auto& sst_iter: _sst_map) {

        not_found = sst_iter.second->read(key, &ret);
        if (!not_found) return ret;
    }
    SST_LOCK.unlock();

    return ret;
}

void Column::write (string key, string value) {

    int err = -1;

    unique_lock<mutex> MEM_LOCK(_mtable_lock, std::defer_lock);
    unique_lock<mutex> TABLES_LOCK(_tables_lock, std::defer_lock);

    MEM_LOCK.lock();
    err = _mtable->write(key, value);

    if (err) { //The memtable is full...

        //Move full table to our full table map
        TABLES_LOCK.lock();
        _tables_map[_mtable->get_uid()] = _mtable;
        assert(_tables_map.size() < 10000);

        long new_table_uid = _table_uid++;
        TABLES_LOCK.unlock();

        //Make new memtable
        _mtable = new Memtable("mtable_" + _name + to_string(new_table_uid),
                               new_table_uid);
        //Try the write again
        err = _mtable->write(key, value);
        assert(!err);
    }

    MEM_LOCK.unlock();
}

//Map an empty string to this key, thus later reads for key will return nothing
void Column::del (string key) {
    this->write(key, "");

    //unique_lock<mutex> MEM_LOCK(_mtable_lock, std::defer_lock);
    //unique_lock<mutex> TABLES_LOCK(_tables_lock, std::defer_lock);
    //unique_lock<mutex> SST_LOCK(_sst_lock, std::defer_lock);

    ////Delete from memtable
    //MEM_LOCK.lock();
    //_mtable->del(key);
    //MEM_LOCK.unlock();

    ////Delete from RONLY tables
    //TABLES_LOCK.lock();
    //for (auto& iter: _tables_map) {

    //    if (!iter.second->is_taken()) {
    //        iter.second->del(key);
    //    }
    //}
    //TABLES_LOCK.unlock();

    ////Invalidate in all SSTables
    //SST_LOCK.lock();
    //for (auto& sst_iter: _sst_map) {
    //    //Blindly invalidate for all SSTs
    //    sst_iter->invalidate(key);
    //}
    //SST_LOCK.unlock();

}

void Column::dump_map_to_disk (Dump_Container *data) {

    //Create an SST from the provided map
    string sst_name = "sst" +  to_string(data->sst_uid) + "_" + _name;

    data->new_sst = new SSTable(sst_name, data->raw_map, _compression_opt);

}

void Column::Compact_Master() {

    unique_lock<mutex> TABLE_LOCK(_tables_lock, std::defer_lock);
    unique_lock<mutex> SST_LOCK(_sst_lock, std::defer_lock);

    deque<Compact_Worker*> workers;

    int delay_ms = 10;

    while(1) {

        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

        //Initiate Compaction Stuffs
        if (_sst_map.size() < 10) {
            //Nothing to do
            delay_ms += 1;

        } else {
            //Initiate compacting two SSTs together
            //long newer_uid, older_uid;
            delay_ms = (delay_ms * 4) / 5;
            SST_LOCK.lock();
            //FIXME -implement
            SST_LOCK.unlock();
        }

        //Join on Workers, and finalize a compaction

    }
}
void Column::Dump_Master() {

    unique_lock<mutex> TABLE_LOCK(_tables_lock, std::defer_lock);
    unique_lock<mutex> SST_LOCK(_sst_lock, std::defer_lock);

    vector<Dump_Container> data;
    vector<thread> workers;

    int delay_ms = 10;

    while(1) {

        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        cout << "IM AWAKE!!!!!" << std::endl;

        //Initiate Dumps to Disk
        if (_tables_map.size() < 10) {
            //Nothing to do, might as well sleep
            delay_ms += 1;

        } else {
            //Initiate splilling a table to disk
            long table_uid, sst_uid;
            map<string, string> table_raw;

            delay_ms = (delay_ms * 4) / 5;

            //Find the oldest table to dump
            TABLE_LOCK.lock();
            for(auto rit=_tables_map.rbegin();
                rit!=_tables_map.rend();
                ++rit) {

                if (!rit->second->is_taken()) {
                    //Take and flag this table atomically
                    table_raw = rit->second->take_map();
                    table_uid = rit->first;
                    break;
                }
            }
            assert(!table_raw.empty());
            TABLE_LOCK.unlock();

            //Get the next SST uid
            SST_LOCK.lock();
            sst_uid = _sst_uid++;
            SST_LOCK.unlock();

            //Spawn a thread to create the SST
            Dump_Container worker_data;
            worker_data.table_uid = table_uid;
            worker_data.sst_uid = sst_uid;
            worker_data.raw_map = table_raw;

            workers.push_back(
                thread (&Column::dump_map_to_disk, this, &worker_data));

            data.push_back(worker_data);
        }

        //Attempt to join on oldest Worker
        auto& thread = workers[0];
        auto meta = data[0];
        if (thread.joinable()) {

            //This worker is done! Wipe up fastidiously
            thread.join();

            //Add the new sst to our sst_list
            SST_LOCK.lock();
            _sst_map[meta.sst_uid] = meta.new_sst;
            SST_LOCK.unlock();

            //Remove the now redundant table
            TABLE_LOCK.lock();
            _tables_map.erase(meta.table_uid);
            TABLE_LOCK.unlock();

            workers.erase(workers.begin());
            data.erase(data.begin());
        }

    } //while(1)

}

/*****************************************************************************
 *                                  MemTable                                 *
 *****************************************************************************/

Memtable::Memtable(string name, long uid) {

    _name = name;
    _uid = uid;
    _size = 0;
    _taking_dump = false;

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

int Memtable::read (string key, string *out_str) {

    auto iter = _map.find(key);
    if (iter != _map.end()) {

        out_str->assign(iter->second);
        return 0;

    }
    return -1;

}

int Memtable::del (string key) {

    auto iter = _map.find(key);
    if (iter != _map.end()) {

        _size -= iter->second.length();
        _map.erase(key);
    }

    return 0;
}

long Memtable::get_uid() {
    return _uid;
}

bool Memtable::is_taken() {
    return _taking_dump;
}

map<string, string> Memtable::take_map(void) {
    _taking_dump = true;
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
    _compression_opt = compress_opt;

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
}

/***
 *
 *  Destructor for SSTable
 *
 ***/
SSTable::~SSTable(void) {

    //FIXME - delete the index_entry_t values

    return;
}

/*
 *
 */
void SSTable::invalidate(string key) {

    auto iter = _index.find(key);
    if (iter != _index.end()) {
        iter->second->valid = false;
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
int SSTable::read(string key, string *out_str) {

    long offset;

    ifstream infile(_filename.c_str());
    auto iter = _index.find(key);

    if (iter != _index.end()) {

        index_entry_t *entry = iter->second;

        //Data is in SSTable
        if (entry->valid) {

            offset = entry->offset;

            infile.seekg(offset);

            std::getline(infile, *out_str);

            if(!infile.good()) {
                cout <<"ERROR: Reading "<<key<<" from "<<_name<<std::endl;
                //ret.clear();
            }

            //FOUND!
            return 0;
        }
    }

    //NOT FOUND!
    return -1;
}

bool SSTable::peek(string key) {

    auto iter = _index.find(key);

    return (iter != _index.end());

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

    for (auto& iter : _index) {

        //  key is not already in new table ---- key has valid entry
        if ((!new_table.peek(iter.first)) and (iter.second->valid)) {
            //Found an entry to add!
            data_key = iter.first;
            data_len = iter.second->len;
            local_offset = iter.second->offset;

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
    return new_table.append_data_block(new_block, new_map, _compression_opt);
}

int SSTable::append_data_block(string data,
                               map<string, index_entry_t*> new_index,
                               int compression_opt) {

    ofstream outfile;
    long data_length = data.length();

    if (compression_opt != _compression_opt) {
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
BloomFilter::BloomFilter(int size) {
    cout << "Creating new BloomFilter" << std::endl;

    _size = size;
    _bf   = (int*) std::malloc(sizeof(int) * size);
    memset(_bf, 0, sizeof(int)*size);

    return;
}


BloomFilter::~BloomFilter() {
    std::free(_bf);
    return;
}

bool BloomFilter::check(std::string k) {

    unsigned int idx0 = h0(k);
    unsigned int idx1 = h1(k);
    unsigned int idx2 = h2(k);

    std::cout << idx0 << ", " << idx1 << ", " << idx2 << std::endl;

    return (_bf[idx0] & _bf[idx1] & _bf[idx2]);

}

void BloomFilter::insert(std::string k) {

    std::cout << "Inserting " << k << std::endl;

    _bf[h0(k)] = 1;
    _bf[h1(k)] = 1;
    _bf[h2(k)] = 1;

    return;
}

unsigned int BloomFilter::h0(std::string k) {

    unsigned int sum = 0;

    //Some random fast hash
    for(auto& c : k) {
        sum = (sum ^ c) + (c*c);
    }

    return sum % _size;
}

unsigned int BloomFilter::h1(std::string k) {

    unsigned int sum = 0;

    for(auto& c : k) {
        sum = (sum + c) * c;
    }

    return sum % _size;

}

unsigned int BloomFilter::h2(std::string k) {

    unsigned int sum = 0;

    for(auto& c : k) {
        sum = (c*c*c) + sum;
    }

    return sum % _size;

}

