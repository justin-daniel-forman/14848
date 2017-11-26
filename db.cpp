#include <iostream>
#include <string>
#include <map>
#include <set>
#include <list>

#include "inc/columnfamily.h"

/*****************************************************************************
 *                             SEARCH RESULT                                 *
 *****************************************************************************/

Search_Result::~Search_Result(void) {
    std::map <std::string, std::list<std::string>*>::iterator kv;

    kv = _table.begin();
    while(kv != _table.end()) {
        delete kv->second;
        kv++;
    }

}

void Search_Result::add_row(std::string key, std::list <std::string> *row) {

    _table[key] = row;

    return;

}

void Search_Result::add_col(std::string col) {
    _col_names.push_back(col);

}

void Search_Result::print_result(void) {

    std::map <std::string, std::list<std::string>* >::iterator miter;
    std::list <std::string>::iterator siter;

    //Print out column names
    std::cout << "\t| ";
    siter = _col_names.begin();
    while(siter != _col_names.end()) {
        std::cout << *siter << "\t| ";
        siter++;
    }
    std::cout << std::endl;


    //Print out row
    miter = _table.begin();
    while(miter != _table.end()) {

        std::cout << miter->first << "\t| ";

        siter = miter->second->begin();
        while(siter != miter->second->end()) {
            std::cout << *siter << "\t| ";
            siter++;
        }
        std::cout << std::endl;


        miter++;
    }

    return;
}


/*****************************************************************************
 *                             COLUMN_FAMILY                                 *
 *****************************************************************************/

/***
 *
 *  Creates a new column family from a list of names and compression
 *  option.
 *
 ***/
Column_Family::Column_Family(std::set<std::string> *col_set, int copt) {

    std::set<std::string>::iterator iter;
    Column *my_col;

    iter = col_set->begin();
    while(iter != col_set->end()) {
        my_col = new Column(*iter, copt);
        _cols[*iter] = my_col;
        iter++;
    }

}


/***
 *
 *  Removes each column in the column family. Deleting each column should
 *  remove all of the SSTables on disk as well.
 *
 ***/
Column_Family::~Column_Family(void) {

    _col_iter = _cols.begin();
    while(_col_iter != _cols.end()) {
        delete (_col_iter->second);
        _col_iter++;
    }

}


/***
 *
 *  Pulls out a set of rows from the column family between the range of
 *  keys min and max.
 *
 *  If either min or max is the empty string, the select will search until
 *  the lower or upper bound of the keyspace respectively.
 *
 ***/
int Column_Family::cf_select(Search_Result *r, std::string min, std::string max) {

    std::set <std::string>::iterator lb;
    std::set <std::string>::iterator ub;
    std::list <std::string> *results;

    //Check that user has allocated space for the result
    if(r == NULL) {
        return -1;
    }

    lb = _idx.lower_bound(min);
    ub = _idx.upper_bound(max);

    _idx_iter = lb;
    while(_idx_iter != ub) {

        //Retrieve the value from each of the columns
        results = new std::list<std::string>;
        _col_iter = _cols.begin();

        while(_col_iter != _cols.end()) {

            //record the columns exactly once
            if(_idx_iter == lb) {
                r->add_col(_col_iter->first);
            }

            //gather the entry from each column
            results->push_back(_col_iter->second->read(*_idx_iter));
            _col_iter++;
        }

        //Add the results to the search result
        r->add_row(*_idx_iter, results);

        _idx_iter++;
    }

    return 0;

}


/***
 *
 *  Inserts the entry represented by the map of column->val into the column
 *  family. If the map is ill formed (does not contain all columns, or there
 *  is a column mismatch, then the insert is discared).
 *
 ***/
int Column_Family::cf_insert(std::string row_key,
    std::map<std::string, std::string> *m) {

    std::string value;
    std::map<std::string, std::string>::iterator entry_iter;

    //Check that each entry is valid
    entry_iter = m->begin();
    while(entry_iter != m->end()) {

        //Check that name in map is actually a column in this family
        _col_iter = _cols.find(entry_iter->first);
        if(_col_iter == _cols.end()) {
            return -1;
        }

        //Check that the value being written is appropriate
        if(entry_iter->second.length() > PAGE_SIZE) {
            return -1;
        }

        entry_iter++;
    }

    //Write the values into the columns
    entry_iter = m->begin();
    while(entry_iter != m->end()) {
        _col_iter = _cols.find(entry_iter->first);
        _col_iter->second->write(row_key, entry_iter->second);

        //Add the row key to our ordered index
        _idx.insert(row_key);

        entry_iter++;
    }

    return 0;

}


/***
 *
 *  Removes an entry from a column family by row key.
 *
 ***/
int Column_Family::cf_delete(std::string key) {

    for(auto& kv : _cols) {
        kv.second->del(key);
    }

    return 0;

}
