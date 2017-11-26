#include <iostream>
#include <string>
#include <map>
#include <set>

#include "inc/columnfamily.h"

/*****************************************************************************
 *                             SEARCH RESULT                                 *
 *****************************************************************************/

void Search_Result::add_row(std::set <std::string> *row) {
    //TODO
    return;
}

void Search_Result::print_result(void) {
    //TODO
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
    std::set <std::string> results;

    //Check that user has allocated space for the result
    if(r == NULL) {
        return -1;
    }

    lb = _idx.lower_bound(min);
    ub = _idx.upper_bound(max);

    _idx_iter = lb;
    while(_idx_iter != ub) {

        //Retrieve the value from each of the columns
        results.clear();
        _col_iter = _cols.begin();

        //FIXME: Actually add to the search result
        while(_col_iter != _cols.end()) {
            std::cout << _col_iter->second->read(*_idx_iter) << std::endl;
            _col_iter++;
        }

        //Add the results to the search result
        r->add_row(&results);

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
