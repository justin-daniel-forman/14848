#include <iostream>
#include <string>
#include <map>
#include <set>
#include <list>

#include "inc/columnfamily.h"
#include "inc/db.h"

/*****************************************************************************
 *                              DB                                           *
 *****************************************************************************/

/***
 *
 *  Create a new column family for the database given that the parameters
 *  are valid. A column family is representative of a traditional table.
 *
 ***/
int DB::new_column_family(std::string name, std::set<std::string> *schema, int copt) {

    std::map<std::string, Column_Family*>::iterator cf_iter;

    //Check that a column family of this name does not already exist
    cf_iter = _cf_map.find(name);
    if(cf_iter != _cf_map.end()) {
        return -1;
    }

    //Invalid column family name
    if(name.length() == 0 ) {
        //FIXME: Probably want some more restrictions on what you can call
        //the column family
        return -1;
    }

    //Column names are not specified properly
    if(schema == NULL || schema->size() < 1) {
        return -1;
    }

    _cf_map[name] = new Column_Family(schema, copt);

    return 0;
}


/***
 *
 *  Insert a row into a column family by specifying the column->value mappings
 *  for the row_key indicated.
 *
 ***/
int DB::insert(std::string cf_name, std::string row_key,
    std::map<std::string, std::string> *m) {

    std::map<std::string, Column_Family*>::iterator cf_iter;

    //Check that a column family of this name does not already exist
    cf_iter = _cf_map.find(cf_name);
    if(cf_iter == _cf_map.end()) {
        return -1;
    }

    return (cf_iter->second)->cf_insert(row_key, m);

}

/***
 *
 * Delete a row out of a column family
 *
 ***/
int DB::del(std::string cf_name, std::string row_key) {

    std::map<std::string, Column_Family*>::iterator cf_iter;

    //Check that a column family of this name does not already exist
    cf_iter = _cf_map.find(cf_name);
    if(cf_iter == _cf_map.end()) {
        return -1;
    }

    return (cf_iter->second)->cf_delete(row_key);

}


/***
 *
 *  Return a temporary view of two or more tables coalesced by primary key
 *  specified. If an entry does not exist in one of the column families,
 *  it is represented by the empty string.
 *
 ***/
int DB::join(Search_Result *sr, std::set<std::string> *tables, std::string pcol) {

    std::set<std::string>::iterator table_iter;
    std::string curr_cf_name;
    Search_Result temp_result;

    //Check that user has allocated space for result
    if(sr == NULL) {
        return -1;
    }

    //Use the keys from the primary column to pull out rows from all of the;
    if(this->select(sr, pcol, "", "") < 0) {
        return -1;
    }

    //Select the data from each column family, and merge in the results
    //to the existing results
    table_iter = tables->begin();
    while(table_iter != tables->end()) {

        //Error out if there was a problem getting the data
        if(this->select(&temp_result, *table_iter, "", "") < 0) {
            return -1;
        }

        //Merge the results together
        sr->merge(&temp_result);

        table_iter++;
    }

    return 0;
}


/***
 *
 *  Perform an SQL like SELECT on a specific column family. We can only
 *  specify a range of keys to perform the select on. There is a sorted
 *  index within the Column_Family that should make that a relatively quick
 *  operation.
 *
 ***/
int DB::select(Search_Result *sr, std::string cf, std::string min, std::string max) {

    std::map<std::string, Column_Family*>::iterator cf_iter;

    //Check that user has allocated space for result
    if(sr == NULL) {
        return -1;
    }

    //Check that column family specified is a valid cf
    cf_iter = _cf_map.find(cf);
    if(cf_iter == _cf_map.end()) {
        return -1;
    }

    //Perform the select from the indicated column family
    return (cf_iter->second)->cf_select(sr, min, max);

}

/*****************************************************************************
 *                             SEARCH RESULT                                 *
 *****************************************************************************/

/***
 *
 *  Destructor makes sure to delete all of the lists created to hold
 *  data.
 *
 ***/
Search_Result::~Search_Result(void) {
    std::map <std::string, std::list<std::string>*>::iterator kv;

    kv = _table.begin();
    while(kv != _table.end()) {
        delete kv->second;
        kv++;
    }

}

/***
 *
 *  Ensures that a search result object is cleared out between uses
 *
 ***/
void Search_Result::reset(void) {
    _table.clear();
    _col_names.clear();
    return;
}

/***
 *
 *  Merge together two search results to represent a join operation.
 *  This is probably a little bit inefficient to construct the second
 *  search result, and then merge into the first one instead of adding
 *  directly to the first one.
 *
 ***/
void Search_Result::merge(Search_Result *new_result) {

    int ncols;
    std::map <std::string, std::list<std::string>*>::iterator oldi;
    std::map <std::string, std::list<std::string>*>::iterator newi;

    //add in all of the column names
    ncols = new_result->_col_names.size();
    _col_names.splice(_col_names.end(), new_result->_col_names);

    //Coalesce the rows together
    oldi = _table.begin();
    while(oldi != _table.end()) {

        //Locate the row in the new table by the key in the old table
        newi = new_result->_table.find(oldi->first);

        //The key does not exist in the new table, so insert 0ed out vals
        if(newi == new_result->_table.end()) {
            for(int i = 0; i < ncols; i++) {
                oldi->second->push_back("N/A");
            }

        //The key exists in the new table, so merge the row
        } else {
            oldi->second->splice(oldi->second->end(), *(newi->second));
        }

        oldi++;
    }

}

void Search_Result::add_row(std::string key, std::list <std::string> *row) {

    _table[key] = row;

    return;

}

void Search_Result::add_col(std::string col) {
    _col_names.push_back(col);

}

/***
 *
 *  Simple visualization of a search result in row format
 *
 ***/
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

    //Make sure that the result is zeroed out
    r->reset();

    lb = (min.length() == 0) ? _idx.begin() : _idx.lower_bound(min);
    ub = (max.length() == 0) ? _idx.end()   : _idx.upper_bound(max);

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

    if(_idx.erase(key) <= 0) {
        return -1;
    }

    return 0;

}
