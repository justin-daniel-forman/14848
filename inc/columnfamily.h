#ifndef COLUMN_FAMILY_H
#define COLUMN_FAMILY_H

#include <map>
#include <set>
#include <string>
#include <list>

#include "column.h"

/***
 *
 * Used to query information out of a column family
 *
 ***/
class Search_Result {

public:
    std::map <std::string, std::list<std::string>*> _table;
    std::list <std::string> _col_names;

    std::map <std::string, std::list<std::string>*>::iterator _table_iter;
    std::list <std::string>::iterator _col_names_iter;

    void add_row(std::string, std::list<std::string>*);
    void add_col(std::string);
    void reset(void);
    void print_result(void);
    void merge(Search_Result*);

    ~Search_Result();

};


/***
 *
 * Represents a series of columns linked by a row key.
 *
 ***/
class Column_Family {

private:

    std::set <std::string> _idx;
    std::set <std::string>::iterator  _idx_iter;

    std::map <std::string, Column*> _cols;
    std::map <std::string, Column*>::iterator _col_iter;

public:

    Column_Family(std::set<std::string>*, int);
    ~Column_Family(void);

    int cf_select(Search_Result *r, std::string min, std::string max,
        std::set<std::string>*);
    int cf_insert(std::string row_key, std::map<std::string, std::string> *m);
    int cf_delete(std::string key);

};

#endif /* COLUMN_FAMILY_H */
