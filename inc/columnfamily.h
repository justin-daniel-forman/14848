#ifndef COLUMN_FAMILY_H
#define COLUMN_FAMILY_H

#include <map>
#include <set>
#include <string>

#include "column.h"

/***
 *
 * Used to query information out of a column family
 *
 ***/
class Search_Result {

private:
    std::map <std::string, std::set<std::string>> _table;

public:
    void add_row(std::set <std::string>*);
    void print_result(void);

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

    int cf_select(Search_Result *r, std::string min, std::string max);
    int cf_insert(std::string row_key, std::map<std::string, std::string> *m);
    int cf_delete(std::string key);

};

#endif /* COLUMN_FAMILY_H */
