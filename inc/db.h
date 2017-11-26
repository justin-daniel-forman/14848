#ifndef DB_H
#define DB_H

#include <list>

#include "columnfamily.h"


class DB {

private:
    std::map <std::string, Column_Family*> _cf_map;
    std::map <std::string, Column_Family*>::iterator _cf_map_iter;


public:

    int new_column_family(std::string, std::set<std::string>*, int);
    int join(Search_Result*, std::set<std::string>*, std::string);
    int select(Search_Result*, std::string, std::string, std::string);
    int insert(std::string, std::string, std::map<std::string, std::string>*);

};




#endif
