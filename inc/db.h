#ifndef DB_H
#define DB_H

#include <list>

#include "columnfamily.h"


class DB {

private:
    std::map <std::string, Column_Family*> _cf_map;
    std::map <std::string, Column_Family*>::iterator _cf_map_iter;


public:

    //DB Management
    int new_column_family(std::string, std::set<std::string>*, int);
    int delete_column_family(std::string);

    int join(Search_Result*, std::set<std::string>*, std::string);
    int select(Search_Result*, std::string, std::string, std::string,
        std::set<std::string>*);

    int insert(std::string, std::string, std::map<std::string, std::string>*);
    int del(std::string, std::string);

    //Analytics functions
    std::string compare(std::string, std::string, std::string,
        int (*cmp)(std::string, std::string));

    std::string aggregate(std::string, std::string, std::string,
        std::string (*agg)(std::string, std::string));

    int cross(Search_Result*, std::string, std::string, std::string,
        std::string (*cross)(std::string, std::string));

};

#endif
