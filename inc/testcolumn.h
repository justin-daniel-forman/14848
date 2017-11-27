#ifndef TCOLUMN_H
#define TCOLUMN_H

#include <string>
#include <map>
#include <set>
#include <vector>

#include "column.h"
#include "db.h"

class Test_DB {

private:
    std::set <std::string> column_names;
    std::map <std::string, int> hashes;
    std::map <std::string, std::string> entry;
    std::vector <std::string> row_keys;
    std::string key;
    std::string cf_name;

    Search_Result sr;
    DB db;

public:

    Test_DB(std::set <std::string>);
    ~Test_DB();

    //Tests
    int single_insert_test(int);
    int many_mixed_test(int, int, int);

    //Helpers
    void insert_random_entry(int);
    void insert_dup_entry(int);
    void delete_random_entry(void);

    void generate_random_key(int);
    void initialize(void);
    int  check_results(void);

};


/*
 *
 * Class used to black-box test column implementation
 *
 */
class Test_Column {

private:
    std::map <std::string, std::string> _map;
    std::map <std::string, std::string>::iterator _iter;
    std::vector <std::string> _keys;
    Column *_col;

public:
    int insert_test(int);
    int mixed_test(int, int, int);
    Test_Column(int = 0);
    ~Test_Column();

};


#endif /* TCOLUMN_H */
