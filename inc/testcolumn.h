#ifndef TCOLUMN_H
#define TCOLUMN_H

#include <string>
#include <map>

#include "column.h"

/*
 *
 * Class used to black-box test column implementation
 *
 */
class Test_Column {

private:
    std::map <std::string, std::string> _map;
    std::map <std::string, std::string>::iterator _iter;
    Column *_col;

public:
    int random_test(int, int, int);
    void generate_rand_string(char*, int);
    Test_Column(int);
    ~Test_Column();

};


#endif /* TCOLUMN_H */
