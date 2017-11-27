//CPP Packages
#include <iostream>
#include <string>
#include <cstdlib>
#include <string.h>
#include <vector>

//User defined libraries
#include "inc/column.h"
#include "inc/testcolumn.h"
#include "inc/columnfamily.h"
#include "inc/db.h"

using namespace std;

int cmp (std::string, std::string);
std::string agg(std::string, std::string);
std::string cross(std::string, std::string);

void generate_rand_string(char*, int);
int  string_to_int(string);

int main() {

    //FIXME: This segfaults
    //Test_Column tc(0);
    //tc.random_test(0, 100, 0);

// BEGIN DB INSERTION TEST 0
    DB db;
    int successes = 0;
    std::set <std::string> column_names = {"c0", "c1", "c2", "c3", "c4"};

    //Initialize hash values to 0
    std::map <std::string, int> hashes;
    hashes["c0"] = 0;
    hashes["c1"] = 0;
    hashes["c2"] = 0;
    hashes["c3"] = 0;
    hashes["c4"] = 0;

    db.new_column_family("cf0", &column_names, 0);

    for(int i = 0; i < 1000; i++) {

        //Create a set of random entries for each column
        std::map<std::string, std::string> entry;
        char value [400];
        for (auto& col_iter : column_names) {
            generate_rand_string(value, 400);
            string v(value);
            entry[col_iter] = v;
            hashes[col_iter] += string_to_int(v);
        }

        //Generate a random row key for this entry
        char rkey[100];
        generate_rand_string(rkey, 100);
        string rk(rkey);

        db.insert("cf0", rk, &entry);

    }

    for (auto& i : hashes) {
        if(i.second != stoi(db.aggregate("cf0", i.first, "0", agg))) {
            std::cout << "DB INSERTION TEST FAILED!" << std::endl;
        } else {
            successes++;
        }
    }

    if(successes == 5) {
        std::cout << "DB INSERTION TEST PASSED!" << std::endl;
    }
//END DB INSERTION TEST 0

    return 0;

}

int cmp(std::string a, std::string b) {
    if(a < b) {
        return -1;
    } else if(a == b) {
        return 0;
    } else {
        return 1;
    }
}

std::string agg(std::string a, std::string b) {

    int ia = std::stoi(a);

    ia += string_to_int(b);

    return std::to_string(ia);

}

std::string cross(std::string a, std::string b) {
    return a.append(b);
}


/*****************************************************************************
 *                             TEST_DB                                       *
 *****************************************************************************/

int string_to_int(string a) {

    int sum = 0;

    for(char& c : a) {
        sum +=  (int) c;

    }

    return sum;
}


/*****************************************************************************
 *                             TEST_COLUMN                                   *
 *****************************************************************************/

Test_Column::Test_Column(int comp_opt) {
    _col = new Column("dut", comp_opt);
}

Test_Column::~Test_Column() {
    delete _col;
}


int Test_Column::random_test(int num_small, int num_med, int num_large) {

    int ssize = 100;
    int msize = 1000;
    int lsize = 10000;

    char small_buf [ssize];
    char med_buf   [msize];
    char large_buf [lsize]; //larger than page size

    int remaining_small = 0;//num_small;
    int remaining_med   = num_med;
    int remaining_large = 0;//num_large;

    string vstr;
    string kstr;

    cout << "WRITING IN TEST_COLUMN" << std::endl;

    //Write all values into test column
    while((remaining_small + remaining_med + remaining_large) > 0) {

        //switch(rand() % 3) {
        switch(1) {
            case 0: //small value, med key
                generate_rand_string(small_buf, ssize);
                generate_rand_string(med_buf, msize);
                vstr = small_buf;
                kstr = med_buf;
                remaining_small--;
                break;

            case 1: //med value, small key
                generate_rand_string(small_buf, ssize);
                generate_rand_string(med_buf, msize);
                kstr = small_buf;
                vstr = med_buf;
                remaining_med--;
                break;

            default: //large value, med key
                generate_rand_string(large_buf, lsize);
                generate_rand_string(med_buf, msize);
                kstr = med_buf;
                vstr = large_buf;
                remaining_large--;
                break;
        }

        _col->write(kstr, vstr);
        _map[kstr] = vstr;

    }

    cout << "READING IN TEST_COLUMN" << std::endl;
    //Read all values from test column
    _iter = _map.begin();
    while(_iter != _map.end()) {
        vstr = _col->read(_iter->first);
        if(vstr != _iter->second) {
            cout << "Test Failed!" << std::endl;
            cout << "Actual: " << _iter->second << std::endl;
            cout << "Observed: " << vstr << std::endl;
            return -1;
        }
        _iter++;

    }

    cout << "Test success" << std::endl;
    return 0;

}

void Test_Column::generate_rand_string(char *buf, int size) {
    generate_rand_string(buf, size);
}

void generate_rand_string(char *buf, int size) {

    int  i;
    char r;

    memset(buf, '\0', size);

    for(i = 0; i < (size-1); i++) {
        r = (char) (rand() % 126 + 33); //Useful ascii chars are 33-126
        memset((buf + i), r, 1);
    }

    return;

}
