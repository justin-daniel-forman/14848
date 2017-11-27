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

    int num_fails = 0;

    Test_Column tc(0);
    num_fails += tc.insert_test(5000);
    num_fails += tc.mixed_test(10000, 10, 10); //This can fail as we increase dups and deletes

    std::set <std::string> column_names = {"c0", "c1", "c2", "c3", "c4"};
    Test_DB tdb(column_names);

    num_fails += tdb.single_insert_test(500);
    num_fails += tdb.single_insert_test(4000);
    num_fails += tdb.many_mixed_test(1000, 10, 10); //This fails as we increase # writes

    if(num_fails != 0) {
        std::cout << "FAILURE WITH SIGNATURE: " << num_fails << std::endl;
    } else {
        std::cout << "ALL TESTS PASS" << std::endl;
    }

    return 0;

}

/*****************************************************************************
 *                      TEST ANALYTIC FUNCTIONS                              *
 *****************************************************************************/

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

Test_DB::Test_DB(std::set <std::string> cnames) {
    column_names = cnames;
    cf_name = "0";
    initialize();
}

Test_DB::~Test_DB() {
    return;
}


void Test_DB::initialize(void) {

    cf_name = std::to_string(stoi(cf_name) + 1);

    //Create a new hash that we can use to compute aggregates accross our cols
    for (auto& cname_iter : column_names) {
        hashes[cname_iter] = 0;
    }

}

/***
 *
 * Simplest possible test to check whether or not we can actually read/write
 * a single value to our column family
 *
 ***/
int Test_DB::single_insert_test(int size) {

    //Startup
    initialize();
    db.new_column_family(cf_name, &column_names, 0);

    //Perform the test
    insert_random_entry(size);

    //Check results
    if(check_results() < 0) {
        std::cout << "DB SINGLE INSERTION TEST FAILED!" << std::endl;
        return -1;
    } else {
        std::cout << "DB SINGLE INSERTION TEST PASSED!" << std::endl;
        return 0;
    }

}

/***
 *
 * Checks that some sequence of interleaved inserts, updates, and deletes
 * performs correctly.
 *
 ***/
int Test_DB::many_mixed_test(int size, int ninserts, int ndeletes) {

    //Startup
    initialize();
    db.new_column_family(cf_name, &column_names, 0);

    //Perform the test
    int total = ninserts + ndeletes;
    for(int i = 0; i < total; i++) {

        int entropy = rand() % 10;

        if(entropy == 3 && (ndeletes > 0) && (i > 0)) {
            delete_random_entry();
            ndeletes--;

        } else if(entropy == 5 && (ninserts > 0) && (i > 0)) {
            insert_dup_entry(size);
            ninserts--;

        } else if(ninserts > 0) {
            insert_random_entry(size);
            ninserts--;

        } else if(ndeletes > 0) {
            delete_random_entry();
            ndeletes--;
        }

    }

    insert_random_entry(size);

    //Check results
    if(check_results() < 0) {
        std::cout << "DB MANY MIX TEST FAILED!" << std::endl;
        return -1;
    } else {
        std::cout << "DB MANY MIX TEST PASSED!" << std::endl;
        return 0;
    }

}


void Test_DB::insert_random_entry(int vsize) {

    //Create a set of random entries for each column
    char value [vsize];
    for (auto& col_iter : column_names) {
        generate_rand_string(value, vsize);
        string v(value);
        entry[col_iter] = v;
        hashes[col_iter] += string_to_int(v);
    }

    //Generate a random row key for this entry
    char rkey[100];
    generate_rand_string(rkey, 100);
    string rk(rkey);
    row_keys.push_back(rk);

    db.insert(cf_name, rk, &entry);

}

void Test_DB::insert_dup_entry(int vsize) {

    //Create a set of random entries for each column
    char value [vsize];
    for (auto& col_iter : column_names) {
        generate_rand_string(value, vsize);
        string v(value);
        entry[col_iter] = v;
        hashes[col_iter] += string_to_int(v);
    }

    //Recycle an old key at random
    int i;
    if(row_keys.size() > 0) {
        i = rand() % row_keys.size();

        //Read out the value and "unupdate" the hash values accordingly
        db.select(&sr, cf_name, row_keys[i], row_keys[i], &column_names);
        for(auto& table_iter : sr._table) {

            std::list<std::string> *val_list = table_iter.second;

            for(auto & col_iter : column_names) {
                hashes[col_iter] -= string_to_int(val_list->front());
                val_list->pop_front();
            }

        }


    //If there are no keys, just use a dummy one. This is very unlikely
    //and ultimately irrelevant
    } else {
        row_keys.push_back("foo");
    }


    db.insert(cf_name, row_keys[i], &entry);

}

void Test_DB::delete_random_entry(void) {

    //Recycle an old key at random
    int i = rand() % row_keys.size();
    unsigned int tries = 0;
    while(row_keys[i] == "") {
        if(tries == row_keys.size()) { //nothing left to delete
            return;
        }

        i = rand() % row_keys.size();
        tries++;
    }

    //Read out the value and "unupdate" the hash values accordingly
    db.select(&sr, cf_name, row_keys[i], row_keys[i], &column_names);
    for(auto& table_iter : sr._table) {

        std::list<std::string> *val_list = table_iter.second;

        for(auto & col_iter : column_names) {
            hashes[col_iter] -= string_to_int(val_list->front());
            val_list->pop_front();
        }

    }

    //Delete the entry out of the column family
    db.del(cf_name, row_keys[i]);
    row_keys[i] = "";

    return;

}

/***
 *
 *  Check that all hash values are consistent with our precalculated ones
 *
 ***/
int Test_DB::check_results(void) {

    for (auto& i : hashes) {
        if(i.second != stoi(db.aggregate(cf_name, i.first, "0", agg))) {
            return -1;
        }
    }

    return 0;

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


int Test_Column::insert_test(int num_inserts) {

    int ssize = 50;
    int msize = 100;

    char small_buf [ssize];
    char med_buf   [msize];

    string vstr;
    string kstr;

    //Write all values into test column
    for(int i = 0; i < num_inserts; i++) {

        //med value, small key
        generate_rand_string(small_buf, ssize);
        generate_rand_string(med_buf, msize);

        string tk(small_buf);
        string tv(med_buf);

        kstr = tk;
        vstr = tv;

        _col->write(kstr, vstr);
        _map[kstr] = vstr;

    }

    //Read all values from test column
    _iter = _map.begin();
    while(_iter != _map.end()) {
        vstr = _col->read(_iter->first);
        if(vstr != _iter->second) {
            cout << "COLUMN INSERT TEST FAIL!" << std::endl;
            cout << "Actual: " << _iter->second << std::endl;
            cout << "Observed: " << vstr << std::endl;
            return -1;
        }
        _iter++;

    }

    cout << "COLUMN INSERT TEST SUCCESS" << std::endl;
    return 0;

}


int Test_Column::mixed_test(int num_inserts, int num_deletes, int num_dups) {

    int ssize = 50;
    int msize = 100;

    char small_buf [ssize];
    char med_buf   [msize];

    string vstr;
    string kstr;

    //Write all values into test column
    for(int i = 0; i < num_inserts + num_deletes + num_dups; i++) {

        int entropy = rand() % 10;

        if(entropy == 3 && num_deletes > 0 && i > 0) { //delete an existing entry

            _col->del(_keys[i % _keys.size()]);
            _map.erase(_keys[i % _keys.size()]);

            num_deletes--;

        } else if(entropy == 5 && num_dups > 0 && i > 0) {
            //duplicate an existing entry with a new value

            generate_rand_string(med_buf, msize);
            string tv(med_buf);
            vstr = tv;

            _map[_keys[i % _keys.size()]] = vstr;
            _col->write(_keys[i % _keys.size()], vstr);

            num_dups--;

        } else if(num_inserts > 0) {
            //med value, small key
            generate_rand_string(small_buf, ssize);
            generate_rand_string(med_buf, msize);

            string tk(small_buf);
            string tv(med_buf);

            kstr = tk;
            vstr = tv;

            _col->write(kstr, vstr);
            _map[kstr] = vstr;
            _keys.push_back(kstr);

            num_inserts--;

        }

    }

    //Read all values from test column
    _iter = _map.begin();
    while(_iter != _map.end()) {
        vstr = _col->read(_iter->first);
        if(vstr != _iter->second) {
            cout << "COLUMN INSERT TEST FAIL!" << std::endl;
            cout << "Actual: " << _iter->second << std::endl;
            cout << "Observed: " << vstr << std::endl;
            return -1;
        }
        _iter++;

    }

    cout << "COLUMN INSERT TEST SUCCESS" << std::endl;
    return 0;

}

/*****************************************************************************
 *                       RANDOM HELPER FUNCTIONS                             *
 *****************************************************************************/

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


int string_to_int(string a) {

    int sum = 0;

    for(char& c : a) {
        sum +=  (int) c;

    }

    return sum;
}
