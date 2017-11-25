//CPP Packages
#include <iostream>
#include <string>
#include <cstdlib>
#include <string.h>

//User defined libraries
#include "inc/column.h"
#include "inc/testcolumn.h"
#include "inc/columnfamily.h"

using namespace std;


int main() {

    cout << "Hello World!" << std::endl;

    Column col0("col0", 0);
    col0.read("foo");

    Memtable mtable0("mtable0");
    mtable0.write("1", "line1\t");
    mtable0.write("2", "line2\t");
    mtable0.write("3", "line3\t");
    mtable0.write("4", "line4\t");
    mtable0.write("5", "line5\t");
    mtable0.write("3", "stew3\t");
    mtable0.write("2", "stew2\t");

    //char result[4096];
    SSTable sstable0("sstable0", mtable0.get_map());

    std::string ret;
    ret = sstable0.read("1");
    cout << "We got: [" << ret << "]" << std::endl;
    ret = sstable0.read("5");
    cout << "We got: [" << ret << "]" << std::endl;

    //sstable1.merge_older_table(&sstable0);


    //if(sstable1.read("mark", result) >= 0) {
    //}

    //if(sstable1.read("new_only", result) >= 0) {
    //    cout << "We got: " << result << std::endl;
    //}

    //if(sstable1.read("old_only", result) >= 0) {
    //    cout << "We got: " << result << std::endl;
    //}

    cout << "Goodbye!" << std::endl;

    Test_Column tc(0);
    tc.random_test(1, 0, 0);

    std::set<std::string> column_names = {"ca", "cb", "cc"};
    Column_Family cf(&column_names, 0);

    std::map<std::string, std::string> entry;
    entry["ca"] = "a";
    entry["cb"] = "b";
    entry["cc"] = "c";

    cf.cf_insert("d", &entry);


    Search_Result sr;
    cf.cf_select(&sr, "a", "z");

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


int Test_Column::random_test(int num_small, int num_med, int num_large) {

    int ssize = 100;
    int msize = 1000;
    int lsize = 10000;

    char small_buf [ssize];
    char med_buf   [msize];
    char large_buf [lsize]; //larger than page size

    int remaining_small = num_small;
    int remaining_med   = num_med;
    int remaining_large = num_large;

    string vstr;
    string kstr;

    //Write all values into test column
    while((remaining_small + remaining_med + remaining_large) > 0) {

        switch(rand() % 3) {
            case 0: //small value, med key
                generate_rand_string(small_buf, ssize);
                generate_rand_string(med_buf, ssize);
                vstr = small_buf;
                kstr = med_buf;
                remaining_small--;
                break;

            case 1: //med value, small key
                generate_rand_string(small_buf, ssize);
                generate_rand_string(med_buf, ssize);
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

    }

    cout << "Test success" << std::endl;
    return 0;

}

void Test_Column::generate_rand_string(char *buf, int size) {

    int  i;
    char r;

    memset(buf, '\0', size);

    for(i = 0; i < (size-1); i++) {
        r = (char) (rand() % 126 + 33); //Useful ascii chars are 33-126
        memset((buf + i), r, 1);
    }

    return;

}
