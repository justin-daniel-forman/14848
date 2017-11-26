//CPP Packages
#include <iostream>
#include <string>
#include <cstdlib>
#include <string.h>

//User defined libraries
#include "inc/column.h"
#include "inc/testcolumn.h"
#include "inc/columnfamily.h"
#include "inc/db.h"

using namespace std;


int main() {

//    cout << "Hello World!" << std::endl;
//
//    cout << std::endl;
//    Memtable mtable0("mtable0");
//    mtable0.write("1", "table0 1");
//    mtable0.write("2", "table0 3");
//    mtable0.write("3", "table0 3");
//    mtable0.write("4", "table0 4");
//    mtable0.write("5", "table0 5");
//    mtable0.write("6", "table0 6");
//    mtable0.write("7", "table0 7");
//    //cout << "Reading from mtable0: [1:" << mtable0.read("1") << "]\n";
//    //cout << "Reading from mtable0: [2:" << mtable0.read("2") << "]\n";
//    //cout << "Reading from mtable0: [4:" << mtable0.read("4") << "]\n";
//    SSTable sstable0("sstable0", mtable0.get_map());
//    //cout << "Reading from sstable0: [1:" << sstable0.read("1") << "]\n";
//    //cout << "Reading from sstable0: [2:" << sstable0.read("2") << "]\n";
//    //cout << "Reading from sstable0: [3:" << sstable0.read("3") << "]\n";
//    //cout << "Reading from sstable0: [7:" << sstable0.read("7") << "]\n";
//
//    cout << std::endl;
//    Memtable mtable1("mtable1");
//    mtable1.write("1", "table1 1");
//    mtable1.write("3", "table1 3");
//    mtable1.write("5", "table1 5");
//    mtable1.write("7", "table1 7");
//    mtable1.write("9", "table1 9");
//    mtable1.write("11", "table1 11");
//    mtable1.write("13", "table1 13");
//    SSTable sstable1("sstable1", mtable1.get_map());
//    //cout << "Reading from table1: [1:" << sstable1.read("1") << "]\n";
//    //cout << "Reading from table1: [3:" << sstable1.read("3") << "]\n";
//    //cout << "Reading from table1: [5:" << sstable1.read("5") << "]\n";
//
//
//    sstable1.merge_into_table(sstable0, sstable0.get_file_len());
//
//    //cout << "Reading from table0: [1:" << sstable0.read("1") << "]\n";
//    //cout << "Reading from table0: [2:" << sstable0.read("2") << "]\n";
//    //cout << "Reading from table0: [2:" << sstable0.read("2") << "]\n";
//
//    cout << "Goodbye!" << std::endl;

    //Test_Column tc(0);
    //tc.random_test(1, 0, 0);


    //Try out Column Family Implementation
    std::set<std::string> column_names = {"ca", "cb", "cc"};
    Column_Family cf(&column_names, 0);

    std::map<std::string, std::string> entry;
    entry["ca"] = "a";
    entry["cb"] = "b";
    entry["cc"] = "c";

    cf.cf_insert("a", &entry);
    cf.cf_insert("d", &entry);
    cf.cf_insert("e", &entry);

    std::set<std::string> cols = {"ca", "cb", "dd"};
    Search_Result sr;

//    cf.cf_select(&sr, "a", "z", &cols);
//    sr.print_result();
//
//    std::cout << "\n\n";
//    cf.cf_select(&sr, "e", "e", &cols);
//    sr.print_result();


    //DB implementation
    DB db;
    db.new_column_family("foobar", &column_names, 0);
    db.insert("foobar", "a", &entry);
    db.insert("foobar", "d", &entry);
    db.insert("foobar", "e", &entry);
    db.select(&sr, "foobar", "", "", &cols);
    sr.print_result();

    column_names = { "dc", "dd" };
    db.new_column_family("mark", &column_names, 0);
    entry.clear();
    entry["dc"] = "x";
    entry["dd"] = "y";
    db.insert("mark", "a", &entry);

    std::set<std::string> cf_names = {"mark"};
    db.select(&sr, "mark", "", "", &cols);
    sr.print_result();

    std::cout << "\n\n\n";
    db.join(&sr, &cf_names, "foobar");
    sr.print_result();

    db.del("foobar", "d");
    std::cout << "\n\n\n";
    db.join(&sr, &cf_names, "foobar");
    sr.print_result();

    return 0;

}

/*****************************************************************************
 *                             TEST_COLUMN_FAMILY                            *
 *****************************************************************************/

//int Test_Column_Family::random_test(int num_columns) {
//
//
//}


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
