//CPP Packages
#include <iostream>
#include <string>

//User defined libraries
#include "inc/Column.h"

using namespace std;


int main() {

    cout << "Hello World!" << std::endl;

    Column col(0);
    col.read("foo");

    Memtable table0;
    table0.write("key", ":LKIJ:LK;kllellldllakdkfja;lkelkj;");
    table0.write("foo", "bar");
    table0.write("forman", "forman");
    table0.write("mark", "wubbs");
    table0.write("old_only", "unique_old");
    table0.read("key");
    table0.read("foo");
    table0.del("foo");
    table0.read("foo");

    Memtable table1;
    table1.write("key", "new0");
    table1.write("foo", "new1");
    table1.write("forman", "new2");
    table1.write("mark", "new3");
    table1.write("new_only", "unique_new");

    char result[4096];
    SSTable sstable0("table0", table0.get_index(), table0.get_data());
    SSTable sstable1("table1", table1.get_index(), table1.get_data());

    sstable1.merge_older_table(&sstable0);


    if(sstable1.read("mark", result) >= 0) {
        cout << "We got: " << result << std::endl;
    }

    if(sstable1.read("new_only", result) >= 0) {
        cout << "We got: " << result << std::endl;
    }

    if(sstable1.read("old_only", result) >= 0) {
        cout << "We got: " << result << std::endl;
    }

    cout << "Goodbye!" << std::endl;

    return 0;

}
