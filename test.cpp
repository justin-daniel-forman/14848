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

    Memtable table;
    table.write("key", ":LKIJ:LK;kllellldllakdkfja;lkelkj;");
    table.write("foo", "bar");
    table.write("forman", "forman");
    table.write("mark", "wubbs");
    table.read("key");
    table.read("foo");
    table.del("foo");
    table.read("foo");


    cout << "Goodbye!" << std::endl;
    return 0;

}
