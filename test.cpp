//CPP Packages
#include <iostream>
#include <string>

//User defined libraries
#include "inc/column.h"

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

    return 0;

}
