//CPP Packages
#include <iostream>
#include <string>

//User defined libraries
#include "inc/column.h"

using namespace std;


int main() {

    cout << "Hello World!" << std::endl;

    cout << std::endl;
    Memtable mtable0("mtable0");
    mtable0.write("1", "table0 1");
    mtable0.write("2", "table0 3");
    mtable0.write("3", "table0 3");
    mtable0.write("4", "table0 4");
    mtable0.write("5", "table0 5");
    mtable0.write("6", "table0 6");
    mtable0.write("7", "table0 7");
    //cout << "Reading from mtable0: [1:" << mtable0.read("1") << "]\n";
    //cout << "Reading from mtable0: [2:" << mtable0.read("2") << "]\n";
    //cout << "Reading from mtable0: [4:" << mtable0.read("4") << "]\n";
    SSTable sstable0("sstable0", mtable0.get_map());
    //cout << "Reading from sstable0: [1:" << sstable0.read("1") << "]\n";
    //cout << "Reading from sstable0: [2:" << sstable0.read("2") << "]\n";
    //cout << "Reading from sstable0: [3:" << sstable0.read("3") << "]\n";
    //cout << "Reading from sstable0: [7:" << sstable0.read("7") << "]\n";

    cout << std::endl;
    Memtable mtable1("mtable1");
    mtable1.write("1", "table1 1");
    mtable1.write("3", "table1 3");
    mtable1.write("5", "table1 5");
    mtable1.write("7", "table1 7");
    mtable1.write("9", "table1 9");
    mtable1.write("11", "table1 11");
    mtable1.write("13", "table1 13");
    SSTable sstable1("sstable1", mtable1.get_map());
    //cout << "Reading from table1: [1:" << sstable1.read("1") << "]\n";
    //cout << "Reading from table1: [3:" << sstable1.read("3") << "]\n";
    //cout << "Reading from table1: [5:" << sstable1.read("5") << "]\n";


    sstable1.merge_into_table(sstable0, sstable0.get_file_len());

    //cout << "Reading from table0: [1:" << sstable0.read("1") << "]\n";
    //cout << "Reading from table0: [2:" << sstable0.read("2") << "]\n";
    //cout << "Reading from table0: [2:" << sstable0.read("2") << "]\n";

    cout << "Goodbye!" << std::endl;

    return 0;

}
