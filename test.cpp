//CPP Packages
#include <iostream>
#include <string>

//User defined libraries
#include "inc/Column.h"

using namespace std;


int main() {

    Column col(0);
    col.read("foo");

    cout << "Hello" << std::endl;

    return 0;

}
