#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <string>
#include <map>

#define PAGE_SIZE 4096

/*
 *  Memtable implementation
 */
class Memtable {

private:
    std::string _name;
    std::map<std::string, std::string> _map;
    std::map<std::string, std::string>::iterator _iter;
    int _size;

public:

    Memtable(std::string);
    ~Memtable(void);

    std::string read(std::string);
    int write(std::string, std::string);
    void del(std::string);

    std::map<std::string, std::string> get_map();
};

#endif /* MEMTABLE_H */
