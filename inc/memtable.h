#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <string>
#include <map>

#define PAGE_SIZE 4096 * 8

/*
 *  Memtable implementation
 */
class Memtable {

private:
    std::string _name;
    std::map<std::string, std::string> _map;
    bool _taking_dump; //Indicates a thread has started to dump on this table
    long _size, _uid;

public:

    Memtable(std::string, long);
    ~Memtable(void);

    int read(std::string, std::string*);
    int write(std::string, std::string);
    int del(std::string);

    bool is_taken();
    long get_uid();
    std::map<std::string, std::string>* take_map();
};

#endif /* MEMTABLE_H */
