#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <string>

/*
 *  Bloom filter for checking membership in a set
 */
class BloomFilter {

private:
    int _size;
    int *_bf;

    int h0(std::string k);
    int h1(std::string k);
    int h2(std::string k);

public:
    bool check(std::string);
    void insert(std::string);

    BloomFilter(int);
    ~BloomFilter(void);

};

#endif /* BLOOMFILTER_H */
