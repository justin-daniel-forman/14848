#ifndef SSINDEX_H
#define SSINDEX_H




/*
 *  Representation of the index for an SSTable to be kept in memory
 */
class SSIndex {

private:

    std::string _name;
    std::map<std::string, index_entry_t*> _index;
    std::map<std::string, index_entry_t*>::iterator _iter;

public:

    SSIndex(std::string);
    ~SSIndex(void);

    index_entry_t *lookup(std::string);
    void map(std::string, int, int);
    void erase(std::string); //Actually remove the key from this map
    int consume_index(SSIndex*);

    std::map<std::string, index_entry_t*>::iterator gen_iter();

};

#endif /* SSINDEX_H */
