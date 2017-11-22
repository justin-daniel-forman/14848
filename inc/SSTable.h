#ifndef SSTABLE_H
#define SSTABLE_H


/*
 *  Representation of an SST to be kept on disk
 */
class SSTable {

private:
    std::string _name;
    std::string _filename;
    bool _valid; //Hacky error handling for now
    SSIndex *_index;
    int      _size;

public:
    //Reads the associated value if key is mapped
    std::string read(std::string key);

    //Called on the newer one with a pointer to the older one
    int merge_older_table(SSTable *oldtable);

    SSIndex *get_index(void);
    std::string get_filename(void);

    //write the data array to specified filename
    //Save index internally
    SSTable(std::string, SSIndex*, std::string);

    //delete _filename to free up disk space
    //Free the index here?
    //implies we are done with this table
    ~SSTable(void);

};

#endif /* SSTABLE_H */
