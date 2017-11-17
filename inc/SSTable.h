#ifndef SSTABLE_H
#define SSTABLE_H


/*
 *  Representation of an SST to be kept on disk
 */
class SSTable {

private:
    //FIXME: Keep open file descriptor?
    std::string _filename;
    SSIndex *index;

public:
    //Reads the associated value if key is mapped
    int read(std::string key, std::string *result);

    //Called on the newer one with a pointer to the older one
    bool merge_older_table(SSTable *oldtable);

    //write the data array to specified filename
    //Save index internally
    SSTable(std::string filename, SSIndex *index, char *data_array);

    //delete _filename to free up disk space
    //Free the index here?
    //implies we are done with this table
    ~SSTable(void);

};

#endif /* SSTABLE_H */
