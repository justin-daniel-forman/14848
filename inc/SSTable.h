#ifndef SSTABLE_H
#define SSTABLE_H


/*
 *  Representation of an SST to be kept on disk
 */
class SSTable {

private:
    //FIXME: Keep open file descriptor?
    char  _filename[100];
    SSIndex     *_index;

public:
    //Reads the associated value if key is mapped
    int read(std::string key, char *result);

    //Called on the newer one with a pointer to the older one
    bool merge_older_table(SSTable *oldtable);

    //write the data array to specified filename
    //Save index internally
    SSTable(const char *filename, SSIndex *index, const char *data);

    //delete _filename to free up disk space
    //Free the index here?
    //implies we are done with this table
    ~SSTable(void);

};

#endif /* SSTABLE_H */
