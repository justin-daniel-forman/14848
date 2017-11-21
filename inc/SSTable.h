#ifndef SSTABLE_H
#define SSTABLE_H


/*
 *  Representation of an SST to be kept on disk
 */
class SSTable {

private:
    std::string _name;
    char     _filename[100];
    SSIndex *_index;
    int      _size;

public:
    //Reads the associated value if key is mapped
    int read(std::string key, char *result);

    //Called on the newer one with a pointer to the older one
    int merge_older_table(SSTable *oldtable);

    SSIndex *get_index(void);
    char *get_filename(void);

    //write the data array to specified filename
    //Save index internally
    SSTable(std::string, const char *filename, SSIndex *index, const char *data);

    //delete _filename to free up disk space
    //Free the index here?
    //implies we are done with this table
    ~SSTable(void);

};

#endif /* SSTABLE_H */
