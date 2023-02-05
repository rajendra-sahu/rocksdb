// Copyright (c) Rajendra Sahu, Columnar Storage 
// bucket_example.h
#ifndef BUCKET_HEADER_H
#define BUCKET_HEADER_H


#include <cstdio>
#include <string>
#include <iostream>

#include "sys/time.h"
#include <dirent.h>
#include <sys/types.h>

#include <fstream>
#include <sstream>
#include <queue>
#include <thread>
#include <future>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include "rocksdb/slice_transform.h"


//#include <readerwriterqueue/readerwriterqueue.h>
//#include <readerwriterqueue/readerwritercircularbuffer.h>
#include <MPMCQueue.h>

//#define NO_BACKGROUND_GC_
#define GC_QUEUE_SIZE 15728700


using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::BlockBasedTableOptions;
using ROCKSDB_NAMESPACE::NewBloomFilterPolicy;
using ROCKSDB_NAMESPACE::NewCappedPrefixTransform;
using ROCKSDB_NAMESPACE::NewBlockBasedTableFactory;
using namespace std;
//using namespace moodycamel;
using namespace rigtorp;

struct particle_value_schema
{
    uint64_t padding;
    float    x;              //location of particle in X direction
    float    y;              //location of particle in Y direction
    float    z;              //location of particle in Z direction
    float    i;              //index of the cell that had the particle
    float    ux;             //momentum of particle in X direction
    float    uy;             //momentum of particle in Y direction
    float    uz;             //momentum of particle in Z direction
    float    ke;             //kinetic energy of particle
};

struct particle_schema
{
    uint64_t ID;             //unique ID of a particle
    particle_value_schema  value;
};


struct gc_request
{
    uint64_t key;                 //unique ID of a particle
    uint16_t new_bucket_index;    //key exisitng in this bucket
};

class BucketedDB
{
    private:
    uint16_t instance_count;

    //vector<DB*> bucketedDB;
    DB* bucketedDB[30];   //try to parameterize this size
    int put_record_count[30];

    uint8_t pivot_offset;
    uint8_t pivot_size;

    //ReaderWriterQueue<uint64_t> gc_queue;   //create concurrent_queue
    MPMCQueue<gc_request> *gc_queue;   

    public:
    Options options;
    BlockBasedTableOptions table_options;
    bool update_mode;

    BucketedDB();
    BucketedDB(uint16_t count = 1, uint8_t offset = 0, uint8_t size = 4);
    ~BucketedDB();
    uint16_t get_index(float value);
    rocksdb::Status put(const rocksdb::Slice &key, std::string value);
    rocksdb::Status get(const rocksdb::Slice &key, std::string* value);
    rocksdb::Status put(const rocksdb::Slice &key, const rocksdb::Slice &value, float pivot);
    //rocksdb::Status get(const rocksdb::Slice &key, std::string* value);
    uint32_t value_range_query(float pv1, float pv2);
    void value_point_query(float pv);
    void print_db_stat();
    bool gc_function();
    
};


#endif


