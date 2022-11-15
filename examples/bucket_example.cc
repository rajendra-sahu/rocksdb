// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include "bucket_example.h"
#include <fstream>
#include <sstream>
#define INSTANCE_COUNT 5 

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_bucket_example/db";
#endif

BucketedDB::BucketedDB()
{
  instance_count = 1;
  pivot_offset = 0; //starting of the value
  pivot_size = 4;   //default 4 bytes
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  for(uint16_t i =0; i < instance_count; i++)
  {
    Status s = DB::Open(options, kDBPath + char(32+ i), &bucketedDB[i]);
    assert(s.ok());

  }
}

BucketedDB::BucketedDB(uint16_t count, uint8_t offset, uint8_t size)
{
  instance_count = count;
  pivot_offset = offset;
  pivot_size = size;  

  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;
  for(uint16_t i =0; i < instance_count; i++)
  {
    Status s = DB::Open(options, kDBPath + char(48+ i), &bucketedDB[i]); 
    assert(s.ok());
  }
}

BucketedDB::~BucketedDB()
{
  for(uint16_t i =0; i < instance_count; i++)
  {
    delete bucketedDB[i];
  }
}

uint16_t BucketedDB::get_index(float value) //Implement the hashing fuction
{
  //return (uint16_t)(value*100) % instance_count;
  uint16_t normalized_value = (uint16_t)(value*1000);
  uint16_t normalized_pivot_range_interval = 200/instance_count;   //hardcoded
  if(normalized_value >= 200)
  {
    return instance_count - 1;
  }
  else
  {
    return  normalized_value/normalized_pivot_range_interval;
  }

}

rocksdb::Status BucketedDB::put( const rocksdb::Slice &key, string value)   //deprecated, shouldn't be used
{
  return bucketedDB[get_index(stof(value.substr(pivot_offset, pivot_size)))]->Put(WriteOptions(), key, value);
}

rocksdb::Status BucketedDB::put( const rocksdb::Slice &key, const rocksdb::Slice &value, float pivot)
{
  uint16_t index = get_index(pivot);
  //cout << "Putting in bucket = " <<index<<endl;
  return bucketedDB[index]->Put(WriteOptions(), key, value);
}

rocksdb::Status BucketedDB::get( const rocksdb::Slice &key, string* value)
{
  for(uint16_t i =0; i < instance_count; i++)
  {
    Status s = bucketedDB[i]->Get(ReadOptions(), key, value);
    if(s.IsNotFound())
    {
      continue;
    }
    if(s.ok())
    {
      //cout<< "Getting from bucket ="<<i<<endl;
      return s;
    }

  }
  return Status::NotFound();
}

void BucketedDB::value_point_query(float pv)
{
  rocksdb::Iterator* iter = bucketedDB[get_index(pv)]->NewIterator(ReadOptions());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next())
  {
    particle_value_schema* particle_read_value = (particle_value_schema*)(iter->value().data());
    if(particle_read_value->ke == pv)
    {
      cout << "Record -" <<iter->key().data() <<":";
      cout << particle_read_value->x<<particle_read_value->y<<particle_read_value->z<<particle_read_value->i<<particle_read_value->ux<<particle_read_value->uy<<particle_read_value->uz<<particle_read_value->ke<<endl;
    }
  }
}

uint32_t BucketedDB::value_range_query(float pv1, float pv2)
{
  /*for(float i = pv1; i<= pv2; i++)    //wrong logic;
  {
    value_point_query(i);
  }*/

  uint32_t count = 0;
  rocksdb::Iterator* iter;
  particle_value_schema* particle_read_value;
  /*if(get_index(pv1) == get_index(pv2))
  {
    iter = bucketedDB[get_index(pv1)]->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
      particle_read_value = (particle_value_schema*)(iter->value().data());
      if( pv1 <= particle_read_value->ke && particle_read_value->ke <= pv2)
      {
        cout << "Record -" <<iter->key().data() <<":";
        cout << particle_read_value->x<<particle_read_value->y<<particle_read_value->z<<particle_read_value->i<<particle_read_value->ux<<particle_read_value->uy<<particle_read_value->uz<<particle_read_value->ke<<endl;
      }
    }
  }
  else
  {

    iter = bucketedDB[get_index(pv1)]->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
      particle_read_value = (particle_value_schema*)(iter->value().data());
      if( pv1 <= particle_read_value->ke && particle_read_value->ke <= pv2)
      {
        cout << "Record -" <<iter->key().data() <<":";
        cout << particle_read_value->x<<particle_read_value->y<<particle_read_value->z<<particle_read_value->i<<particle_read_value->ux<<particle_read_value->uy<<particle_read_value->uz<<particle_read_value->ke<<endl;
      }
    }

  }*/

  
  for(uint16_t i = get_index(pv1); i<= get_index(pv2); i++)   
  {
    iter = bucketedDB[i]->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
      particle_read_value = (particle_value_schema*)(iter->value().data());
      if(i!= get_index(pv1) && i!=get_index(pv2))
      {
        //cout << "Record -" <<iter->key().data() <<":";
        //cout << particle_read_value->x<<particle_read_value->y<<particle_read_value->z<<particle_read_value->i<<particle_read_value->ux<<particle_read_value->uy<<particle_read_value->uz<<particle_read_value->ke<<endl;
        //cout << "Spanning multiple buckets"<<endl;
        count++;
      }
      else
      {
        if( pv1 <= particle_read_value->ke && particle_read_value->ke <= pv2)
        {
          //cout << "Record -" <<iter->key().data() <<":";
          //cout << particle_read_value->x<<particle_read_value->y<<particle_read_value->z<<particle_read_value->i<<particle_read_value->ux<<particle_read_value->uy<<particle_read_value->uz<<particle_read_value->ke<<endl;
          count++;
        }
      }

    }
  }
  
  return count;
}

int main() 
{
  BucketedDB* db = new BucketedDB(1, 28, 4);

  /*unordered_map<string, int> kv;
  kv["key1"] = 15;
  kv["key2"] = 25;
  kv["key3"] = 45;
  kv["key4"] = 75;
  kv["key5"] = 95;
  kv["key6"] = 11;
  kv["key7"] = 26;
  kv["key8"] = 47;
  kv["key9"] = 73;
  kv["key10"] = 99;
  kv["key11"] = 10;
  kv["key12"] = 21;
  kv["key13"] = 49;
  kv["key14"] = 72;
  kv["key15"] = 94;
  kv["key16"] = 8;
  kv["key17"] = 17;
  kv["key18"] = 86;
  kv["key19"] = 66;
  kv["key20"] = 68;
  kv["key21"] = 16;
  kv["key22"] = 33;
  kv["key23"] = 55;
  kv["key24"] = 48;
  kv["key25"] = 88;

  unordered_map<string, int>::iterator it;
  for(it = kv.begin();it != kv.end(); it++)
  {
    assert(db->put(it->first, to_string(it->second)).ok());

  }

  std::string value;

  for(it = kv.begin();it != kv.end(); it++)
  {
    // get value
    assert(db->get(it->first, &value).ok());
    assert(to_string(kv[it->first]) == value);
    cout << "Record -" << it->first <<":" <<  value << endl;

  }

  cout << "Value Range Query"<< endl;
  db->value_range_query(20, 65);*/

  vector<uint64_t> key_collection;
  bool flag = true;
  struct timeval start, stop; 
  /********************************************************LOADING THE DATA***************************************************************************/

  FILE* file_;
  file_ = fopen("iparticle.312.0.bin", "r");

  particle_schema* particle = new particle_schema();
  string read_value;
  particle_value_schema* particle_read_value;

  cout << "******************************Loading data......************************"<< endl;
  double total_time=0;
  double time;

  while (!feof(file_))
  {

    fread(particle, sizeof(particle_schema), 1, file_);
    rocksdb::Slice value((char*)(&particle->value), sizeof(particle_value_schema));
    rocksdb::Slice key((char*)(&particle->ID), sizeof(particle->ID));
    
    /*
    if((key_collection.size() <= 5000) && flag)
    {
      key_collection.push_back(particle->ID);
    }
    */

    //cout << particle->value.x<<particle->value.y<<particle->value.z<<particle->value.i<<particle->value.ux<<particle->value.uy<<particle->value.uz<<particle->value.ke<<endl;
    gettimeofday(&start, NULL); 
    assert(db->put(key, value, particle->value.ke).ok());
    gettimeofday(&stop, NULL);
    time = (stop.tv_sec-start.tv_sec)+0.000001*(stop.tv_usec-start.tv_usec);
    total_time += time;
    /*assert(db->get(key, &read_value).ok());
    particle_read_value = (particle_value_schema *)(read_value.data());
    cout << particle_read_value->x<<particle_read_value->y<<particle_read_value->z<<particle_read_value->i<<particle_read_value->ux<<particle_read_value->uy<<particle_read_value->uz<<particle_read_value->ke<<endl;
    assert(particle->value.x == particle_read_value->x);
    assert(particle->value.y == particle_read_value->y);
    assert(particle->value.z == particle_read_value->z);
    assert(particle->value.ux == particle_read_value->ux);
    assert(particle->value.uy == particle_read_value->uy);
    assert(particle->value.uz == particle_read_value->uz);
    assert(particle->value.i == particle_read_value->i);
    assert(particle->value.ke == particle_read_value->ke);

    flag = !flag;*/
  }
  cout << "Time required = " << total_time << " seconds" <<endl;
  cout << "******************************Loading data ends************************"<< endl;
  /*********************************************************************END***************************************************************************/





  /***************************************************TIME MEASUREMENTS FOR RANDOM GETS**************************************************************/
  /*
  cout << "**********************************Time Measurements**********************"<< endl;
  gettimeofday(&start, NULL); 
  for(uint16_t i= 0; i < key_collection.size(); i++)
  {
    rocksdb::Slice key((char*)(&key_collection[i]), sizeof(particle->ID));
    assert(db->get(key, &read_value).ok());
  }
  gettimeofday(&stop, NULL);
  double total_time = (stop.tv_sec-start.tv_sec)+0.000001*(stop.tv_usec-start.tv_usec);
  cout << "Time required = " << total_time << " seconds" <<endl;
  cout << "**********************************Time Measurement ends**********************"<< endl;
  */
  /****************************************************************END******************************************************************************/




  /********************************************************VALUE RANGE QUERY**************************************************************************/

  cout << "****************************************Value Range Query********************************"<< endl;
  uint32_t record_count;
  gettimeofday(&start, NULL); 
  //record_count = db->value_range_query(0.021, 0.027);  //spanning single bucket
  record_count = db->value_range_query(0.021, 0.077);  //spanning single bucket
  gettimeofday(&stop, NULL);
  double total_time1 = (stop.tv_sec-start.tv_sec)+0.000001*(stop.tv_usec-start.tv_usec);
  cout << "Time required = " << total_time1 << " seconds" <<endl;
  cout << "Record count = "<< record_count<<endl;

  /****************************************************************END*******************************************************************************/


  delete db;
  return 0;
  
}
