// Copyright (c) Rajendra Sahu, Columnar Storage 


#include "bucket_example.h"

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
  options.max_open_files = 1000;
  // create the DB if it's not already present
  options.create_if_missing = true;

  options.prefix_extractor.reset(NewCappedPrefixTransform(8));
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  table_options.optimize_filters_for_memory = true;
  table_options.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  for(uint16_t i =0; i < instance_count; i++)
  {
    Status s = DB::Open(options, kDBPath + char(32+ i), &bucketedDB[i]);
    assert(s.ok());
    put_record_count[i] = 0;

  }
}

BucketedDB::BucketedDB(uint16_t count, uint8_t offset, uint8_t size)
{
  instance_count = count;
  pivot_offset = offset;
  pivot_size = size;  

  cout << "Creating a db of insatnce count : "<<instance_count<<endl;

  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;

  options.prefix_extractor.reset(NewCappedPrefixTransform(8));
  table_options.filter_policy.reset(NewBloomFilterPolicy(30, false));
  table_options.optimize_filters_for_memory = true;
  table_options.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));


  for(uint16_t i =0; i < instance_count; i++)
  {
    Status s = DB::Open(options, kDBPath + char(48+ i), &bucketedDB[i]); 
    assert(s.ok());
    put_record_count[i] = 0;
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

#ifdef BACKGROUND_GC_
rocksdb::Status BucketedDB::put( const rocksdb::Slice &key, const rocksdb::Slice &value, float pivot)
{
  uint16_t new_index = get_index(pivot);
  uint16_t old_index;
  bool found = false;
  //cout << "Putting in bucket = " <<index<<endl;
  string read_value;
  Status s;

  /*
  for(uint16_t i =0; i < instance_count; i++)
  {
    s = bucketedDB[i]->Get(ReadOptions(), key, &read_value);
    if(s.IsNotFound())
    {
      continue;
    }
    if(s.ok())
    {
      old_index = i;
      found = true;
      break;
    }
  }
  */

  //Bloom filter implementation
  
  for(uint16_t i =0; i < instance_count; i++)
  {
    if(!bucketedDB[i]->KeyMayExist(ReadOptions(), key, &read_value))
    {
      continue;
    }
    else                                                          //might be a case of false positive
    {
      s = bucketedDB[i]->Get(ReadOptions(), key, &read_value);    //Do actual get to confirm 
      if(s.IsNotFound())
      {
        continue;
      }
      if(s.ok())
      {
        old_index = i;
        found = true;
        break;
      }
    }

  }
  

  if(found)
  {
    if(old_index == new_index)
    {
      return bucketedDB[new_index]->Put(WriteOptions(), key, value);
    }
    else
    {
      assert(bucketedDB[old_index]->SingleDelete(WriteOptions(), key).ok());
      put_record_count[old_index]--;
      put_record_count[new_index]++;
      return bucketedDB[new_index]->Put(WriteOptions(), key, value);
    }
  }
  else
  {
    put_record_count[new_index]++;
    return bucketedDB[new_index]->Put(WriteOptions(), key, value);
  }
  

}
#else
rocksdb::Status BucketedDB::put( const rocksdb::Slice &key, const rocksdb::Slice &value, float pivot)
{

  //Step1: Do the actual Put
  Status put_status = bucketedDB[get_index(pivot)]->Put(WriteOptions(), key, value);

  //Step2: Enqueu a delete background request

  if(gc_queue.size() < GC_QUEUE_SIZE)
  {
    gc_queue.push(key.data());
  }



}

#endif


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

void BucketedDB::print_db_stat()
{
  rocksdb::Iterator* iter;
  uint64_t count = 0;
  string read_value;
  particle_value_schema * particle_read_value;
  for(uint16_t i = 0; i< instance_count; i++)
  {
    iter = bucketedDB[i]->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
      if(iter->status().ok())
      {
        assert(bucketedDB[i]->Get(ReadOptions(), iter->key(), &read_value).ok());
        particle_read_value = (particle_value_schema *)(read_value.data());
        //cout << particle_read_value->x<<particle_read_value->y<<particle_read_value->z<<particle_read_value->i<<particle_read_value->ux<<particle_read_value->uy<<particle_read_value->uz<<particle_read_value->ke<<endl;
        count++;
      }
    }
    cout << "DB Bucket number : " << i << "Records Count :"<< count<<endl;
    cout << "DB Bucket number : " << i << "Expected Records Count :"<< put_record_count[i]<<endl;
    count = 0;
  }
}


int main() 
{
  BucketedDB* db = new BucketedDB(25, 28, 4);

  vector<uint64_t> key_collection;
  bool flag = true;
  struct timeval start, stop; 

  db->print_db_stat();
  /********************************************************LOADING THE DATA***************************************************************************/
  DIR *dr;
  struct dirent *en;
  //string dataset_path = "/home/rajendrasahu/workspace/c2-vpic-sample-dataset/particles/";
  string dataset_path = "/home/rajendrasahu/workspace/ouo-vpic-dataset/";
  dr = opendir(dataset_path.c_str());

  FILE* file_;
  //file_ = fopen("/home/rajendrasahu/workspace/c2-vpic-sample-dataset/particles/iparticle.312.0.bin", "r");

  particle_schema* particle = new particle_schema();
  string read_value;
  particle_value_schema* particle_read_value;

  cout << "******************************Loading data......************************"<< endl;
  double total_time=0;
  double time;
  uint8_t flag1 = 0;
  uint64_t file_record_count = 0;
  if (dr) 
  {
    while ((en = readdir(dr)) != NULL)
    {
      if((en->d_type !=8) || (flag1 >= 1))     //valid file type
      continue;
      cout<<"Reading from "<<en->d_name<<endl; //print file name
      string s(en->d_name);
      s = dataset_path + s;
      file_ = fopen(s.c_str(), "r");
      flag1++;
      while (!feof(file_))
      {

        fread(particle, sizeof(particle_schema), 1, file_);
        rocksdb::Slice value((char*)(&particle->value), sizeof(particle_value_schema));
        rocksdb::Slice key((char*)(&particle->ID), sizeof(particle->ID));
        file_record_count++;
        /*
        if((key_collection.size() <= 50000) && flag)
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

        /*
        if(file_record_count == 50)
        {
          db->print_db_stat();
          break;
        }
        */
      }

      cout<<"Record count in "<<en->d_name<< ": " << file_record_count <<endl; //print file name
      file_record_count = 0;
    }
    closedir(dr); //close all directory
  }

  cout << "Time required = " << total_time << " seconds" <<endl;
  cout << "******************************Loading data ends************************"<< endl;
  /*********************************************************************END***************************************************************************/


  db->print_db_stat();


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
  double total_time1 = (stop.tv_sec-start.tv_sec)+0.000001*(stop.tv_usec-start.tv_usec);
  cout << "Time required = " << total_time1 << " seconds" <<endl;
  cout << "**********************************Time Measurement ends**********************"<< endl;
  */
  /****************************************************************END******************************************************************************/




  /********************************************************VALUE RANGE QUERY**************************************************************************/
  
  cout << "****************************************Value Range Query********************************"<< endl;
  uint64_t record_count;
  gettimeofday(&start, NULL); 
  //record_count = db->value_range_query(0.021, 0.027);  //spanning single bucket
  record_count = db->value_range_query(0.021, 0.027);  //spanning single bucket
  gettimeofday(&stop, NULL);
  double total_time2 = (stop.tv_sec-start.tv_sec)+0.000001*(stop.tv_usec-start.tv_usec);
  cout << "Time required = " << total_time2 << " seconds" <<endl;
  cout << "Record count = "<< record_count<<endl;
  
  /****************************************************************END*******************************************************************************/


  delete db;
  return 0;
  
}
