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

  update_mode = false;

  for(uint16_t i =0; i < instance_count; i++)
  {
    Status s = DB::Open(options, kDBPath + char(32+ i), &bucketedDB[i]);
    assert(s.ok());
    put_record_count[i] = 0;

  }

  gc_queue = new MPMCQueue<gc_request> (GC_QUEUE_SIZE);
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

  update_mode = false;

  for(uint16_t i =0; i < instance_count; i++)
  {
    Status s = DB::Open(options, kDBPath + char(48+ i), &bucketedDB[i]); 
    assert(s.ok());
    put_record_count[i] = 0;
  }

  gc_queue = new MPMCQueue<gc_request> (GC_QUEUE_SIZE);
}

BucketedDB::~BucketedDB()
{
  for(uint16_t i =0; i < instance_count; i++)
  {
    delete bucketedDB[i];

  }
  delete gc_queue;
}

uint16_t BucketedDB::get_index(float value) //Implement the hashing fuction
{
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

void BucketedDB::flush()
{
  for(uint16_t i =0; i < instance_count; i++)
  {
    Status s = bucketedDB[i]->Flush(flush_options); 
    assert(s.ok());
  } 
}

void BucketedDB::compact()
{
  for(uint16_t i =0; i < instance_count; i++)
  {
    Status s = bucketedDB[i]->CompactRange(compact_range_options, nullptr, nullptr); 
    assert(s.ok());
  } 
}


rocksdb::Status BucketedDB::put( const rocksdb::Slice &key, string value)   //deprecated, shouldn't be used
{
  return bucketedDB[get_index(stof(value.substr(pivot_offset, pivot_size)))]->Put(WriteOptions(), key, value);
}

#ifdef NO_BACKGROUND_GC_
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
  

  //debugging prints    
  particle_value_schema* particle_read_value = (particle_value_schema*)(value.data());  
  cout << "Record - " <<(uint64_t)(*(uint64_t*)(key.data()))<<" -> ";
  cout << particle_read_value->x << " : "<<particle_read_value->y<<" : " <<particle_read_value->z<<" : " <<particle_read_value->i<<" : " <<particle_read_value->ux<<" : " <<particle_read_value->uy<<" : " <<particle_read_value->uz<<" : " <<particle_read_value->ke<<endl;
  
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

  uint16_t index = get_index(pivot);
  Status put_status;

  //debugging prints
  /*    
  if(index == 0)
  {
    particle_value_schema* particle_read_value = (particle_value_schema*)(value.data());
    cout << "Record - " <<(uint64_t)(*(uint64_t*)(key.data()))<<" -> ";
    cout << particle_read_value->x << " : "<<particle_read_value->y<<" : " <<particle_read_value->z<<" : " <<particle_read_value->i<<" : " <<particle_read_value->ux<<" : " <<particle_read_value->uy<<" : " <<particle_read_value->uz<<" : " <<particle_read_value->ke<<endl;
  }
  */

  //Step1: Do the actual Put
  {
    std::unique_lock<std::mutex> lock(bucket_lock[index]);
    //Processing
    put_status = bucketedDB[index]->Put(WriteOptions(), key, value);
    put_record_count[index]++;

  } 



  //Step2: Enqueu a delete background request, no need of locking here, queue is thread safe, lockless

  
  gc_request request;
  request.key = (uint64_t)(*(uint64_t*)(key.data()));
  //cout << "Pushing key into quueue"<< request.key<<endl;
  request.new_bucket_index = index;
  bool succeeded = gc_queue->try_push(request);
  assert(succeeded);
  

  /*
  if(update_mode)
  {
    gc_request request;
    request.key = (uint64_t)(*(uint64_t*)(key.data()));
    //cout << "Pushing key into quueue"<< request.key<<endl;
    request.new_bucket_index = index;
    bool succeeded = gc_queue->try_push(request);
    assert(succeeded);
  }
  */

  return put_status;

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
      cout << "Record - " <<(uint64_t)(*(uint64_t*)(iter->key().data())) <<" -> ";
      cout << particle_read_value->x<<particle_read_value->y<<particle_read_value->z<<particle_read_value->i<<particle_read_value->ux<<particle_read_value->uy<<particle_read_value->uz<<particle_read_value->ke<<endl;
    }
  }
}

uint32_t BucketedDB::value_range_query(float pv1, float pv2)
{

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
        count++;
      }
    }
    cout << "DB Bucket number : " << i << "Records Count :"<< count<<endl;
    cout << "DB Bucket number : " << i << "Expected Records Count :"<< put_record_count[i]<<endl;
    count = 0;
  }
}

bool BucketedDB::gc_function()
{
  rocksdb::Slice key;
  gc_request pop_value;
  bool found = false;
  string read_value;
  Status s;
  uint16_t old_index;
  uint32_t request_counter = 0;
  bool gc_flag = false;

  //put some delay for warmup 
  std::this_thread::sleep_for(std::chrono::milliseconds(1));                   //CALIBRATED


  //now start popping delete requests
  //cout << " GC Thread trying to dequeue"<< endl;
  while(gc_queue->try_pop(pop_value))
  {
    //cout << "Thread got actual delete requests"<< endl;

    gc_flag= true;
    rocksdb::Slice key((char*)(&pop_value.key), 8);
    request_counter++;
    //bloom filter checking
    for(uint16_t i =0; i < instance_count; i++)
    {
      std::unique_lock<std::mutex> lock(bucket_lock[i]);
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
      //TODO:eliminate doing puts when old_index == new_index (can't distinguish between fresh put and update)
      //doing actual delete after finding the dulplicate key
      if(old_index != pop_value.new_bucket_index)    //when buckets don't match then only delete
      {
        std::unique_lock<std::mutex> lock(bucket_lock[old_index]);

        //Processing
        assert(bucketedDB[old_index]->SingleDelete(WriteOptions(), key).ok());
        put_record_count[old_index]--;
      }


      found = false;
    }
    else
    {
      //No delete to process; fresh key  //TODO This case is abnormal if a delete request has been pushed  
      continue;
    }

  }

  return true;
}

int main() 
{
  BucketedDB* db = new BucketedDB(5, 28, 4);

  vector<uint64_t> key_collection;
  struct timeval start, stop; 



  db->print_db_stat();
  /********************************************************LOADING THE DATA***************************************************************************/
  DIR *dr;
  struct dirent *en;
  string dataset_path = "/home/rajendrasahu/workspace/c2-vpic-sample-dataset/particles/";
  //string dataset_path = "/home/rajendrasahu/workspace/ouo-vpic-dataset/";
  dr = opendir(dataset_path.c_str());
  FILE* file_;

  particle_schema* particle = new particle_schema();
  string read_value;

  cout << "******************************Loading data......************************"<< endl;
  double total_time=0;
  double time;
  uint8_t files_count = 0;
  uint64_t file_record_count = 0;
  if (dr) 
  {
    while ((en = readdir(dr)) != NULL)
    {
      if((en->d_type !=8) /*|| (files_count >= 2)*/)     //valid file type
      continue;
      cout<<"Reading from "<<en->d_name<<endl; //print file name
      string s(en->d_name);
      s = dataset_path + s;
      file_ = fopen(s.c_str(), "r");

      if(files_count == 1)
      { db->update_mode = true;}  //TODO implement this attribute

      files_count++;

      #ifndef NO_BACKGROUND_GC_
      auto backgroundThread = async(launch::async, &BucketedDB::gc_function, db);     //launch bg thread
      #endif

      while (!feof(file_))
      {

        fread(particle, sizeof(particle_schema), 1, file_);
        rocksdb::Slice value((char*)(&(particle->value)), sizeof(particle_value_schema));
        rocksdb::Slice key((char*)(&(particle->ID)), sizeof(particle->ID));
        file_record_count++;

        gettimeofday(&start, NULL); 
        assert(db->put(key, value, particle->value.ke).ok());
        gettimeofday(&stop, NULL);
        time = (stop.tv_sec-start.tv_sec)+0.000001*(stop.tv_usec-start.tv_usec);
        total_time += time;

        /*
        if(file_record_count == 50)
        {
          break;
        }
        */
        

      }

      #ifndef NO_BACKGROUND_GC_
      gettimeofday(&start, NULL); 
      backgroundThread.wait();
      assert(backgroundThread.get());
      gettimeofday(&stop, NULL);
      time = (stop.tv_sec-start.tv_sec)+0.000001*(stop.tv_usec-start.tv_usec);
      total_time += time;
      cout << "Garbage collection done" << std::endl;
      #endif

      cout<<"Record count in "<<en->d_name<< ": " << file_record_count <<endl; //print file name
      file_record_count = 0;


    }
    closedir(dr); //close all directory
  }

  cout << "Time required to load = " << total_time << " seconds" <<endl;
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
