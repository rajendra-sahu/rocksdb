#include <atomic>
#include <mutex>
#include <condition_variable>

class Spinlock
{
  std::atomic_flag flag;
public:
  Spinlock(): flag(ATOMIC_FLAG_INIT) {}

  void lock(){
    while( flag.test_and_set() );
  }

  void unlock(){
    flag.clear();
  }
};


class Lock {
 public:
  Lock() = default;
  void lock() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (locked_) {
    cv_.wait(lock);
    }
    locked_ = true;
  }
  void unlock() {
    std::unique_lock<std::mutex> lock(mutex_);
    locked_ = false;
    cv_.notify_one();
  }
 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool locked_ = false;
};