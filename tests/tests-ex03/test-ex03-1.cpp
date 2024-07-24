#include <gtest/gtest.h>
#include <cstdlib>
#include <ctime>
#include <random>
#include "unistd.h"

extern "C"
{
    #include "buffer_manager.h"
}

const size_t k_ntests = 1;

TEST (unittest, SampleTestA)
{
  const char* db_name = "test1.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  StorageManager *sm = 0;
  BufferManager *bm = 0;


  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<uint8_t> uniform_dist(0);

  int ret = InitStorageManager(db_name, &sm);
  assert(ret == 0);
  ret = InitBufferManager(buffer_pool_size, k, sm, &bm);
  assert(ret == 0);

  block_id bid_temp;
  Page *page0 = NewPage(&bid_temp, bm);

  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, bid_temp);

  char random_binary_data[PAGE_SIZE];
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  random_binary_data[PAGE_SIZE / 2] = '\0';
  random_binary_data[PAGE_SIZE - 1] = '\0';

  memcpy(page0->data_, random_binary_data, PAGE_SIZE);
  EXPECT_EQ(0, memcmp(page0->data_, random_binary_data, PAGE_SIZE));

  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, NewPage(&bid_temp, bm));
  }

  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, NewPage(&bid_temp, bm));
  }

  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, UnpinPage(i, true, bm));
    FlushPage(i, bm);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, NewPage(&bid_temp, bm));
    UnpinPage(bid_temp, false, bm);
  }
  
  page0 = FetchPage(0, bm);
  EXPECT_EQ(0, memcmp(page0->data_, random_binary_data, PAGE_SIZE));
  EXPECT_EQ(true, UnpinPage(0, true, bm));

  StopStorageManager(sm);
  StopBufferManager(bm);
  remove("test1.db");
}

int
main (int argc, char **argv)
{
  for (size_t i = 0; i < k_ntests; i++){
    std::string filename = "test";
    filename += ('1' + i);
    filename += ".db";
    if (access(filename.c_str(), F_OK) == 0){
      remove(filename.c_str());
    }
  }
  ::testing::InitGoogleTest (&argc, argv);
  return RUN_ALL_TESTS ();
}