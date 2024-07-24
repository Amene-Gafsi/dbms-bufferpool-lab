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

TEST (unittest, SampleTestB)
{
  const char* db_name = "test1.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  StorageManager *sm = 0;
  BufferManager *bm = 0;

  int ret = InitStorageManager(db_name, &sm);
  assert(ret == 0);
  ret = InitBufferManager(buffer_pool_size, k, sm, &bm);
  assert(ret == 0);

  block_id bid_temp;
  auto *page0 = NewPage(&bid_temp, bm);

  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, bid_temp);

  snprintf(page0->data_, PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->data_, "Hello"));

  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, NewPage(&bid_temp, bm));
  }
  EXPECT_EQ(bm->free_list_, nullptr);

  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, NewPage(&bid_temp, bm));
  }

  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, UnpinPage(i, true, bm));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, NewPage(&bid_temp, bm));
  }

  page0 = FetchPage(0, bm);
  EXPECT_EQ(0, strcmp(page0->data_, "Hello"));

  EXPECT_EQ(true, UnpinPage(0, true, bm));
  EXPECT_NE(nullptr, NewPage(&bid_temp, bm));
  EXPECT_EQ(nullptr, FetchPage(0, bm));

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