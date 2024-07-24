#include <gtest/gtest.h>
#include <fcntl.h>
#include "unistd.h"
extern "C"
{
    #include "storage_manager.h"
    #include "bp_error.h"
}

const size_t k_ntests = 2;

TEST(unittest, read_less_than_page)
{
  StorageManager *p_sm = 0;
  // Create file
  int ret = InitStorageManager("test1", &p_sm);
  assert(access("test1", F_OK) == 0);
  char *data = (char*) malloc(PAGE_SIZE);
  for (unsigned i = 0; i< PAGE_SIZE / 2; i++){
    data[i] = i % 256;
  }
  for (unsigned i = PAGE_SIZE / 2; i < PAGE_SIZE; i++){
    data[i] = 0;
  }
  ret = WritePage(0, data, p_sm);
  EXPECT_EQ(ret, 0);
  char *data1 = (char*)malloc(PAGE_SIZE);
  for (unsigned i = 0; i< PAGE_SIZE; i++){
    data1[i] = (i + 10) % 256;
  }
  ret = ReadPage(0, data1, p_sm);
  for (unsigned i = 0; i < PAGE_SIZE/2; i++){
    EXPECT_EQ(data[i], data1[i]);
  }
  for (unsigned i = PAGE_SIZE / 2; i < PAGE_SIZE; i++){
    EXPECT_EQ(0, data1[i]);
  }
  ret = StopStorageManager(p_sm);
  ret = remove("test1");
  assert(ret == 0);
  free(data);
  free(data1);
}

TEST(unittest, interleaving_read_write)
{
  StorageManager *p_sm = 0;
  // Create file
  int ret = InitStorageManager("test2", &p_sm);
  assert(access("test2", F_OK) == 0);
  char *data = (char*) malloc(PAGE_SIZE);
  for (unsigned i = 0; i< PAGE_SIZE; i++){
    data[i] = i % 256;
  }
  ret = WritePage(0, data, p_sm);
  EXPECT_EQ(ret, 0);
  char *data1 = (char*)malloc(PAGE_SIZE);
  ret = ReadPage(0, data1, p_sm);
  for (unsigned i = 0; i < PAGE_SIZE; i++){
    EXPECT_EQ(data[i], data1[i]);
  }
  for (unsigned i = 0; i< PAGE_SIZE; i++){
    data[i] = (i+10) % 256;
  }
  ret = WritePage(0, data, p_sm);
  EXPECT_EQ(ret, 0);
  ret = ReadPage(0, data1, p_sm);
  EXPECT_EQ(ret, 0);
  for (unsigned i = 0; i < PAGE_SIZE; i++){
    EXPECT_EQ(data[i], data1[i]);
  }
  ret = StopStorageManager(p_sm);
  ret = remove("test2");
  assert(ret == 0);
  free(data);
  free(data1);

}

int
main (int argc, char **argv)
{  
  for (size_t i = 0; i < k_ntests; i++){
    std::string filename = "test";
    filename += ('1' + i);
    if (access(filename.c_str(), F_OK) == 0){
      remove(filename.c_str());
    }
  }
  ::testing::InitGoogleTest (&argc, argv);
  return RUN_ALL_TESTS ();
}
