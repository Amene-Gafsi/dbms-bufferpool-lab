#include <gtest/gtest.h>
#include <fcntl.h>
#include "unistd.h"
extern "C"
{
    #include "storage_manager.h"
    #include "bp_error.h"
}

const size_t k_ntests = 3;

TEST(unittest, write_block)
{
  StorageManager *p_sm = 0;
  // Create file
  int ret = InitStorageManager("test1", &p_sm);
  assert(access("test1", F_OK) == 0);
  char *data = (char*) malloc(PAGE_SIZE);
  for (unsigned i = 0;  i < PAGE_SIZE; i++){
    data[i] = i % 256;
  }
  ret = WritePage(0, data, p_sm);
  EXPECT_EQ(ret, 0);
  // The file size should be 1 page size
  fseek(p_sm->file_ptr_, 0, SEEK_END);
  size_t sz = ftell(p_sm->file_ptr_);
  EXPECT_EQ(sz, 1 * PAGE_SIZE);

  ret = WritePage(10, data, p_sm);
  fseek(p_sm->file_ptr_, 0, SEEK_END);
  sz = ftell(p_sm->file_ptr_);
  EXPECT_EQ(sz, 11 * PAGE_SIZE);

  ret = WritePage(20, data, p_sm);
  fseek(p_sm->file_ptr_, 0, SEEK_END);
  sz = ftell(p_sm->file_ptr_);
  EXPECT_EQ(sz, 21 * PAGE_SIZE);
  ret = StopStorageManager(p_sm);
  ret = remove("test1");
  assert(ret == 0);
  free(data);
}

TEST(unittest, read_block_non_exist)
{
  StorageManager *p_sm = 0;
  // Create file
  int ret = InitStorageManager("test2", &p_sm);
  assert(ret == 0);
  assert(access("test2", F_OK) == 0);
  char *data = (char*) malloc(PAGE_SIZE);
  ret = ReadPage(0, data, p_sm);
  EXPECT_EQ(ret, -ESTORAGEREAD);
  ret = ReadPage(10, data, p_sm);
  EXPECT_EQ(ret, -ESTORAGEREAD);
  ret = StopStorageManager(p_sm);
  ret = remove("test2");
  assert(ret == 0);
  free(data);
}

TEST(unittest, read_block_exist)
{
  auto *fptr = fopen("test3", "w+");
  char *data = (char*) malloc(PAGE_SIZE);
  for (unsigned i = 0;  i < PAGE_SIZE; i++){
    data[i] = i % 256;
  }
  fseek(fptr, 0, SEEK_SET);
  fwrite(data, PAGE_SIZE, 1, fptr);
  fclose(fptr);
  StorageManager *p_sm = 0;
  // Create file
  int ret = InitStorageManager("test3", &p_sm);
  assert(access("test3", F_OK) == 0);
  char *data1 = (char*) malloc(PAGE_SIZE);
  ret = ReadPage(0, data1, p_sm);
  EXPECT_EQ(ret, 0);
  for (unsigned i = 0; i < PAGE_SIZE; i++){
    EXPECT_EQ(data[i], data1[i]);
  }

  ret = StopStorageManager(p_sm);
  ret = remove("test3");
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
