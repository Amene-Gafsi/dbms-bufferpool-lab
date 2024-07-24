#include <gtest/gtest.h>
#include "unistd.h"
extern "C"
{
    #include "replacer.h"
}

TEST(unittest, evicttest0){
  Replacer *replacer = 0;
  int ret = InitReplacer(3, 3, &replacer);
  page_id pid;

  ASSERT_EQ(ReplacerSize(replacer), 0);

  RecordAccess(1, replacer);
  RecordAccess(1, replacer);
  RecordAccess(1, replacer);
  RecordAccess(2, replacer);
  RecordAccess(2, replacer);
  RecordAccess(3, replacer);
  RecordAccess(3, replacer);
  RecordAccess(3, replacer);
  RecordAccess(2, replacer);

  ASSERT_EQ(ReplacerSize(replacer), 3);

  Evict(&pid, replacer);
  EXPECT_EQ(pid, 1);

  Evict(&pid, replacer);
  EXPECT_EQ(pid, 2);

  Evict(&pid, replacer);
  EXPECT_EQ(pid, 3);

  StopReplacer(replacer);
}

int
main (int argc, char **argv)
{
  ::testing::InitGoogleTest (&argc, argv);
  return RUN_ALL_TESTS ();
}