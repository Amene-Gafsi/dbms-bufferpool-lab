#include <gtest/gtest.h>
#include "unistd.h"
extern "C"
{
    #include "replacer.h"
}

TEST(unittest, evicttest2)
{
  Replacer *replacer = 0;
  int ret = InitReplacer(7, 2, &replacer);
  assert(ret == 0);

  RecordAccess(1, replacer);
  RecordAccess(2, replacer);
  RecordAccess(3, replacer);
  RecordAccess(4, replacer);
  RecordAccess(5, replacer);
  RecordAccess(6, replacer);
  SetEvictable(1, true, replacer);
  SetEvictable(2, true, replacer);
  SetEvictable(3, true, replacer);
  SetEvictable(4, true, replacer);
  SetEvictable(5, true, replacer);
  SetEvictable(6, false, replacer);
  ASSERT_EQ(5, ReplacerSize(replacer));

  RecordAccess(1, replacer);

  int value;
  Evict(&value, replacer);
  ASSERT_EQ(2, value);
  Evict(&value, replacer);
  ASSERT_EQ(3, value);
  Evict(&value, replacer);
  ASSERT_EQ(4, value);
  ASSERT_EQ(2, ReplacerSize(replacer));

  RecordAccess(3, replacer);
  RecordAccess(4, replacer);
  RecordAccess(5, replacer);
  RecordAccess(4, replacer);
  SetEvictable(3, true, replacer);
  SetEvictable(4, true, replacer);
  ASSERT_EQ(4, ReplacerSize(replacer));

  Evict(&value, replacer);
  ASSERT_EQ(3, value);
  ASSERT_EQ(3, ReplacerSize(replacer));

  SetEvictable(6, true, replacer);
  ASSERT_EQ(4, ReplacerSize(replacer));
  Evict(&value, replacer);
  ASSERT_EQ(6, value);
  ASSERT_EQ(3, ReplacerSize(replacer));

  SetEvictable(1, false, replacer);
  ASSERT_EQ(2, ReplacerSize(replacer));
  ASSERT_EQ(true, Evict(&value, replacer));
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, ReplacerSize(replacer));

  RecordAccess(1, replacer);
  RecordAccess(1, replacer);
  SetEvictable(1, true, replacer);
  ASSERT_EQ(2, ReplacerSize(replacer));
  ASSERT_EQ(true, Evict(&value, replacer));
  ASSERT_EQ(value, 4);

  ASSERT_EQ(1, ReplacerSize(replacer));
  Evict(&value, replacer);
  ASSERT_EQ(value, 1);
  ASSERT_EQ(0, ReplacerSize(replacer));

  ASSERT_EQ(false, Evict(&value, replacer));
  ASSERT_EQ(0, ReplacerSize(replacer));
  StopReplacer(replacer);
}

int
main (int argc, char **argv)
{
  ::testing::InitGoogleTest (&argc, argv);
  return RUN_ALL_TESTS ();
}