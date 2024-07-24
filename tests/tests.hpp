#ifndef LAB2_TESTS_TESTS_HPP
#define LAB2_TESTS_TESTS_HPP

#include <cstdlib>
#include <gtest/gtest.h>
#include <string>
#include <vector>

class MinimalistPrinter : public testing::EmptyTestEventListener
{
  // Called after a failed assertion or a SUCCESS().
  void
  OnTestPartResult (const testing::TestPartResult &test_part_result) override
  {
    if (test_part_result.failed ())
      std::cout << "[HIDDEN TEST FAILED] " << message_ << '\n';
  }

  void
  OnTestEnd (const testing::TestInfo &test_info) override
  {
    if (test_info.result ()->Passed ())
      std::cout << "[HIDDEN TEST PASSED]\n";
  }
  std::string message_;

public:
  MinimalistPrinter (std::string message) : message_ (message) {}
};

#define MARK_HIDDEN_TEST(msg)                                                 \
  [&] () {                                                                    \
    testing::TestEventListeners &listeners                                    \
        = testing::UnitTest::GetInstance ()->listeners ();                    \
    delete listeners.Release (listeners.default_result_printer ());           \
    listeners.Append (new MinimalistPrinter (msg));                           \
  }()

#endif