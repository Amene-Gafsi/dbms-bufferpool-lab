#pragma once
#include "config.h"

#include <stdbool.h>

typedef struct Page_t{
    char* data_;
    block_id block_id_;
    int pin_count_;
    bool is_dirty_;
} Page;
