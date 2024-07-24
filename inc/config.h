#pragma once

#define PAGE_SIZE 4096
#define INVALID_BLOCK_ID -1


// Currently our buffer pool supports size of maximum 4GB
typedef int block_id;
typedef int page_id;
