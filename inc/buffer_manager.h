#pragma once

#include "page.h"
#include "stddef.h"
#include "storage_manager.h"
#include "replacer.h"
#include "config.h"
#include "uthash.h"

typedef struct FreeListNode_t{
  page_id page_id_;
  struct FreeListNode_t* next_;
}FreeListNode;

typedef struct PageTableNode_t {
  block_id bid_;
  page_id pid_;
  UT_hash_handle hh;
}PageTableNode;

typedef struct BufferManager_t{
    block_id next_block_id_;
    size_t pool_size_;
    Page *pages_;
    FreeListNode *free_list_;
    StorageManager *storage_manager_;
    Replacer *replacer_;
    struct PageTableNode_t *page_table_;
}BufferManager;

int InitBufferManager(size_t pool_size, size_t k, StorageManager *sm, BufferManager **bm);

int StopBufferManager(BufferManager *bm);

Page* FetchPage(block_id id, BufferManager *bm);

Page* NewPage(block_id *id, BufferManager *bm);

bool UnpinPage(block_id id, bool is_dirty, BufferManager *bm);

bool FlushPage(block_id id, BufferManager *bm);

bool DeletePage(block_id id, BufferManager *bm);

block_id AllocateBlock(BufferManager *bm);
