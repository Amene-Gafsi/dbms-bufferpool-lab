#pragma once
#include "config.h"
#include "uthash.h"
#include "bp_error.h"

#include "stddef.h"
#include <stdbool.h>

typedef struct HistoryNode_t{
    size_t ts_;
    struct HistoryNode_t *next_;
}HistoryNode;

typedef struct KLRUListNode_t{
    page_id id_;
    struct KLRUListNode_t *next_;
}KLRUListNode;

typedef struct ReplacerNode_t{
    bool is_evictable_;
    page_id pid_;
    size_t k_;
    size_t history_size_;
    struct HistoryNode_t *history_;
}ReplacerNode;

typedef struct ReplacerHashNode_t{
    page_id id_;
    ReplacerNode lru_node_;
    UT_hash_handle hh;
}ReplacerHashNode;

typedef struct Replacer_t{
    // Map frame id to node
    ReplacerHashNode *node_store_;
    KLRUListNode *list_; // k-LRU list 
    size_t replacer_size_;
    size_t k_;
    size_t current_size_;
    size_t current_ts_;
}Replacer;

int InitReplacer(size_t pool_size, size_t k, Replacer **r);

int StopReplacer(Replacer *r);

bool Evict(page_id *id, Replacer *r);

void RecordAccess(page_id id, Replacer *r);

void SetEvictable(page_id id, bool set_evictable, Replacer *r);

void Remove(page_id id, Replacer *r);

size_t ReplacerSize(Replacer *r);
