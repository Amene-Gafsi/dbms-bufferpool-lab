#include "buffer_manager.h"
#include "replacer.h"
#include "bp_error.h"
#include <assert.h>


int InitBufferManager(size_t pool_size, size_t k,  StorageManager *sm, BufferManager **bm){
    if (*bm || pool_size == 0 || sm == NULL) return -EBUFFERMANAGER;
    *bm = malloc(sizeof(BufferManager));
    memset(*bm, 0, sizeof(BufferManager));
    (*bm)->next_block_id_ = 0;
    (*bm)->pool_size_ = pool_size;
    (*bm)->pages_ = malloc(sizeof(Page) * pool_size);
    for (size_t i = 0; i < pool_size; i++){
        (*bm)->pages_[i].pin_count_ = 0;
        (*bm)->pages_[i].is_dirty_ = 0;
        (*bm)->pages_[i].block_id_ = INVALID_BLOCK_ID;
        (*bm)->pages_[i].data_ = malloc(sizeof(char) * PAGE_SIZE);
    }
    (*bm)->storage_manager_ = sm;
    (*bm)->replacer_ = NULL;
    int ret = InitReplacer(pool_size, k, &((*bm)->replacer_));
    if (ret) {
        for (size_t i = 0; i < pool_size; i++){
            free((*bm)->pages_[i].data_);
        }
        free((*bm)->pages_);
        free(bm);
        return -EBUFFERMANAGER;
    }
    FreeListNode *node = malloc(sizeof(FreeListNode));
    node->page_id_ = 0;
    node->next_ = NULL;
    (*bm)->free_list_  = node;
    for (size_t i = 1; i < pool_size; i++){
        FreeListNode *new_node = malloc(sizeof(FreeListNode));
        new_node->page_id_ = i;
        new_node->next_ = NULL;
        node->next_ = new_node;
        node = new_node;
    }
    return 0;
}

int StopBufferManager(BufferManager *bm){
    if (!bm) return -EBUFFERMANAGER;
    // Storage Manager will be handled separately.
    // Delete the page_table_ first    
    PageTableNode *current_node, *tmp;

    HASH_ITER(hh, bm->page_table_, current_node, tmp) {
        HASH_DEL(bm->page_table_, current_node);
        free(current_node); 
    }
    FreeListNode *node = bm->free_list_;
    FreeListNode *next_node;
    while(node) {
        next_node = node->next_;
        free(node);
        node = next_node;
    }
    for(size_t i = 0; i < bm->pool_size_; i++){
        free(bm->pages_[i].data_);
    }
    free(bm->pages_);
    StopReplacer(bm->replacer_);
    free(bm);
    return 0;
}

Page* FetchPage(block_id id, BufferManager *bm) {
    if (bm == NULL) {
        return NULL;
    }
    PageTableNode *pt_node = NULL;
    HASH_FIND_INT(bm->page_table_, &id, pt_node);
    if (false) {
        Page *page = &bm->pages_[pt_node->pid_];
        page->pin_count_++;
        RecordAccess(pt_node->pid_, bm->replacer_);
        return page;
    }
    Page *page = NULL;
    FreeListNode *prev_free = NULL, *free_node = bm->free_list_;
    while (free_node) {
        if (bm->pages_[free_node->page_id_].pin_count_ == 0) {
            page = &bm->pages_[free_node->page_id_];
            if (prev_free) {
                prev_free->next_ = free_node->next_;
            } else {
                bm->free_list_ = free_node->next_;
            }
            free(free_node);
            break;
        }
        prev_free = free_node;
        free_node = free_node->next_;
    }
    if (!page) {
        page_id evict_id;
        if (Evict(&evict_id, bm->replacer_)) {
            page = &bm->pages_[evict_id];
        }
    }
    if (!page || page->pin_count_ != 0) {
        return NULL;
    }
    if (page->is_dirty_) {
        FlushPage(page->block_id_, bm);
    }
    if (ReadPage(id, page->data_, bm->storage_manager_) != 0) {
        return NULL; 
    }
    pt_node = malloc(sizeof(PageTableNode));
    if (!pt_node) {
        return NULL;
    }
    pt_node->bid_ = id;
    pt_node->pid_ = page - bm->pages_;
    HASH_ADD_INT(bm->page_table_, bid_, pt_node);
    page->block_id_ = id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    RecordAccess(pt_node->pid_, bm->replacer_);
    return page;
}


Page* NewPage(block_id *id, BufferManager *bm) {
    /* Replace a page or find a free page. */
    
    /* Allocate the block */

    /* Allocate a new PageTableNode */

    /* Record the access and mark it as evictable */
    Page *page = NULL;
    FreeListNode *free_node = bm->free_list_;
    if (free_node != NULL) {
        page = &(bm->pages_[free_node->page_id_]);
        bm->free_list_ = free_node->next_;
        free(free_node);
    } else {
        page_id evict_id;
        if (Evict(&evict_id, bm->replacer_) && bm->pages_[evict_id].pin_count_ == 0) {
            page = &(bm->pages_[evict_id]);
            if (page->is_dirty_) {
                FlushPage(page->block_id_, bm);
            }
        }
    }
    if (page == NULL || page->pin_count_ != 0) {
        *id = INVALID_BLOCK_ID;
        return NULL;
    }
    *id = AllocateBlock(bm);
    page->block_id_ = *id;
    page->pin_count_ = 1;  
    page->is_dirty_ = false;
    memset(page->data_, 0, PAGE_SIZE); 
    PageTableNode *pt_node = malloc(sizeof(PageTableNode));
    pt_node->bid_ = *id;
    pt_node->pid_ = page - bm->pages_; 
    HASH_ADD_INT(bm->page_table_, bid_, pt_node);
    RecordAccess(pt_node->pid_, bm->replacer_);
    ReplacerHashNode *hash_node = NULL;
    HASH_FIND_INT(bm->replacer_->node_store_, &(pt_node->pid_), hash_node);
    SetEvictable(hash_node->id_,false, bm->replacer_);
    return page;
}

bool UnpinPage(block_id id, bool is_dirty, BufferManager *bm){
    PageTableNode *pt_node;
    HASH_FIND_INT(bm->page_table_, &id, pt_node);
    if (pt_node == NULL) {
        return false;}
    Page *page = &(bm->pages_[pt_node->pid_]);
    if (page->pin_count_ <= 0) {
        return false; }
    page->pin_count_--;
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    if (page->pin_count_ <= 0) {
        SetEvictable(pt_node->pid_, true, bm->replacer_);
    }
    return true;
}

bool FlushPage(block_id id, BufferManager *bm) {
    PageTableNode *pt_node = NULL;
    HASH_FIND_INT(bm->page_table_, &id, pt_node);
    if (pt_node == NULL) {
        return false; 
    }
    Page *page = &(bm->pages_[pt_node->pid_]);
    if (WritePage(id, page->data_, bm->storage_manager_) == 0) {
        page->is_dirty_ = false;
        return true;
    }
    return false;
}

bool DeletePage(block_id id, BufferManager *bm) {
    PageTableNode *pt_node;
    HASH_FIND_INT(bm->page_table_, &id, pt_node);
    if (!pt_node) return true; 
    Page *page = &(bm->pages_[pt_node->pid_]);
    if (page->pin_count_ > 0) return false;
    if (page->is_dirty_) {
        if (!FlushPage(id, bm)) { 
            return false;
        }
    }
    page->block_id_ = INVALID_BLOCK_ID;
    page->pin_count_ = 0;
    page->is_dirty_ = false;
    memset(page->data_, 0, PAGE_SIZE); 
    HASH_DEL(bm->page_table_, pt_node);
    free(pt_node);
    Remove(page->block_id_, bm->replacer_);
    FreeListNode *new_free_node = malloc(sizeof(FreeListNode));
    new_free_node->page_id_ = pt_node->pid_;
    new_free_node->next_ = bm->free_list_;
    bm->free_list_ = new_free_node;
    return true;
}

block_id AllocateBlock(BufferManager *bm) {
    if (bm->next_block_id_ == INVALID_BLOCK_ID) {
        return INVALID_BLOCK_ID; }
    block_id new_block_id = bm->next_block_id_;
    bm->next_block_id_++; 
    return new_block_id;}
