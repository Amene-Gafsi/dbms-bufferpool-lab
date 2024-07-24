#include "replacer.h"
#include <assert.h>
#include <stdio.h>

int InitReplacer(size_t pool_size, size_t k, Replacer **r)
{
    if (!pool_size || !k || *r)
        return -EREPLACER;
    *r = (Replacer *)malloc(sizeof(Replacer));
    memset(*r, 0, sizeof(**r));
    (*r)->replacer_size_ = pool_size;
    (*r)->k_ = k;
    return 0;
}

int StopReplacer(Replacer *r)
{
    if (!r)
        return -EREPLACER;
    KLRUListNode *node = r->list_;
    KLRUListNode *next_node = NULL;
    while (node)
    {
        next_node = node->next_;
        page_id pid = node->id_;
        ReplacerHashNode *hash_node = NULL;
        HASH_FIND_INT(r->node_store_, &pid, hash_node);
        assert(hash_node != NULL);
        HistoryNode *hist_node = hash_node->lru_node_.history_;
        HistoryNode *tmp = NULL;
        while (hist_node)
        {
            tmp = hist_node->next_;
            free(hist_node);
            hist_node = tmp;
        }
        HASH_DEL(r->node_store_, hash_node);
        free(hash_node);
        free(node);
        node = next_node;
    }
    free(r);
    return 0;
}

bool Evict(page_id *id, Replacer *r)
{
    if (r == NULL || r->list_ == NULL || r->current_size_ == 0)
        return false;
    KLRUListNode *node_to_evict = r->list_;
    KLRUListNode *prev = NULL;
    while (node_to_evict != NULL)
    {
        ReplacerHashNode *hash_node = NULL;
        HASH_FIND_INT(r->node_store_, &node_to_evict->id_, hash_node);
        if (hash_node->lru_node_.is_evictable_)
        {
            if (prev != NULL)
            {
                prev->next_ = node_to_evict->next_;
            }
            else
            {
                r->list_ = node_to_evict->next_;
            }
            break;
        }
        prev = node_to_evict;
        node_to_evict = node_to_evict->next_;
    }
    *id = node_to_evict->id_;
    free(node_to_evict);
    ReplacerHashNode *hash_node = NULL;
    HASH_FIND_INT(r->node_store_, id, hash_node);
    if (hash_node != NULL)
    {
        HASH_DEL(r->node_store_, hash_node);
        HistoryNode *current_history = hash_node->lru_node_.history_;
        while (current_history != NULL)
        {
            HistoryNode *next_history = current_history->next_;
            free(current_history);
            current_history = next_history;
        }
        free(hash_node);
    }
    r->current_size_--;
    return true;
}

void RecordAccess(page_id id, Replacer *r)
{
    /* Look for the node from node_store_ */

    /* If found, insert a new history to the header of the history list */

    /* If found, remove the KLRUListNode from the list_ */

    /* If not found, insert a new hash_node_ to the node_store_ */

    /* For both cases, count the back-k distance of the page */

    /* Insert the page (pid) to the proper place of list_ */

    if (r == NULL) return;
    KLRUListNode *current = r->list_;
    ReplacerHashNode *update_k = NULL;
    while (current != NULL)
    {
        HASH_FIND_INT(r->node_store_, &current->id_, update_k);
        if (update_k->lru_node_.k_ != INT_FAST32_MAX)
            update_k->lru_node_.k_++;
        current = current->next_;
    }
    ReplacerHashNode *hash_node = NULL;
    HASH_FIND_INT(r->node_store_, &id, hash_node);
    long int back_k = 0;
    int k_history = 1;
    r->current_ts_++; 
    if (hash_node)
    {
        HistoryNode *new_history = calloc(1, sizeof(HistoryNode));
        new_history->ts_ = r->current_ts_;
        new_history->next_ = hash_node->lru_node_.history_;
        hash_node->lru_node_.history_ = new_history;
        KLRUListNode *prev = NULL;
        KLRUListNode *curr = r->list_;
        while (curr != NULL && curr->id_ != id)
        {
            prev = curr;
            curr = curr->next_;
        }
        if (curr != NULL)
        {
            if (prev != NULL)
            {
                prev->next_ = curr->next_;
            }
            else
            {
                r->list_ = curr->next_;
            }
        }
        free(curr);
    }
    else
    {
        hash_node = calloc(1, sizeof(ReplacerHashNode));
        hash_node->id_ = id;
        hash_node->lru_node_.history_ = calloc(1, sizeof(HistoryNode));
        hash_node->lru_node_.history_->ts_ = r->current_ts_;
        hash_node->lru_node_.is_evictable_ = false;
        HASH_ADD_INT(r->node_store_, id_, hash_node);
        SetEvictable(id, true, r);
    }
    HistoryNode *history = hash_node->lru_node_.history_;
    while (history != NULL && k_history < r->k_)
    {
        history = history->next_;
        k_history++;
    }
    if (history == NULL)
    {
        back_k = INT_FAST32_MAX;
    }
    else
    {
        back_k = r->current_ts_ - history->ts_;
    }
    hash_node->lru_node_.k_ = back_k;
    KLRUListNode *new_node = calloc(1, sizeof(KLRUListNode));
    new_node->id_ = id;
    KLRUListNode *prev = NULL;
    KLRUListNode *curr = r->list_;
    ReplacerHashNode *curr_node = NULL;
    int added = 0;
    if (curr == NULL)
    {
        r->list_ = new_node;
        return;
    }
    else
    {
        while (curr != NULL)
        {
            HASH_FIND_INT(r->node_store_, &curr->id_, curr_node);
            if (curr_node->lru_node_.k_ == INT_FAST32_MAX && back_k == INT_FAST32_MAX)
            {
                HistoryNode *history_ts = NULL;
                history_ts = curr_node->lru_node_.history_;
                while (history_ts->next_ != NULL)
                {
                    history_ts = history_ts->next_;
                }
                int curr_ts = history_ts->ts_;
                history_ts = hash_node->lru_node_.history_;
                while (history_ts->next_ != NULL)
                {
                    history_ts = history_ts->next_;
                }
                int new_ts = history_ts->ts_;
                if (new_ts < curr_ts)
                {
                    if (prev == NULL)
                    {
                        new_node->next_ = curr;
                        r->list_ = new_node;
                    }
                    else
                    {
                        prev->next_ = new_node;
                        new_node->next_ = curr;
                    }
                    added = 1;
                    return;
                }
            }
            if (curr_node->lru_node_.k_ < back_k)
            {
                added = 1;
                if (prev == NULL)
                {
                    new_node->next_ = curr;
                    r->list_ = new_node;
                }
                else
                {
                    prev->next_ = new_node;
                    new_node->next_ = curr;
                }
                return;
            }
            prev = curr;
            curr = curr->next_;
        }
        if (!added)
            prev->next_ = new_node;
    }
}

void SetEvictable(page_id id, bool set_evictable, Replacer *r)
{ 
    if (r == NULL){return;}
    ReplacerHashNode *hash_node = NULL;
    HASH_FIND_INT(r->node_store_, &id, hash_node);
    if (hash_node == NULL){return;}
    if (set_evictable && !hash_node->lru_node_.is_evictable_)
    {
        hash_node->lru_node_.is_evictable_ = true;
        r->current_size_++;
    }
    else if (!set_evictable && hash_node->lru_node_.is_evictable_)
    {
        hash_node->lru_node_.is_evictable_ = false;
        r->current_size_--;
    }
}

void Remove(page_id id, Replacer *r)
{
    ReplacerHashNode *hash_node;
    HASH_FIND_INT(r->node_store_, &id, hash_node);
    if (hash_node != NULL && hash_node->lru_node_.is_evictable_)
    {
        HistoryNode *curr = hash_node->lru_node_.history_;
        while (curr != NULL)
        {
            HistoryNode *tmp = curr->next_;
            free(curr);
            curr = tmp;
        }
        hash_node->lru_node_.history_ = NULL;
        hash_node->lru_node_.history_size_ = 0;
        HASH_DEL(r->node_store_, hash_node);
        free(hash_node);
        r->current_size_--;
    }
}

size_t ReplacerSize(Replacer *r)
{
    if (r == NULL)return 0;
    return r->current_size_;
}
