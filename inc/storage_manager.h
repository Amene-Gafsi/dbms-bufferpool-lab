#pragma once
#include "page.h"
#include "stdio.h"

typedef struct StorageManager_t{
    char *filename_;
    FILE *file_ptr_;
} StorageManager;

int InitStorageManager(const char* filename, StorageManager **storage_manager);
int StopStorageManager(StorageManager *storage_manager);
int WritePage(block_id id, const char* page_data, StorageManager *storage_manager);
int ReadPage(block_id id, char* page_data, StorageManager *storage_manager);
