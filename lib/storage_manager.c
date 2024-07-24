#include "storage_manager.h"
#include "bp_error.h"

#include "stdlib.h"
#include <string.h>
#include <assert.h>

int InitStorageManager(const char* filename, StorageManager **storage_manager){
    // Don't initialize it twice
    if (*storage_manager) return -ESTORAGEMANAGER;
    *storage_manager = malloc(sizeof(StorageManager));
    FILE* fptr = NULL;
    fptr = fopen(filename, "r+");

    if (fptr == NULL){
        fptr = fopen(filename, "w+");
        if (fptr == NULL) return -ESTORAGEMANAGER;
    }

    (*storage_manager)->file_ptr_ = fptr;
    (*storage_manager)->filename_ = malloc(strlen(filename) + 1);
    strcpy((*storage_manager)->filename_, filename); 
    return 0;
}

int StopStorageManager(StorageManager *storage_manager){
    if (storage_manager){
        int ret = fclose(storage_manager->file_ptr_);
        free(storage_manager->filename_);
        free(storage_manager);
        return ret;
    }
    return -ESTORAGEMANAGER;
}

int WritePage(block_id id, const char* page_data, StorageManager *storage_manager){

    if (storage_manager == NULL)
        return -ESTORAGEMANAGER;
    if (id < 0 || page_data == NULL)
        return -ESTORAGEWRITE;
    long offset = id * PAGE_SIZE;
    if (fseek(storage_manager->file_ptr_, offset, SEEK_SET) != 0)
        return -ESTORAGEWRITE;
    size_t bytes_written = fwrite(page_data, sizeof(char), PAGE_SIZE, storage_manager->file_ptr_);
    if (bytes_written != PAGE_SIZE)
        return -ESTORAGEWRITE; 
    return 0;
}

int ReadPage(block_id id, char* page_data, StorageManager *storage_manager){
    if (storage_manager == NULL)
        return -ESTORAGEMANAGER;
    if (id < 0 || page_data == NULL)
        return -ESTORAGEREAD;
    long offset = id * PAGE_SIZE;
    if (fseek(storage_manager->file_ptr_, offset, SEEK_SET) != 0)
        return -ESTORAGEREAD;
    size_t bytes_read = fread(page_data, sizeof(char), PAGE_SIZE, storage_manager->file_ptr_);
    if (bytes_read < PAGE_SIZE) {
        if (ftell(storage_manager->file_ptr_) < offset || bytes_read == 0) {
            return -ESTORAGEREAD;
        }
        memset(page_data + bytes_read, 0, PAGE_SIZE - bytes_read);
    }
    return 0;
}
