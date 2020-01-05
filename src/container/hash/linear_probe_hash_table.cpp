//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/linear_probe_hash_table.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto header_page_all =  buffer_pool_manager_->NewPage(&header_page_id_);
  header_page_all->WLatch();
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_all->GetData());
  header_page->SetSize(num_buckets);
  header_page->SetPageId(header_page_id_);

  //allocate block page
  for (size_t i = 0; i < num_buckets; i++) {
      page_id_t block_page_id = INVALID_PAGE_ID;
      buffer_pool_manager_->NewPage(&block_page_id);
      if (block_page_id == INVALID_PAGE_ID) { i--; continue; }
      header_page->AddBlockPageId(block_page_id);
      buffer_pool_manager_->UnpinPage(block_page_id, false);
  }
  header_page_all->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto header_page_all =  buffer_pool_manager_->FetchPage(header_page_id_);
  header_page_all->RLatch();
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_all->GetData());

  size_t index, block_ind, bucket_ind;
  GetIndex(key, header_page->NumBlocks(), index, block_ind, bucket_ind);
  
  auto block_page_all = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_ind));
  block_page_all->RLatch();
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_all->GetData());
  
  while (block_page->IsOccupied(bucket_ind)) {
    if (block_page->IsReadable(bucket_ind) && comparator_(block_page->KeyAt(bucket_ind), key) == 0) 
      result->push_back(block_page->ValueAt(bucket_ind));
    bucket_ind++;
    if (block_ind * BLOCK_ARRAY_SIZE + bucket_ind == index) break;
    if (bucket_ind == BLOCK_ARRAY_SIZE) {
      bucket_ind = 0;
      block_page_all->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(block_ind++), false);
      if (block_ind == header_page->NumBlocks()) block_ind = 0;
      block_page_all = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_ind));
      block_page_all->RLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_all->GetData());
      
    }
  }
  block_page_all->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(block_ind), false);
  header_page_all->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_ ,false);
  table_latch_.RUnlock();
  if (result->size() == 0) 
    return false;
  else 
    return true;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto header_page_all =  buffer_pool_manager_->FetchPage(header_page_id_);
  header_page_all->RLatch();
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_all->GetData());

  size_t index, block_ind, bucket_ind;
  GetIndex(key, header_page->NumBlocks(), index, block_ind, bucket_ind);
 
  auto block_page_all = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_ind));
  block_page_all->WLatch();
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_all->GetData());

  while (!block_page->Insert(bucket_ind, key, value)) {
    if (!comparator_(block_page->KeyAt(bucket_ind), key) && block_page->ValueAt(bucket_ind) == value) {
      block_page_all->WUnlatch();
      buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(block_ind), true);
      header_page_all->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, false);
      return false;
    }
    bucket_ind++;
    //Resize hash table
    if (block_ind * BLOCK_ARRAY_SIZE + bucket_ind == index) {
      block_page_all->WUnlatch();
      buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(block_ind), false);
      header_page_all->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, false);
      Resize(header_page->NumBlocks() * BLOCK_ARRAY_SIZE);

      header_page_all =  buffer_pool_manager_->FetchPage(header_page_id_);
      header_page_all->RLatch();
      header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_all->GetData());
      GetIndex(key, header_page->NumBlocks(), index, block_ind, bucket_ind);

      block_page_all = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_ind));
      block_page_all->WLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_all->GetData());
      continue;
    }
    if (bucket_ind == BLOCK_ARRAY_SIZE) {
      bucket_ind = 0;
      block_page_all->WUnlatch();
      buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(block_ind++), false);
      if (block_ind == header_page->NumBlocks()) block_ind = 0;
      block_page_all = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_ind));
      block_page_all->WLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_all->GetData());
    }
  }
  block_page_all->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(block_ind), true);
  header_page_all->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto header_page_all =  buffer_pool_manager_->FetchPage(header_page_id_);
  header_page_all->RLatch();
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_all->GetData());

  size_t index, block_ind, bucket_ind;
  GetIndex(key, header_page->NumBlocks(), index, block_ind, bucket_ind);
  
  auto block_page_all = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_ind));
  block_page_all->WLatch();
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_all->GetData());
  
  while (block_page->IsOccupied(bucket_ind)) {
    if (!comparator_(block_page->KeyAt(bucket_ind), key) && block_page->ValueAt(bucket_ind) == value) {
      // this is a trick
      if (!block_page->IsReadable(bucket_ind)) {
        block_page_all->WUnlatch();
        buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(block_ind), true);
        header_page_all->RUnlatch();
        buffer_pool_manager_->UnpinPage(header_page_id_, false);
        return false;
      }
      block_page->Remove(bucket_ind);
      block_page_all->WUnlatch();
      buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(block_ind), true);
      header_page_all->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, false);
      return true;
    }
    bucket_ind++;
    if (block_ind * BLOCK_ARRAY_SIZE + bucket_ind == index) break;
    if (bucket_ind == BLOCK_ARRAY_SIZE) {
      bucket_ind = 0;
      block_page_all->WUnlatch();
      buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(block_ind++), false);
      if (block_ind == header_page->NumBlocks()) block_ind = 0;
      block_page_all = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_ind));
      block_page_all->WLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_all->GetData());
    }
  }
  block_page_all->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(block_ind), true);
  header_page_all->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  size_t num_buckets = 2 * initial_size / BLOCK_ARRAY_SIZE;
  page_id_t del_header_page_id = header_page_id_;
  auto header_page_all =  buffer_pool_manager_->NewPage(&header_page_id_);
  header_page_all->WLatch();
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_all->GetData());
  header_page->SetSize(num_buckets);
  header_page->SetPageId(header_page_id_);
  // allocate block page
  for (size_t i = 0; i < num_buckets; i++) {
      page_id_t block_page_id = INVALID_PAGE_ID;
      buffer_pool_manager_->NewPage(&block_page_id);
      if (block_page_id == INVALID_PAGE_ID) { i--; continue; }
      header_page->AddBlockPageId(block_page_id);
      buffer_pool_manager_->UnpinPage(block_page_id, false);
  }
  // move key-value
  auto del_header_page_all =  buffer_pool_manager_->FetchPage(header_page_id_);
  del_header_page_all->RLatch();
  auto del_header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_all->GetData());
  for (size_t block_ind = 0; block_ind < del_header_page->NumBlocks(); block_ind++) {
    page_id_t del_block_page_id = del_header_page->GetBlockPageId(block_ind);    
    auto del_block_page_all = buffer_pool_manager_->FetchPage(del_block_page_id);
    del_block_page_all->RLatch();
    auto del_block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(del_block_page_all->GetData());

    for (size_t bucket_ind = 0; bucket_ind < BLOCK_ARRAY_SIZE; bucket_ind++) {
      if (del_block_page->IsReadable(bucket_ind)) {
        KeyType key = del_block_page->KeyAt(bucket_ind);
        ValueType value = del_block_page->ValueAt(bucket_ind);
        Insert(nullptr, key, value);
      }
    }
    del_block_page_all->RUnlatch();
    buffer_pool_manager_->UnpinPage(del_block_page_id, false);
    buffer_pool_manager_->DeletePage(del_block_page_id);
  }
  del_header_page_all->RUnlatch();
  buffer_pool_manager_->UnpinPage(del_header_page_id ,false);
  buffer_pool_manager_->DeletePage(del_header_page_id);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  auto header_page_all =  buffer_pool_manager_->FetchPage(header_page_id_);
  header_page_all->RLatch();
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header_page_all->GetData());
  size_t num_buckets = header_page->NumBlocks();
  header_page_all->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return BLOCK_ARRAY_SIZE * num_buckets;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::GetIndex(const KeyType &key, const size_t &num_buckets, size_t &index,size_t &block_ind, size_t &bucket_ind) {
  index = hash_fn_.GetHash(key) % (BLOCK_ARRAY_SIZE * num_buckets);
  block_ind = index / BLOCK_ARRAY_SIZE;
  bucket_ind = index % BLOCK_ARRAY_SIZE;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;


}  // namespace bustub
