//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  ClockItem clock_item = {true, false};
  for (size_t i = 0; i != num_pages; ++i) {
    clock_replacer.emplace_back(clock_item);
  }
  clock_hand = 0;
  clock_size = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock clock_clk{clock_mutex};
  while (clock_size > 0) {
    if (clock_hand == clock_replacer.size()) {
      clock_hand = 0;
    }

    if (clock_replacer[clock_hand].isPin) {
      clock_hand++;
    } else if (clock_replacer[clock_hand].ref == true) {
      clock_replacer[clock_hand].ref = false;
      clock_hand++;
    } else {
      clock_replacer[clock_hand].isPin = true;
      clock_size--;
      *frame_id = clock_hand++;
      return true;
    }
  }

  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock clock_clk{clock_mutex};
  if (clock_replacer[frame_id].isPin == false) {
    clock_replacer[frame_id].isPin = true;
    clock_size--;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock clock_clk{clock_mutex};
  if (clock_replacer[frame_id].isPin == true) {
    clock_replacer[frame_id].isPin = false;
    clock_replacer[frame_id].ref = true;
    clock_size++;
  }
}

size_t ClockReplacer::Size() {
  std::scoped_lock clock_clk{clock_mutex};
  return clock_size;
}

}  // namespace bustub
