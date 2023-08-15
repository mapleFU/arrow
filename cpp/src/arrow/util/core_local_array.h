#ifndef ARROW_CORE_LOCAL_ARRAY_H
#define ARROW_CORE_LOCAL_ARRAY_H

//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstddef>
#include <thread>
#include <utility>
#include <vector>

#include "type_fwd.h"
#include "macros.h"

namespace arrow::util {

inline int PhysicalCoreID() {
#if defined(__x86_64__) && \
    (__GNUC__ > 2 || (__GNUC__ == 2 && __GNUC_MINOR__ >= 22))
  // sched_getcpu uses VDSO getcpu() syscall since 2.22. I believe Linux offers
  // VDSO support only on x86_64. This is the fastest/preferred method if
  // available.
  int cpuno = sched_getcpu();
  if (cpuno < 0) {
    return -1;
  }
  return cpuno;
#elif defined(__x86_64__) || defined(__i386__)
  // clang/gcc both provide cpuid.h, which defines __get_cpuid(), for x86_64 and
  // i386.
  unsigned eax, ebx = 0, ecx, edx;
  if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
    return -1;
  }
  return ebx >> 24;
#else
  // give up, the caller can generate a random number or something.
  return -1;
#endif
}

// An array of core-local values. Ideally the value type, T, is cache aligned to
// prevent false sharing.
template <typename T>
class CoreLocalArray {
 public:
  CoreLocalArray();

  size_t Size() const;
  // returns pointer to the element corresponding to the core that the thread
  // currently runs on.
  T* Access() const;
  // same as above, but also returns the core index, which the client can cache
  // to reduce how often core ID needs to be retrieved. Only do this if some
  // inaccuracy is tolerable, as the thread may migrate to a different core.
  std::pair<T*, size_t> AccessElementAndIndex() const;
  // returns pointer to element for the specified core index. This can be used,
  // e.g., for aggregation, or if the client caches core index.
  T* AccessAtCore(size_t core_idx) const;

 private:
  std::unique_ptr<T[]> data_;
  int size_shift_;
};

template <typename T>
CoreLocalArray<T>::CoreLocalArray() {
  int num_cpus = static_cast<int>(std::thread::hardware_concurrency());
  // find a power of two >= num_cpus and >= 8
  size_shift_ = 3;
  while (1 << size_shift_ < num_cpus) {
    ++size_shift_;
  }
  data_.reset(new T[static_cast<size_t>(1) << size_shift_]);
}

template <typename T>
size_t CoreLocalArray<T>::Size() const {
  return static_cast<size_t>(1) << size_shift_;
}

template <typename T>
T* CoreLocalArray<T>::Access() const {
  return AccessElementAndIndex().first;
}

template <typename T>
std::pair<T*, size_t> CoreLocalArray<T>::AccessElementAndIndex() const {
  int cpuid = PhysicalCoreID();
  size_t core_idx = static_cast<size_t>(cpuid & ((1 << size_shift_) - 1));
  return {AccessAtCore(core_idx), core_idx};
}

template <typename T>
T* CoreLocalArray<T>::AccessAtCore(size_t core_idx) const {
  assert(core_idx < static_cast<size_t>(1) << size_shift_);
  return &data_[core_idx];
}

}  // namespace ROCKSDB_NAMESPACE


#endif  // ARROW_CORE_LOCAL_ARRAY_H
