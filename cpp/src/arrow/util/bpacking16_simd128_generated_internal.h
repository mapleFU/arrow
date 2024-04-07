// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Automatically generated file; DO NOT EDIT.

#pragma once

#include <cstdint>
#include <cstring>

#include <xsimd/xsimd.hpp>

#include "arrow/util/dispatch.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace internal {
namespace {

using ::arrow::util::SafeLoad;

template <DispatchLevel level>
struct Unpack16Bits128 {

using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;

inline static const uint16_t* unpack0_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  memset(out, 0x0, 16 * sizeof(*out));
  out += 16;

  return in;
}

inline static const uint16_t* unpack1_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x1;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 1-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0) };
  shifts = simd_batch{ 0, 1, 2, 3, 4, 5, 6, 7 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 1-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0) };
  shifts = simd_batch{ 8, 9, 10, 11, 12, 13, 14, 15 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 1;
  return in;
}

inline static const uint16_t* unpack2_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x3;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 2-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0) };
  shifts = simd_batch{ 0, 2, 4, 6, 8, 10, 12, 14 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 2-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1) };
  shifts = simd_batch{ 0, 2, 4, 6, 8, 10, 12, 14 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 2;
  return in;
}

inline static const uint16_t* unpack3_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x7;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 3-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 15 | SafeLoad<uint16_t>(in + 1) << 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1) };
  shifts = simd_batch{ 0, 3, 6, 9, 12, 0, 2, 5 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 3-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 14 | SafeLoad<uint16_t>(in + 2) << 2), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 2) };
  shifts = simd_batch{ 8, 11, 0, 1, 4, 7, 10, 13 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 3;
  return in;
}

inline static const uint16_t* unpack4_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0xf;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 4-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1) };
  shifts = simd_batch{ 0, 4, 8, 12, 0, 4, 8, 12 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 4-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 3), SafeLoad<uint16_t>(in + 3), SafeLoad<uint16_t>(in + 3), SafeLoad<uint16_t>(in + 3) };
  shifts = simd_batch{ 0, 4, 8, 12, 0, 4, 8, 12 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 4;
  return in;
}

inline static const uint16_t* unpack5_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x1f;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 5-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 15 | SafeLoad<uint16_t>(in + 1) << 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 14 | SafeLoad<uint16_t>(in + 2) << 2), SafeLoad<uint16_t>(in + 2) };
  shifts = simd_batch{ 0, 5, 10, 0, 4, 9, 0, 3 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 5-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 2) >> 13 | SafeLoad<uint16_t>(in + 3) << 3), SafeLoad<uint16_t>(in + 3), SafeLoad<uint16_t>(in + 3), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 3) >> 12 | SafeLoad<uint16_t>(in + 4) << 4), SafeLoad<uint16_t>(in + 4), SafeLoad<uint16_t>(in + 4), SafeLoad<uint16_t>(in + 4) };
  shifts = simd_batch{ 8, 0, 2, 7, 0, 1, 6, 11 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 5;
  return in;
}

inline static const uint16_t* unpack6_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x3f;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 6-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 12 | SafeLoad<uint16_t>(in + 1) << 4), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 14 | SafeLoad<uint16_t>(in + 2) << 2), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 2) };
  shifts = simd_batch{ 0, 6, 0, 2, 8, 0, 4, 10 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 6-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 3), SafeLoad<uint16_t>(in + 3), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 3) >> 12 | SafeLoad<uint16_t>(in + 4) << 4), SafeLoad<uint16_t>(in + 4), SafeLoad<uint16_t>(in + 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 4) >> 14 | SafeLoad<uint16_t>(in + 5) << 2), SafeLoad<uint16_t>(in + 5), SafeLoad<uint16_t>(in + 5) };
  shifts = simd_batch{ 0, 6, 0, 2, 8, 0, 4, 10 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 6;
  return in;
}

inline static const uint16_t* unpack7_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x7f;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 7-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 14 | SafeLoad<uint16_t>(in + 1) << 2), SafeLoad<uint16_t>(in + 1), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 12 | SafeLoad<uint16_t>(in + 2) << 4), SafeLoad<uint16_t>(in + 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 2) >> 10 | SafeLoad<uint16_t>(in + 3) << 6), SafeLoad<uint16_t>(in + 3) };
  shifts = simd_batch{ 0, 7, 0, 5, 0, 3, 0, 1 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 7-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 3), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 3) >> 15 | SafeLoad<uint16_t>(in + 4) << 1), SafeLoad<uint16_t>(in + 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 4) >> 13 | SafeLoad<uint16_t>(in + 5) << 3), SafeLoad<uint16_t>(in + 5), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 5) >> 11 | SafeLoad<uint16_t>(in + 6) << 5), SafeLoad<uint16_t>(in + 6), SafeLoad<uint16_t>(in + 6) };
  shifts = simd_batch{ 8, 0, 6, 0, 4, 0, 2, 9 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 7;
  return in;
}

inline static const uint16_t* unpack8_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0xff;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 8-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 0), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 1), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 3), SafeLoad<uint16_t>(in + 3) };
  shifts = simd_batch{ 0, 8, 0, 8, 0, 8, 0, 8 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 8-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 4), SafeLoad<uint16_t>(in + 4), SafeLoad<uint16_t>(in + 5), SafeLoad<uint16_t>(in + 5), SafeLoad<uint16_t>(in + 6), SafeLoad<uint16_t>(in + 6), SafeLoad<uint16_t>(in + 7), SafeLoad<uint16_t>(in + 7) };
  shifts = simd_batch{ 0, 8, 0, 8, 0, 8, 0, 8 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 8;
  return in;
}

inline static const uint16_t* unpack9_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x1ff;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 9-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 9 | SafeLoad<uint16_t>(in + 1) << 7), SafeLoad<uint16_t>(in + 1), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 11 | SafeLoad<uint16_t>(in + 2) << 5), SafeLoad<uint16_t>(in + 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 2) >> 13 | SafeLoad<uint16_t>(in + 3) << 3), SafeLoad<uint16_t>(in + 3), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 3) >> 15 | SafeLoad<uint16_t>(in + 4) << 1) };
  shifts = simd_batch{ 0, 0, 2, 0, 4, 0, 6, 0 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 9-bit bundles 8 to 15
  words = simd_batch{ static_cast<uint16_t>(SafeLoad<uint16_t>(in + 4) >> 8 | SafeLoad<uint16_t>(in + 5) << 8), SafeLoad<uint16_t>(in + 5), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 5) >> 10 | SafeLoad<uint16_t>(in + 6) << 6), SafeLoad<uint16_t>(in + 6), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 6) >> 12 | SafeLoad<uint16_t>(in + 7) << 4), SafeLoad<uint16_t>(in + 7), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 7) >> 14 | SafeLoad<uint16_t>(in + 8) << 2), SafeLoad<uint16_t>(in + 8) };
  shifts = simd_batch{ 0, 1, 0, 3, 0, 5, 0, 7 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 9;
  return in;
}

inline static const uint16_t* unpack10_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x3ff;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 10-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 10 | SafeLoad<uint16_t>(in + 1) << 6), SafeLoad<uint16_t>(in + 1), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 14 | SafeLoad<uint16_t>(in + 2) << 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 2) >> 8 | SafeLoad<uint16_t>(in + 3) << 8), SafeLoad<uint16_t>(in + 3), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 3) >> 12 | SafeLoad<uint16_t>(in + 4) << 4), SafeLoad<uint16_t>(in + 4) };
  shifts = simd_batch{ 0, 0, 4, 0, 0, 2, 0, 6 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 10-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 5), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 5) >> 10 | SafeLoad<uint16_t>(in + 6) << 6), SafeLoad<uint16_t>(in + 6), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 6) >> 14 | SafeLoad<uint16_t>(in + 7) << 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 7) >> 8 | SafeLoad<uint16_t>(in + 8) << 8), SafeLoad<uint16_t>(in + 8), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 8) >> 12 | SafeLoad<uint16_t>(in + 9) << 4), SafeLoad<uint16_t>(in + 9) };
  shifts = simd_batch{ 0, 0, 4, 0, 0, 2, 0, 6 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 10;
  return in;
}

inline static const uint16_t* unpack11_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x7ff;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 11-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 11 | SafeLoad<uint16_t>(in + 1) << 5), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 6 | SafeLoad<uint16_t>(in + 2) << 10), SafeLoad<uint16_t>(in + 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 2) >> 12 | SafeLoad<uint16_t>(in + 3) << 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 3) >> 7 | SafeLoad<uint16_t>(in + 4) << 9), SafeLoad<uint16_t>(in + 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 4) >> 13 | SafeLoad<uint16_t>(in + 5) << 3) };
  shifts = simd_batch{ 0, 0, 0, 1, 0, 0, 2, 0 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 11-bit bundles 8 to 15
  words = simd_batch{ static_cast<uint16_t>(SafeLoad<uint16_t>(in + 5) >> 8 | SafeLoad<uint16_t>(in + 6) << 8), SafeLoad<uint16_t>(in + 6), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 6) >> 14 | SafeLoad<uint16_t>(in + 7) << 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 7) >> 9 | SafeLoad<uint16_t>(in + 8) << 7), SafeLoad<uint16_t>(in + 8), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 8) >> 15 | SafeLoad<uint16_t>(in + 9) << 1), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 9) >> 10 | SafeLoad<uint16_t>(in + 10) << 6), SafeLoad<uint16_t>(in + 10) };
  shifts = simd_batch{ 0, 3, 0, 0, 4, 0, 0, 5 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 11;
  return in;
}

inline static const uint16_t* unpack12_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0xfff;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 12-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 12 | SafeLoad<uint16_t>(in + 1) << 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 8 | SafeLoad<uint16_t>(in + 2) << 8), SafeLoad<uint16_t>(in + 2), SafeLoad<uint16_t>(in + 3), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 3) >> 12 | SafeLoad<uint16_t>(in + 4) << 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 4) >> 8 | SafeLoad<uint16_t>(in + 5) << 8), SafeLoad<uint16_t>(in + 5) };
  shifts = simd_batch{ 0, 0, 0, 4, 0, 0, 0, 4 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 12-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 6), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 6) >> 12 | SafeLoad<uint16_t>(in + 7) << 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 7) >> 8 | SafeLoad<uint16_t>(in + 8) << 8), SafeLoad<uint16_t>(in + 8), SafeLoad<uint16_t>(in + 9), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 9) >> 12 | SafeLoad<uint16_t>(in + 10) << 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 10) >> 8 | SafeLoad<uint16_t>(in + 11) << 8), SafeLoad<uint16_t>(in + 11) };
  shifts = simd_batch{ 0, 0, 0, 4, 0, 0, 0, 4 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 12;
  return in;
}

inline static const uint16_t* unpack13_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x1fff;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 13-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 13 | SafeLoad<uint16_t>(in + 1) << 3), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 10 | SafeLoad<uint16_t>(in + 2) << 6), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 2) >> 7 | SafeLoad<uint16_t>(in + 3) << 9), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 3) >> 4 | SafeLoad<uint16_t>(in + 4) << 12), SafeLoad<uint16_t>(in + 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 4) >> 14 | SafeLoad<uint16_t>(in + 5) << 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 5) >> 11 | SafeLoad<uint16_t>(in + 6) << 5) };
  shifts = simd_batch{ 0, 0, 0, 0, 0, 1, 0, 0 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 13-bit bundles 8 to 15
  words = simd_batch{ static_cast<uint16_t>(SafeLoad<uint16_t>(in + 6) >> 8 | SafeLoad<uint16_t>(in + 7) << 8), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 7) >> 5 | SafeLoad<uint16_t>(in + 8) << 11), SafeLoad<uint16_t>(in + 8), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 8) >> 15 | SafeLoad<uint16_t>(in + 9) << 1), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 9) >> 12 | SafeLoad<uint16_t>(in + 10) << 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 10) >> 9 | SafeLoad<uint16_t>(in + 11) << 7), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 11) >> 6 | SafeLoad<uint16_t>(in + 12) << 10), SafeLoad<uint16_t>(in + 12) };
  shifts = simd_batch{ 0, 0, 2, 0, 0, 0, 0, 3 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 13;
  return in;
}

inline static const uint16_t* unpack14_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x3fff;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 14-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 14 | SafeLoad<uint16_t>(in + 1) << 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 12 | SafeLoad<uint16_t>(in + 2) << 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 2) >> 10 | SafeLoad<uint16_t>(in + 3) << 6), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 3) >> 8 | SafeLoad<uint16_t>(in + 4) << 8), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 4) >> 6 | SafeLoad<uint16_t>(in + 5) << 10), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 5) >> 4 | SafeLoad<uint16_t>(in + 6) << 12), SafeLoad<uint16_t>(in + 6) };
  shifts = simd_batch{ 0, 0, 0, 0, 0, 0, 0, 2 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 14-bit bundles 8 to 15
  words = simd_batch{ SafeLoad<uint16_t>(in + 7), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 7) >> 14 | SafeLoad<uint16_t>(in + 8) << 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 8) >> 12 | SafeLoad<uint16_t>(in + 9) << 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 9) >> 10 | SafeLoad<uint16_t>(in + 10) << 6), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 10) >> 8 | SafeLoad<uint16_t>(in + 11) << 8), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 11) >> 6 | SafeLoad<uint16_t>(in + 12) << 10), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 12) >> 4 | SafeLoad<uint16_t>(in + 13) << 12), SafeLoad<uint16_t>(in + 13) };
  shifts = simd_batch{ 0, 0, 0, 0, 0, 0, 0, 2 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 14;
  return in;
}

inline static const uint16_t* unpack15_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  uint16_t mask = 0x7fff;

  simd_batch masks(mask);
  simd_batch words, shifts;
  simd_batch results;

  // extract 15-bit bundles 0 to 7
  words = simd_batch{ SafeLoad<uint16_t>(in + 0), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 0) >> 15 | SafeLoad<uint16_t>(in + 1) << 1), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 1) >> 14 | SafeLoad<uint16_t>(in + 2) << 2), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 2) >> 13 | SafeLoad<uint16_t>(in + 3) << 3), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 3) >> 12 | SafeLoad<uint16_t>(in + 4) << 4), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 4) >> 11 | SafeLoad<uint16_t>(in + 5) << 5), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 5) >> 10 | SafeLoad<uint16_t>(in + 6) << 6), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 6) >> 9 | SafeLoad<uint16_t>(in + 7) << 7) };
  shifts = simd_batch{ 0, 0, 0, 0, 0, 0, 0, 0 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  // extract 15-bit bundles 8 to 15
  words = simd_batch{ static_cast<uint16_t>(SafeLoad<uint16_t>(in + 7) >> 8 | SafeLoad<uint16_t>(in + 8) << 8), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 8) >> 7 | SafeLoad<uint16_t>(in + 9) << 9), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 9) >> 6 | SafeLoad<uint16_t>(in + 10) << 10), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 10) >> 5 | SafeLoad<uint16_t>(in + 11) << 11), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 11) >> 4 | SafeLoad<uint16_t>(in + 12) << 12), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 12) >> 3 | SafeLoad<uint16_t>(in + 13) << 13), static_cast<uint16_t>(SafeLoad<uint16_t>(in + 13) >> 2 | SafeLoad<uint16_t>(in + 14) << 14), SafeLoad<uint16_t>(in + 14) };
  shifts = simd_batch{ 0, 0, 0, 0, 0, 0, 0, 1 };
  results = (words >> shifts) & masks;
  results.store_unaligned(out);
  out += 8;

  in += 15;
  return in;
}

inline static const uint16_t* unpack16_16(const uint16_t* __restrict__ in, uint16_t* __restrict__ out) {
  memcpy(out, in, 16 * sizeof(*out));
  in += 16;
  out += 16;

  return in;
}

};  // struct Unpack16Bits128

}  // namespace
}  // namespace internal
}  // namespace arrow

