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

#include "arrow/util/compression_internal.h"
#include "arrow/util/core_local_array.h"

#include <cstddef>
#include <cstdint>
#include <memory>

#include <zstd.h>
#include <cassert>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

using std::size_t;

namespace arrow {
namespace util {
namespace internal {

namespace {

Status ZSTDError(size_t ret, const char* prefix_msg) {
  return Status::IOError(prefix_msg, ZSTD_getErrorName(ret));
}


class ZSTDUncompressCachedData;

class CompressionContextCache {
 public:
  // Singleton
  static CompressionContextCache* Instance();
  static void InitSingleton();
  CompressionContextCache(const CompressionContextCache&) = delete;
  CompressionContextCache& operator=(const CompressionContextCache&) = delete;

  ZSTDUncompressCachedData GetCachedZSTDUncompressData();
  void ReturnCachedZSTDUncompressData(int64_t idx);

 private:
  // Singleton
  CompressionContextCache();
  ~CompressionContextCache();

  class Rep;
  Rep* rep_;
};

// Cached data represents a portion that can be re-used
// If, in the future we have more than one native context to
// cache we can arrange this as a tuple
class ZSTDUncompressCachedData {
 public:
  using ZSTDNativeContext = ZSTD_DCtx*;
  ZSTDUncompressCachedData() {}
  // Init from cache
  ZSTDUncompressCachedData(const ZSTDUncompressCachedData& o) = delete;
  ZSTDUncompressCachedData& operator=(const ZSTDUncompressCachedData&) = delete;
  ZSTDUncompressCachedData(ZSTDUncompressCachedData&& o) noexcept
      : ZSTDUncompressCachedData() {
    *this = std::move(o);
  }
  ZSTDUncompressCachedData& operator=(ZSTDUncompressCachedData&& o) noexcept {
    assert(zstd_ctx_ == nullptr);
    std::swap(zstd_ctx_, o.zstd_ctx_);
    std::swap(cache_idx_, o.cache_idx_);
    return *this;
  }
  ZSTDNativeContext Get() const { return zstd_ctx_; }
  int64_t GetCacheIndex() const { return cache_idx_; }
  void CreateIfNeeded() {
    if (zstd_ctx_ == nullptr) {
#ifdef ROCKSDB_ZSTD_CUSTOM_MEM
      zstd_ctx_ =
          ZSTD_createDCtx_advanced(port::GetJeZstdAllocationOverrides());
#else   // ROCKSDB_ZSTD_CUSTOM_MEM
      zstd_ctx_ = ZSTD_createDCtx();
#endif  // ROCKSDB_ZSTD_CUSTOM_MEM
      cache_idx_ = -1;
    }
  }
  void InitFromCache(const ZSTDUncompressCachedData& o, int64_t idx) {
    zstd_ctx_ = o.zstd_ctx_;
    cache_idx_ = idx;
  }
  ~ZSTDUncompressCachedData() {
    if (zstd_ctx_ != nullptr && cache_idx_ == -1) {
      ZSTD_freeDCtx(zstd_ctx_);
    }
  }

 private:
  ZSTDNativeContext zstd_ctx_ = nullptr;
  int64_t cache_idx_ = -1;  // -1 means this instance owns the context
};

class UncompressionContext {
 private:
  CompressionContextCache* ctx_cache_ = nullptr;
  ZSTDUncompressCachedData uncomp_cached_data_;

 public:
  explicit UncompressionContext() {
    ctx_cache_ = CompressionContextCache::Instance();
    uncomp_cached_data_ = ctx_cache_->GetCachedZSTDUncompressData();
  }
  ~UncompressionContext() {
    if (uncomp_cached_data_.GetCacheIndex() != -1) {
      assert(ctx_cache_ != nullptr);
      ctx_cache_->ReturnCachedZSTDUncompressData(
          uncomp_cached_data_.GetCacheIndex());
    }
  }
  UncompressionContext(const UncompressionContext&) = delete;
  UncompressionContext& operator=(const UncompressionContext&) = delete;

  ZSTDUncompressCachedData::ZSTDNativeContext GetZSTDContext() const {
    return uncomp_cached_data_.Get();
  }
};

void* const SentinelValue = nullptr;

constexpr size_t CACHE_LINE_SIZE = 64;

// Cache ZSTD uncompression contexts for reads
// if needed we can add ZSTD compression context caching
// which is currently is not done since BlockBasedTableBuilder
// simply creates one compression context per new SST file.
struct ZSTDCachedData {
  // We choose to cache the below structure instead of a ptr
  // because we want to avoid a) native types leak b) make
  // cache use transparent for the user
  ZSTDUncompressCachedData uncomp_cached_data_;
  std::atomic<void*> zstd_uncomp_sentinel_;

  char
      padding[(CACHE_LINE_SIZE -
               (sizeof(ZSTDUncompressCachedData) + sizeof(std::atomic<void*>)) %
                   CACHE_LINE_SIZE)];  // unused padding field

  ZSTDCachedData() : zstd_uncomp_sentinel_(&uncomp_cached_data_) {}
  ZSTDCachedData(const ZSTDCachedData&) = delete;
  ZSTDCachedData& operator=(const ZSTDCachedData&) = delete;

  ZSTDUncompressCachedData GetUncompressData(int64_t idx) {
    ZSTDUncompressCachedData result;
    void* expected = &uncomp_cached_data_;
    if (zstd_uncomp_sentinel_.compare_exchange_strong(expected,
                                                      SentinelValue)) {
      uncomp_cached_data_.CreateIfNeeded();
      result.InitFromCache(uncomp_cached_data_, idx);
    } else {
      // Creates one time use data
      result.CreateIfNeeded();
    }
    return result;
  }
  // Return the entry back into circulation
  // This is executed only when we successfully obtained
  // in the first place
  void ReturnUncompressData() {
    if (zstd_uncomp_sentinel_.exchange(&uncomp_cached_data_) != SentinelValue) {
      // Means we are returning while not having it acquired.
      assert(false);
    }
  }
};
static_assert(sizeof(ZSTDCachedData) % CACHE_LINE_SIZE == 0,
              "Expected CACHE_LINE_SIZE alignment");

class CompressionContextCache::Rep {
 public:
  Rep() {}
  ZSTDUncompressCachedData GetZSTDUncompressData() {
    auto p = per_core_uncompr_.AccessElementAndIndex();
    int64_t idx = static_cast<int64_t>(p.second);
    return p.first->GetUncompressData(idx);
  }
  void ReturnZSTDUncompressData(int64_t idx) {
    assert(idx >= 0);
    auto* cn = per_core_uncompr_.AccessAtCore(static_cast<size_t>(idx));
    cn->ReturnUncompressData();
  }

 private:
  CoreLocalArray<ZSTDCachedData> per_core_uncompr_;
};

CompressionContextCache::CompressionContextCache() : rep_(new Rep()) {}

CompressionContextCache* CompressionContextCache::Instance() {
  static CompressionContextCache instance;
  return &instance;
}

void CompressionContextCache::InitSingleton() { Instance(); }

ZSTDUncompressCachedData
CompressionContextCache::GetCachedZSTDUncompressData() {
  return rep_->GetZSTDUncompressData();
}

void CompressionContextCache::ReturnCachedZSTDUncompressData(int64_t idx) {
  rep_->ReturnZSTDUncompressData(idx);
}

CompressionContextCache::~CompressionContextCache() { delete rep_; }

// ----------------------------------------------------------------------
// ZSTD decompressor implementation

class ZSTDDecompressor : public Decompressor {
 public:
  ZSTDDecompressor() : stream_(ZSTD_createDStream()) {}

  ~ZSTDDecompressor() override { ZSTD_freeDStream(stream_); }

  Status Init() {
    finished_ = false;
    size_t ret = ZSTD_initDStream(stream_);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD init failed: ");
    } else {
      return Status::OK();
    }
  }

  Result<DecompressResult> Decompress(int64_t input_len, const uint8_t* input,
                                      int64_t output_len, uint8_t* output) override {
    ZSTD_inBuffer in_buf;
    ZSTD_outBuffer out_buf;

    in_buf.src = input;
    in_buf.size = static_cast<size_t>(input_len);
    in_buf.pos = 0;
    out_buf.dst = output;
    out_buf.size = static_cast<size_t>(output_len);
    out_buf.pos = 0;

    size_t ret;
    ret = ZSTD_decompressStream(stream_, &out_buf, &in_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD decompress failed: ");
    }
    finished_ = (ret == 0);
    return DecompressResult{static_cast<int64_t>(in_buf.pos),
                            static_cast<int64_t>(out_buf.pos),
                            in_buf.pos == 0 && out_buf.pos == 0};
  }

  Status Reset() override { return Init(); }

  bool IsFinished() override { return finished_; }

 protected:
  ZSTD_DStream* stream_;
  bool finished_;
};

// ----------------------------------------------------------------------
// ZSTD compressor implementation

class ZSTDCompressor : public Compressor {
 public:
  explicit ZSTDCompressor(int compression_level)
      : stream_(ZSTD_createCStream()), compression_level_(compression_level) {}

  ~ZSTDCompressor() override { ZSTD_freeCStream(stream_); }

  Status Init() {
    size_t ret = ZSTD_initCStream(stream_, compression_level_);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD init failed: ");
    } else {
      return Status::OK();
    }
  }

  Result<CompressResult> Compress(int64_t input_len, const uint8_t* input,
                                  int64_t output_len, uint8_t* output) override {
    ZSTD_inBuffer in_buf;
    ZSTD_outBuffer out_buf;

    in_buf.src = input;
    in_buf.size = static_cast<size_t>(input_len);
    in_buf.pos = 0;
    out_buf.dst = output;
    out_buf.size = static_cast<size_t>(output_len);
    out_buf.pos = 0;

    size_t ret;
    ret = ZSTD_compressStream(stream_, &out_buf, &in_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD compress failed: ");
    }
    return CompressResult{static_cast<int64_t>(in_buf.pos),
                          static_cast<int64_t>(out_buf.pos)};
  }

  Result<FlushResult> Flush(int64_t output_len, uint8_t* output) override {
    ZSTD_outBuffer out_buf;

    out_buf.dst = output;
    out_buf.size = static_cast<size_t>(output_len);
    out_buf.pos = 0;

    size_t ret;
    ret = ZSTD_flushStream(stream_, &out_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD flush failed: ");
    }
    return FlushResult{static_cast<int64_t>(out_buf.pos), ret > 0};
  }

  Result<EndResult> End(int64_t output_len, uint8_t* output) override {
    ZSTD_outBuffer out_buf;

    out_buf.dst = output;
    out_buf.size = static_cast<size_t>(output_len);
    out_buf.pos = 0;

    size_t ret;
    ret = ZSTD_endStream(stream_, &out_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD end failed: ");
    }
    return EndResult{static_cast<int64_t>(out_buf.pos), ret > 0};
  }

 protected:
  ZSTD_CStream* stream_;

 private:
  int compression_level_;
};

// ----------------------------------------------------------------------
// ZSTD codec implementation

class ZSTDCodec : public Codec {
 public:
  explicit ZSTDCodec(int compression_level)
      : compression_level_(compression_level == kUseDefaultCompressionLevel
                               ? kZSTDDefaultCompressionLevel
                               : compression_level) {}

  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    if (output_buffer == nullptr) {
      // We may pass a NULL 0-byte output buffer but some zstd versions demand
      // a valid pointer: https://github.com/facebook/zstd/issues/1385
      static uint8_t empty_buffer;
      DCHECK_EQ(output_buffer_len, 0);
      output_buffer = &empty_buffer;
    }
    auto cacheInstance = CompressionContextCache::Instance();
    auto dctx = cacheInstance->GetCachedZSTDUncompressData().Get();
    size_t ret = ZSTD_decompressDCtx(dctx, output_buffer, static_cast<size_t>(output_buffer_len),
                        input, static_cast<size_t>(input_len));

//    size_t ret = ZSTD_decompress(output_buffer, static_cast<size_t>(output_buffer_len),
//                                 input, static_cast<size_t>(input_len));
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD decompression failed: ");
    }
    if (static_cast<int64_t>(ret) != output_buffer_len) {
      return Status::IOError("Corrupt ZSTD compressed data.");
    }
    return static_cast<int64_t>(ret);
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return ZSTD_compressBound(static_cast<size_t>(input_len));
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer) override {
    size_t ret = ZSTD_compress(output_buffer, static_cast<size_t>(output_buffer_len),
                               input, static_cast<size_t>(input_len), compression_level_);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD compression failed: ");
    }
    return static_cast<int64_t>(ret);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<ZSTDCompressor>(compression_level_);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<ZSTDDecompressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Compression::type compression_type() const override { return Compression::ZSTD; }
  int minimum_compression_level() const override { return ZSTD_minCLevel(); }
  int maximum_compression_level() const override { return ZSTD_maxCLevel(); }
  int default_compression_level() const override { return kZSTDDefaultCompressionLevel; }

  int compression_level() const override { return compression_level_; }

 private:
  const int compression_level_;
};

}  // namespace

std::unique_ptr<Codec> MakeZSTDCodec(int compression_level) {
  return std::make_unique<ZSTDCodec>(compression_level);
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
