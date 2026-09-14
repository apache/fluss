/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <condition_variable>
#include <cstdio>
#include <exception>
#include <mutex>

#include "fluss.hpp"
#include "rust/cxx.h"

namespace fluss {
namespace ffi {

/// Per-writer admission control; independent of Rust buffer memory and ACK completion.
class WriteCallbackCapacity {
   public:
    explicit WriteCallbackCapacity(const WriteCallbackOptions& options) : options_(options) {}

    static Result Validate(const WriteCallbackOptions& options) {
        if (options.max_pending_operations == 0) {
            return {ErrorCode::CLIENT_ERROR, "max_pending_operations must be positive"};
        }
        if (options.enqueue_timeout.count() < 0) {
            return {ErrorCode::CLIENT_ERROR, "enqueue_timeout must be nonnegative"};
        }
        return {};
    }

    Result Acquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (pending_ == options_.max_pending_operations) {
            // Applies across writers and also to callbacks on the fallback executor.
            if (in_callback_ || options_.enqueue_timeout.count() == 0) {
                return {ErrorCode::CLIENT_ERROR, "Write callback capacity is full"};
            }
            if (!available_.wait_for(lock, options_.enqueue_timeout,
                                     [&] { return pending_ < options_.max_pending_operations; })) {
                return {ErrorCode::CLIENT_ERROR, "Timed out waiting for write callback capacity"};
            }
        }
        ++pending_;
        return {};
    }

    void Release() noexcept {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            --pending_;
        }
        available_.notify_one();
    }

   private:
    friend class WriteCallback;
    inline static thread_local bool in_callback_ = false;
    const WriteCallbackOptions options_;
    std::mutex mutex_;
    std::condition_variable available_;
    size_t pending_ = 0;
};

/// Owns a callback transferred to Rust. Access is exclusive, never concurrent.
class WriteCallback {
   public:
    explicit WriteCallback(fluss::WriteCallback callback) : callback_(std::move(callback)) {}

    WriteCallback(const WriteCallback&) = delete;
    WriteCallback& operator=(const WriteCallback&) = delete;

    /// Reserve before entering Rust. Destruction also returns capacity on submission failure.
    Result Reserve(std::shared_ptr<WriteCallbackCapacity> capacity) {
        if (!capacity) {
            return {ErrorCode::CLIENT_ERROR, "Writer not available"};
        }
        auto result = capacity->Acquire();
        if (result.Ok()) {
            reservation_.capacity = std::move(capacity);
        }
        return result;
    }

    /// Invoke once, containing all C++ exceptions on this side of the FFI boundary.
    void Complete(int32_t error_code, rust::Str error_message) noexcept {
        // Release captures before the reservation, even if this wrapper outlives Complete().
        Reservation reservation{std::move(reservation_.capacity)};
        CallbackScope scope;
        // Moving std::function alone need not empty the source. Swap with an
        // empty function so captures are released even if the callback throws.
        fluss::WriteCallback callback;
        callback.swap(callback_);
        Result result;
        result.error_code = error_code;
        try {
            result.error_message = std::string(error_message);
        } catch (...) {
            // Error text is best-effort; allocation failure must not skip completion.
            std::fprintf(stderr, "Fluss write callback could not copy error text (code %d)\n",
                         error_code);
        }
        try {
            callback(std::move(result));
        } catch (const std::exception& e) {
            std::fprintf(stderr, "Fluss write callback threw an exception: %s\n", e.what());
        } catch (...) {
            std::fprintf(stderr, "Fluss write callback threw an unknown exception\n");
        }
    }

   private:
    struct Reservation {
        std::shared_ptr<WriteCallbackCapacity> capacity;
        ~Reservation() {
            if (capacity) {
                capacity->Release();
            }
        }
    };

    struct CallbackScope {
        bool previous = std::exchange(WriteCallbackCapacity::in_callback_, true);
        ~CallbackScope() { WriteCallbackCapacity::in_callback_ = previous; }
    };

    // Member order keeps captures alive until invocation, but not past capacity release.
    Reservation reservation_;
    fluss::WriteCallback callback_;
};

}  // namespace ffi
}  // namespace fluss
