#ifndef RINGBUFFER_H
#define RINGBUFFER_H

#include <stddef.h>
#include <new>
#include <atomic>
#include <string.h>

namespace isaacr {

const std::size_t CACHE_LINE_SIZE = std::hardware_destructive_interference_size;

class RingBuffer {
public:

RingBuffer(size_t size) :
    _startRing(new char[size]()),
    _endRing(_startRing + size),
    _usedSpace(static_cast<std::ptrdiff_t>(0)),
    _freeSpace(static_cast<std::ptrdiff_t>(size)),
    _readBuffer(_startRing),
    _readPointer(_startRing),
    _writeBuffer(_startRing),
    _writePointer(_startRing) {}

~RingBuffer(){
    delete[] _startRing;
}

void write(const void* data, size_t length){
    
    auto size = static_cast<std::ptrdiff_t>(length);
    char* preWriteBuffer;//gfh

    while(true){
        preWriteBuffer = _writeBuffer.load(std::memory_order_acquire);
        while(_freeSpace.load(std::memory_order_acquire) < size);

        auto postWriteBuffer = preWriteBuffer + size;
        postWriteBuffer = postWriteBuffer < _endRing ? postWriteBuffer : postWriteBuffer - (_endRing - _startRing);

        _freeSpace.fetch_sub(size);

        if(_writeBuffer.compare_exchange_strong(preWriteBuffer, postWriteBuffer))
            break;

        _freeSpace.fetch_add(size, std::memory_order_relaxed);
    }

    if(preWriteBuffer + length < _endRing)
        std::memcpy(preWriteBuffer, data, length);



}

private:

char* _startRing;
char* _endRing;
alignas(CACHE_LINE_SIZE) std::atomic<std::ptrdiff_t> _usedSpace;
alignas(CACHE_LINE_SIZE) std::atomic<std::ptrdiff_t> _freeSpace;
alignas(CACHE_LINE_SIZE) std::atomic<char*> _readPointer{nullptr};
alignas(CACHE_LINE_SIZE) std::atomic<char*> _readBuffer{nullptr};
alignas(CACHE_LINE_SIZE) std::atomic<char*> _writePointer{nullptr};
alignas(CACHE_LINE_SIZE) std::atomic<char*> _writeBuffer{nullptr};

    
};

} 

#endif // RINGBUFFER_H