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
    char* preWriteBuffer;

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
        memcpy(preWriteBuffer, data, length);
    else{
        size_t stubLength = _endRing - preWriteBuffer;
        memcpy(preWriteBuffer, data, stubLength);
        memcpy(_startRing, data + stubLength, length - stubLength);
    }

    while(preWriteBuffer != _writePointer.load());

    auto postWriteBuffer = preWriteBuffer + size;
    postWriteBuffer = postWriteBuffer < _endRing ? postWriteBuffer : postWriteBuffer - (_endRing - _startRing);

    _writePointer.store(postWriteBuffer);
    _usedSpace.fetch_add(length, std::memory_order_relaxed);

}

void read(void* data, std::size_t length){

    auto size = static_cast<std::ptrdiff_t>(length);
    char* preReadBuffer;

    while(true){
        preReadBuffer = _readBuffer.load(std::memory_order_consume);
        while(_usedSpace.load(std::memory_order_consume) < size);

        auto postReadBuffer = preReadBuffer + size;
        postReadBuffer = postReadBuffer < _endRing ? postReadBuffer : postReadBuffer - (_endRing - _startRing);
        _usedSpace.fetch_sub(size);

        if(_readPointer.compare_exchange_strong(preReadBuffer, postReadBuffer))
            break;
        
        _usedSpace.fetch_add(size, std::memory_order_relaxed);
    }
    
    if(preReadBuffer + length < _endRing){
        memcpy(data, preReadBuffer, length);
    }
    else{
        auto stubLength = _endRing - preReadBuffer;
        memcpy(data, preReadBuffer, stubLength);
        memcpy(data + stubLength, _startRing, length - stubLength);
    }

    auto postReadBuffer = preReadBuffer + size;
    postReadBuffer = postReadBuffer < _endRing ? postReadBuffer : postReadBuffer - (_endRing - _startRing);

    while(_readPointer.load() != preReadBuffer);

    _readPointer.store(postReadBuffer);
    _freeSpace.fetch_add(length, std::memory_order_relaxed);

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