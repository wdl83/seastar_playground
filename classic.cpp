#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <list>
#include <memory>
#include <queue>
#include <vector>

#include <unistd.h>

#include "trace.h"

namespace trace {
std::unique_ptr<trace::Sink> makeSink() {return std::make_unique<trace::COutSink>();}
} // trace

#define ENSURE(cond) assert((cond))

using IOMode = std::ios_base;
using Position = std::fstream::pos_type;
using Block = std::vector<char>;
using BlockSeq = std::vector<Block>;
using SharedFile = std::shared_ptr<std::fstream>;

namespace std {
inline
std::ostream &operator<<(std::ostream &os, const Block &block)
{
    for(const auto i : block) os << i;
    return os;
}

inline
std::ostream &operator<<(std::ostream &os, const BlockSeq &seq)
{
    for(const auto &i : seq) os << i;
    return os;
}
} // std

Block loadBlock(std::fstream &input, size_t size, Position position)
{
    Block block(size);

    ENSURE(input);
    TRACE("position: ", position, ", size: ", block.size());
    input.seekg(position);
    input.read(block.data(), size);
    //TRACE(block);
    ENSURE(input);
    ENSURE(input.gcount() == Position(size));
    return block;
}

BlockSeq loadBlockSeq(
    std::fstream &input, const size_t blockSize, size_t size, Position position)
{
    ENSURE(0 == size % blockSize);

    const size_t num = size / blockSize;
    BlockSeq seq(num);

    TRACE("position: ", position, ", num: ", num, ", size: ", size);

    for(auto &block : seq)
    {
        block = loadBlock(input, std::min(blockSize, size), position);
        position += block.size();
        size -= block.size();
    }

    ENSURE(0 == size);
    return seq;
}

void storeBlock(std::fstream &output, const Block &block, Position position)
{
    ENSURE(output);
    output.seekp(position);
    output.write(block.data(), block.size());
    ENSURE(output);
    TRACE("position: ", position, ", size: ", block.size());
    //TRACE(block);
}

void storeBlockSeq(std::fstream &output, const BlockSeq &seq, Position position)
{
    TRACE("position: ", position, ", size: ", size(seq));

    for(const auto &i : seq)
    {
        storeBlock(output, i, position);
        position += i.size();
    }
}

inline
Position fileSize(const std::string &name)
{
    std::ifstream file{name, IOMode::binary | IOMode::ate};
    const auto position = file.tellg();
    if(file) return position;
    return 0;
}

struct Chunk
{
    Position position;
    size_t size;

    explicit Chunk(Position position, size_t size):
        position{position}, size{size}
    {}
};

inline
std::ostream &operator<<(std::ostream &os, const Chunk &chunk)
{
    os << "[" << chunk.position << " s" << chunk.size << "]";
    return os;
}

using ChunkSeq = std::list<Chunk>;

inline
std::pair<ChunkSeq::iterator, ChunkSeq::iterator::difference_type> successor(
    ChunkSeq::iterator i,
    ChunkSeq::iterator::difference_type n,
    const ChunkSeq::const_iterator end)
{
    ChunkSeq::iterator::difference_type l{0};

    while(0 < n && i != end) {--n; ++l; ++i;}
    return {i, l};
}

inline
void dump(ChunkSeq::const_iterator begin, ChunkSeq::const_iterator end)
{
    size_t n{0};
    std::ostringstream oss;

    while(begin != end) {oss << n << *begin << " "; ++n; ++begin;}
    TRACE(oss.str());
}

struct BlockInfo
{
    Block block;
    Chunk *chunk{nullptr};

    friend
    bool operator>(const BlockInfo &a, const BlockInfo &b){return a.block > b.block;}
};

using BlockQueue =
    std::priority_queue<
        BlockInfo,
        std::vector<BlockInfo>,
        std::greater<BlockInfo>>;

/* split file into chunks */
ChunkSeq split(Position inFileSize, size_t maxChunkSize)
{
    ChunkSeq seq;
    Position position = 0;

    while(inFileSize > position)
    {
        const auto chunkSize =
            std::min(Position(maxChunkSize), Position{inFileSize - position});

        seq.emplace_back(position, chunkSize);
        position += chunkSize;
    }

    TRACE("inFileSize: ", inFileSize, ", seqSize: ", seq.size());
    return seq;
}

/* sort single chunk data (load + sort + store) */
void sortChunk(SharedFile inFile, SharedFile outFile, Chunk &chunk, size_t blockSize)
{
    auto seq = loadBlockSeq(*inFile, blockSize, chunk.size, chunk.position);

    std::sort(std::begin(seq), std::end(seq));
    storeBlockSeq(*outFile, seq, chunk.position);
}

/* sort sequence of chunks */
void sortChunks(
    SharedFile inFile, SharedFile outFile, ChunkSeq &seq, size_t blockSize)
{
    TRACE("chunkSeq: ", seq.size());
    ENSURE(!seq.empty());
    ENSURE(outFile);

    for(auto &chunk : seq) sortChunk(inFile, outFile, chunk,  blockSize);
}

void pushBlock(std::fstream &file, Chunk &chunk, size_t blockSize, BlockQueue &queue)
{
    if(0 == chunk.size) return;

    queue.push({loadBlock(file, blockSize, chunk.position), &chunk});
    chunk.position += blockSize;
    chunk.size -= blockSize;
}

size_t nWayMerge(
    SharedFile file,
    ChunkSeq::iterator begin, ChunkSeq::const_iterator end,
    const size_t blockSize,
    Position dstOffset)
{
    ENSURE(file);
    BlockQueue queue;

    for(auto i = begin; end != i; ++i) pushBlock(*file, *i, blockSize, queue);

    size_t size{0};

    for(;;)
    {
         if(queue.empty()) break;

         const auto &blockInfo = queue.top();

         storeBlock(*file, blockInfo.block, dstOffset);
         dstOffset += blockSize;
         size += blockSize;
         ENSURE(blockInfo.chunk);
         pushBlock(*file, *blockInfo.chunk, blockSize, queue);
         queue.pop();
    }
    return size;
}

void merge(
    SharedFile file,
    ChunkSeq &seq,
    Position inFileSize,
    const size_t maxChunkSize, const size_t blockSize)
{
    ENSURE(!seq.empty());

    const auto maxBlockNum = maxChunkSize / blockSize;
    /* file:
     *      [sorted chk0 | ... | sorted chkN ][ tmp chk0 | ........ | tmp chkM ]
     *      |< ---------   stride   -------->||< ---------   stride   -------->|
     */
    const auto stride{inFileSize};
    const auto stride2x{stride << 1};

    while(1 < seq.size())
    {
        /* max number of chunks we are able to process with maxChunkSize limit */
        const auto maxChunkNum = std::min(maxBlockNum, seq.size());
        auto begin = std::begin(seq);

        TRACE("seqSize: ", seq.size(), ", stride: ", stride);
        dump(begin, std::end(seq));

        while(begin != std::end(seq))
        {
            const auto next = successor(begin, maxChunkNum, std::end(seq));
            const auto offset = (begin->position + stride) % stride2x;

            TRACE(
                "range: ", next.second,
                ", srcOffset: ", begin->position,
                ", dstOffset: ", offset);

            dump(begin, next.first);

            const auto size = nWayMerge(file, begin, next.first, blockSize, offset);

            begin->position = offset;
            begin->size = size;
            seq.erase(std::next(begin), next.first);
            begin = next.first;
        }
    }

    ENSURE(1 == seq.size());

    TRACE("result: position: ", seq.front().position, ", size: ", seq.front().size);

    if(0 != seq.front().position)
    {
        TRACE("copy right-to-left");

        std::vector<char> buf(maxChunkSize);
        Position size{0};

        while(inFileSize != size)
        {
            const auto bufSize = std::min(Position(inFileSize - size), Position(buf.size()));
            ENSURE(file);
            file->seekg(size + inFileSize);
            file->read(buf.data(), bufSize);
            file->seekp(size);
            file->write(buf.data(), bufSize);
            size += bufSize;
        }
    }
}

void classicSort(
    const std::string &inFileName,
    const std::string &outFileName,
    size_t maxChunkSize,
    size_t blockSize)
{
    ENSURE(0 == maxChunkSize % blockSize);
    ENSURE(maxChunkSize >= blockSize << 1);

    const auto inFileSize = fileSize(inFileName);

    ENSURE(0 == inFileSize % blockSize);

    {
        auto seq = split(inFileSize, maxChunkSize);

        auto inFile =
            std::make_shared<std::fstream>(inFileName, IOMode::binary | IOMode::in);
        auto outFile =
            std::make_shared<std::fstream>(
                outFileName, IOMode::binary | IOMode::in | IOMode::out | IOMode::trunc);

        sortChunks(inFile, outFile, seq, blockSize);
        merge(outFile, seq, inFileSize, maxChunkSize, blockSize);
    }

    truncate(outFileName.c_str(), inFileSize);
}

void help(const char *argv0, const char *message = nullptr)
{
    if(message) std::cerr << "WARNING: " << message << '\n';

    std::cerr
        << argv0
        << " -i filename -o filename -m max_size -b block_size"
        << std::endl;
}

int error(const char *argv0, const char *reason)
{
    help(argv0, reason);
    return EXIT_FAILURE;
}

int main(int argc, char *argv[])
{
    std::string inFileName, outFileName;
    size_t maxSize = 0, blockSize = 0;

    for(int c; -1 != (c = ::getopt(argc, argv, "hi:o:m:b:"));)
    {
        switch(c)
        {
            case 'h':
                help(argv[0]);
                return EXIT_SUCCESS;
                break;
            case 'i':
                inFileName = optarg ? optarg : "";
                break;
            case 'o':
                outFileName = optarg ? optarg : "";
                break;
            case 'm':
                maxSize = optarg ? atoll(optarg) : maxSize;
                break;
            case 'b':
                blockSize = optarg ? atoll(optarg) : blockSize;
                break;
            case ':':
            case '?':
            default:
                help(argv[0], "geopt() failure");
                return EXIT_FAILURE;
                break;
        }
    }

    if(inFileName.empty()) return error(argv[0], "input file name missing");
    const auto inFileSize = fileSize(inFileName);
    if(0 == inFileSize) return error(argv[0], "input file empty");
    if(outFileName.empty()) return error(argv[0], "output file name missing");
    if(1 > blockSize) return error(argv[0], "block_size must be > 0");
    if(maxSize < (blockSize << 1)) return error(argv[0], "max_size must be >= 2x block_size");
    if(0 != inFileSize % blockSize) return error(argv[0], "input file must be aligned to block boundary");

    if(0 != maxSize % blockSize)
    {
        maxSize = (maxSize / blockSize) * blockSize;
        std::cout
            << "realigned max_size: " << maxSize
            << " to block_size: " << blockSize
            << " boundary";
    }

    TRACE(
        "inFileName: ", inFileName,
        ", inFileSize: ", inFileSize,
        ", maxSize: ", maxSize,
        ", blockSize: ", blockSize,
        ", outFileName: ", outFileName);

    classicSort(inFileName, outFileName, maxSize, blockSize);
    return EXIT_SUCCESS;
}
