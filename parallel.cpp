#include <cassert>
#include <cstdint>
#include <iostream>
#include <list>
#include <queue>

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include "trace.h"

#define ENSURE_ALIGNED(value, alignment) ENSURE(0 == ((value) & ((alignment) - 1)))
//#define ENSURE_ALIGNED(value, alignment)

using Block = std::vector<char>;
using BlockSeq = std::vector<Block>;

/* filesystem access */
using Size = uint64_t;
using Position = uint64_t;
using IOFlags = seastar::open_flags;

struct Chunk
{
    Position position{0};
    size_t size{0};

    Chunk()
    {}
    explicit Chunk(Position position, size_t size):
        position{position}, size{size}
    {}
};

using ChunkSeq = std::list<Chunk>;

inline
std::ostream &operator<<(std::ostream &os, const Chunk &chunk)
{
    os << "[" << chunk.position << " s" << chunk.size << "]";
    return os;
}

template <typename I, typename CI, typename D>
I successor(I begin, const CI end, D n)
{
    while(D{0} < n && begin != end){--n; ++begin;}
    return begin;
}

struct BlockInfo
{
    Block block;
    Chunk *chunk{nullptr};

    friend
    bool operator>(const BlockInfo &a, const BlockInfo &b)
    {return a.block > b.block;}
};

using BlockQueue =
    std::priority_queue<BlockInfo, std::vector<BlockInfo>, std::greater<BlockInfo>>;

/* hack over seastar */
namespace std {
inline
std::ostream &operator<<(std::ostream &os, const Block &block)
{
    for(const auto i : block) os << i;
    return os;
}
} // std

inline
void dump(ChunkSeq::const_iterator begin, ChunkSeq::const_iterator end)
{
    size_t n{0};
    std::ostringstream oss;

    while(begin != end)
    {
        oss << n << *begin << " ";
        ++n; ++begin;
    }
    TRACE(oss.str());
}

seastar::future<> loadBlock(
    seastar::file file, Block &block, const Position position)
{
    ENSURE(file);
    ENSURE(block.size());
    ENSURE_ALIGNED(block.size(), file.disk_read_dma_alignment());
    ENSURE_ALIGNED(uintptr_t(block.data()), file.disk_read_dma_alignment());
    size_t size{0};

    while(block.size() != size)
    {
        try
        {
            size +=
                co_await
                file.dma_read(position, block.data() + size, block.size() - size);
        }
        catch(...) {ENSURE(false);}
    }

    //TRACE("position: ", position);
    //TRACE("\n", block);
    co_return;
}

seastar::future<BlockSeq> loadBlockSeq(
    seastar::file file,
    const size_t blockSize, const size_t seqSize, const Position position)
{
    ENSURE(file);
    ENSURE(0 == seqSize % blockSize);
    size_t num = seqSize / blockSize;

    //TRACE_SCOPE("position: ", position, ", num: ", num, ", seqSize: ", seqSize);

    BlockSeq seq(num, Block(blockSize));
    auto offset = position;

    for(auto &block : seq)
    {
        co_await loadBlock(file, block, offset);
        offset += blockSize;
    }
    co_return seq;
}

seastar::future<> storeBlock(
    seastar::file file, const Block &block, Position position)
{
    ENSURE(file);
    ENSURE(block.size());
    ENSURE_ALIGNED(block.size(), file.disk_write_dma_alignment());
    ENSURE_ALIGNED(uintptr_t(block.data()), file.disk_write_dma_alignment());

    try
    {
        co_await file.dma_write(position, block.data(), block.size());
    }
    catch(...) {ENSURE(false);}

    //TRACE("position: ", position, ", size: ", block.size());
    //TRACE("\n", block);
    co_return;
}

seastar::future<> storeBlockSeq(
    seastar::file file, const BlockSeq &seq, const Position position)
{
    ENSURE(file);
    //TRACE_SCOPE("position: ", position, ", num: ", seq.size());

    auto offset = position;

    for(const auto &i : seq)
    {
        co_await storeBlock(file, i, offset);
        offset += i.size();
    }
    co_return;
}

/* split file into chunks */
ChunkSeq split(Size fileSize, size_t maxChunkSize)
{
    ChunkSeq seq;
    Position position = 0;

    while(fileSize > position)
    {
        const auto chunkSize = std::min(Size(maxChunkSize), fileSize - position);

        seq.emplace_back(position, chunkSize);
        position += chunkSize;
    }

    TRACE("fileSize: ", fileSize, ", seqSize: ", seq.size());
    return seq;
}

/* sort single chunk data (load + sort + store) */
seastar::future<> sortChunk(
    seastar::file inFile,
    std::string outName,
    const Chunk &chunk, size_t blockSize)
{
    auto seq = co_await loadBlockSeq(inFile, blockSize, chunk.size, chunk.position);
    std::sort(std::begin(seq), std::end(seq));
    auto outFile = co_await seastar::open_file_dma(outName, IOFlags::wo);
    co_await storeBlockSeq(outFile, seq, chunk.position);
    co_await outFile.flush();
    co_await outFile.close();
    co_return;
}

struct Sorter
{
    std::vector<Chunk> chunks;

    Sorter(
        ChunkSeq::const_iterator begin, ChunkSeq::const_iterator end):
        chunks(seastar::smp::count)
    {
        auto i{0u};
        while(begin != end && seastar::smp::count > i)
        {
            chunks[i] = *begin; ++begin; ++i;
        }
    }
    seastar::future<> stop() {return seastar::make_ready_future<>();}
};

seastar::future<> createOutFile(Size inFileSize, std::string name)
{
    auto outFile =
        co_await
            seastar::open_file_dma(
                name, IOFlags::create | IOFlags::rw | IOFlags::truncate);
    /* pre-allocate output file, fill last block to get file size fixed
     * before concurrent writes */
    Block blank(outFile.disk_write_dma_alignment(), '?');
    co_await outFile.allocate(0, inFileSize << 1);
    co_await storeBlock(outFile, blank, (inFileSize << 1) - blank.size());
    co_await outFile.flush();
    co_await outFile.close();
}

/* sort sequence of chunks */
seastar::future<> sortChunks(
    std::string inName, Size inFileSize, std::string outName,
    const ChunkSeq &seq, size_t blockSize)
{
    ENSURE(!inName.empty());
    ENSURE(!outName.empty());
    ENSURE(!seq.empty());
    TRACE_SCOPE("chunkSeq: ", seq.size());

    auto inFile = co_await seastar::open_file_dma(inName, IOFlags::ro);
    co_await createOutFile(inFileSize, outName);

    const auto end = std::end(seq);
    auto begin = std::begin(seq);

    while(begin != end)
    {
        seastar::sharded<Sorter> sorter;

        auto next = successor(begin, end, seastar::smp::count);
        co_await sorter.start(begin, next);

        auto exec =
            [](
            Sorter &sorter,
            seastar::file_handle inHandle, std::string outName,
            size_t blockSize)
            -> seastar::future<>
            {
                auto inFile = inHandle.to_file();
                auto chunk = sorter.chunks[seastar::this_shard_id()];
                if(0 == chunk.size) co_return;
                co_await sortChunk(inFile, outName, chunk, blockSize);
                co_await inFile.close();
                co_return;
            };

        co_await
            sorter.invoke_on_all(
                std::move(exec), inFile.dup(), std::string{outName}, size_t{blockSize});
        co_await sorter.stop();
        begin = next;
    }

    co_await inFile.close();
    co_return;
}

seastar::future<> pushBlock(
    seastar::file file, Chunk &chunk, size_t blockSize, BlockQueue &queue)
{
    if(0 == chunk.size) co_return;

    Block block(blockSize);

    co_await loadBlock(file, block, chunk.position);
    queue.push({std::move(block), &chunk});
    chunk.position += blockSize;
    chunk.size -= blockSize;
    co_return;
}

seastar::future<size_t> nWayMerge(
    std::string fileName,
    ChunkSeq::iterator begin, ChunkSeq::const_iterator end,
    const size_t blockSize,
    Position dstOffset)
{
    BlockQueue queue;
    auto file = co_await seastar::open_file_dma(fileName, IOFlags::rw);

    for(auto i = begin; end != i; ++i) co_await pushBlock(file, *i, blockSize, queue);

    size_t size{0};

    while(!queue.empty())
    {
         const auto &blockInfo = queue.top();
         co_await storeBlock(file, blockInfo.block, dstOffset);
         dstOffset += blockSize;
         size += blockSize;
         ENSURE(blockInfo.chunk);
         co_await pushBlock(file, *blockInfo.chunk, blockSize, queue);
         queue.pop();
    }

    co_await file.flush();
    co_await file.close();
    co_return size;
}

struct Merger
{
    struct Range
    {
        ChunkSeq::iterator begin;
        ChunkSeq::iterator end;
        size_t length{0};
    };
    std::vector<Range> ranges;

    Merger(
        ChunkSeq::iterator begin, ChunkSeq::const_iterator const end,
        size_t maxChunkNum):
        ranges(seastar::smp::count)
    {
        for(auto &range : ranges)
        {
            if(begin == end) break;

            auto next = begin;
            auto i{0u};
            while(next != end && maxChunkNum > i) {++next; ++i;}
            range.begin = begin;
            range.end = next;
            range.length = i;
            begin = next;
        }
    }
    seastar::future<> stop() {return seastar::make_ready_future<>();}
};

seastar::future<> truncateOutFile(
    Size inFileSize, std::string name, size_t chunkSize, bool shift)
{
    auto file = co_await seastar::open_file_dma(name, IOFlags::rw);

    if(shift)
    {
        TRACE("shift data");
        std::vector<char> buf(chunkSize);
        Position size{0};

        while(inFileSize != size)
        {
            const auto bufSize = std::min(Position(inFileSize - size), Position(buf.size()));
            co_await file.dma_read(size + inFileSize, buf.data(), bufSize);
            co_await file.dma_write(size, buf.data(), bufSize);
            size += bufSize;
        }
    }

    co_await file.truncate(inFileSize);
    co_await file.flush();
    co_await file.close();
}

seastar::future<> merge(
    const Size inFileSize,
    const std::string &fileName,
    ChunkSeq &seq,
    const size_t chunkSize, const size_t blockSize)
{
    ENSURE(!seq.empty());
    ENSURE((inFileSize << 1) == co_await seastar::file_size(fileName));

    const auto maxBlockNum = chunkSize / blockSize;
    /* file:
     *      [sorted chk0 | ... | sorted chkN ][ tmp chk0 | ........ | tmp chkM ]
     *      |< ---------   stride   -------->||< ---------   stride   -------->|
     */
    const auto stride{inFileSize};

    while(1 < seq.size())
    {
        /* max number of chunks we are able to process with chunkSize limit */
        const auto maxChunkNum = std::min(maxBlockNum, seq.size());
        auto begin = std::begin(seq);
        auto end = std::cend(seq);

        TRACE("seqSize: ", seq.size(), ", stride: ", stride, ", maxChunkNum: ", maxChunkNum);
        //dump(begin, end);

        while(begin != end)
        {
            seastar::sharded<Merger> merger;
            const auto next = successor(begin, end, seastar::smp::count * maxChunkNum);
            co_await merger.start(begin, next, maxChunkNum);
            begin = next;

            auto reduce =
                [&seq](Merger::Range range) -> seastar::future<>
                {
                    if(0 == range.length) co_return;
                    seq.erase(std::next(range.begin), range.end);
                };

            auto exec =
                [](
                    Merger &merger,
                    std::string fileName,
                    size_t stride, size_t blockSize)
                -> seastar::future<Merger::Range>
                {
                    auto range = merger.ranges[seastar::this_shard_id()];
                    if(size_t{0} == range.length) co_return range;
                    const auto offset = (range.begin->position + stride) % (stride << 1);

                    TRACE(
                        "src/dst offset: ", range.begin->position, "/", offset,
                        ", chunks: ", range.length);
                    //dump(range->begin, range->end);

                    const auto size =
                        co_await nWayMerge(
                            fileName, range.begin, range.end, blockSize, offset);

                    range.begin->position = offset;
                    range.begin->size = size;
                    co_return std::move(range);
                };

            co_await
                merger.map_reduce(
                    std::move(reduce), std::move(exec),
                    std::string{fileName}, size_t{stride}, size_t{blockSize});
            co_await merger.stop();
        }
    }

    ENSURE(1 == seq.size());
    co_await truncateOutFile(inFileSize, fileName, chunkSize, 0 != seq.front().position);
    co_return;
}
/*----------------------------------------------------------------------------*/
void help(const char *message = nullptr)
{
    if(message) std::cout << "WARNING: " << message << "\n";
    std::cout << " -i filename" " -o filename" " -m max_size/shard" " -b block_size\n";
}

int error(const char *reason)
{
    help(reason);
    return EXIT_FAILURE;
}

int main(int argc, char* argv[])
{
    namespace options = boost::program_options;

    seastar::app_template app;

    app.add_options()("input", options::value<std::string>(), "input file");
    app.add_options()("output", options::value<std::string>(), "output file");
    app.add_options()("block", options::value<size_t>(), "block size");
    app.add_options()("chunk", options::value<size_t>(), "chunk size");

    app.run(
        argc, argv,
        [&app]() -> seastar::future<int>
        {
            auto cfg = app.configuration();
            const auto inName = cfg["input"].as<std::string>();
            const auto outName = cfg["output"].as<std::string>();
            const auto blockSize = cfg["block"].as<size_t>();
            auto chunkSize = cfg["chunk"].as<size_t>();
            const auto inFileSize = co_await seastar::file_size(inName);

            TRACE("inFileSize: ", inFileSize, ", blockSize: ", blockSize, ", chunkSize: ", chunkSize);

            if(0 >= inFileSize)
                co_return error("input file empty");
            if(0 >= blockSize)
                co_return error("block size >= 1");
            if(0 != inFileSize % blockSize)
                co_return error("input file not aligned to block boundary");
            if(chunkSize < (blockSize << 1))
                co_return error("chunk size > 2x block size");
            if(0 != chunkSize % blockSize)
            {
                TRACE("realigned chunk size to block boundary: ", chunkSize);
                chunkSize = (chunkSize / blockSize) * blockSize;
            }

            auto seq = split(inFileSize, chunkSize);
            co_await sortChunks(inName, inFileSize, outName, seq, blockSize);
            co_await merge(inFileSize, outName, seq, chunkSize, blockSize);
            co_return EXIT_SUCCESS;
        });
}
