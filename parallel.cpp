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
#define ALIGN_TO(value, alignment) ((value) & (~((alignment) - 1)))

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
    size_t no{0};
    // for nWayMerge
    size_t preloadSize{0};

    Chunk()
    {}
    explicit Chunk(Position position, size_t size, size_t no):
        position{position}, size{size}, no{no}
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

seastar::future<BlockSeq> loadBlockSeq(
    seastar::file file,
    const size_t blockSize, const size_t seqSize, const Position position)
{
    ENSURE(file);
    ENSURE(0 == seqSize % blockSize);
    size_t num = seqSize / blockSize;

    //TRACE_SCOPE("position: ", position, ", num: ", num, ", seqSize: ", seqSize);

    BlockSeq seq(num, Block(blockSize));
    std::vector<iovec> iov(num, iovec{nullptr, 0});

    for(size_t i{0}; num > i; ++i)
    {
        iov[i].iov_base = seq[i].data();
        iov[i].iov_len = blockSize;
    }

    auto size{seqSize};
    auto offset{position};

    while(size)
    {
        const auto n = co_await file.dma_read(offset, iov);

        //TRACE("n: ", n, ", size: ", size, ", iov.size: ", iov.size());

        ENSURE(0 != n);
        ENSURE(0 == n % blockSize);
        ENSURE(size >= n);
        size -= n;
        offset += n;

        if(size)
        {
            std::rotate(
                std::begin(iov),
                std::next(std::begin(iov), n / blockSize),
                std::end(iov));
            iov.resize(size / blockSize);
        }
    }
    co_return seq;
}

seastar::future<> storeBlockSeq(
    seastar::file file,
    BlockSeq &seq, const size_t blockSize, const Position position)
{
    ENSURE(file);
    //TRACE_SCOPE("position: ", position, ", num: ", seq.size());

    const auto num{seq.size()};
    std::vector<iovec> iov(num, iovec{nullptr, 0});

    for(size_t i{0}; num > i; ++i)
    {
        iov[i].iov_base = seq[i].data();
        iov[i].iov_len = blockSize;
        ENSURE(seq[i].size() == blockSize);
    }

    auto size{num * blockSize};
    auto offset{position};

    while(size)
    {
        const auto n = co_await file.dma_write(offset, iov);

        //TRACE("n: ", n, ", size: ", size, ", iov.size: ", iov.size());

        ENSURE(0 != n);
        ENSURE(0 == n % blockSize);
        ENSURE(size >= n);
        size -= n;
        offset += n;

        if(size)
        {
            std::rotate(
                std::begin(iov),
                std::next(std::begin(iov), n / blockSize),
                std::end(iov));
            iov.resize(size / blockSize);
        }
    }

    co_return;
}

/* split file into chunks */
ChunkSeq split(Size fileSize, size_t maxChunkSize)
{
    ChunkSeq seq;
    Position position = 0;
    size_t n{0};

    while(fileSize > position)
    {
        const auto chunkSize = std::min(Size(maxChunkSize), fileSize - position);

        seq.emplace_back(position, chunkSize, n++);
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
    TRACE_SCOPE("chunk: ", chunk.no);

    auto seq = co_await loadBlockSeq(inFile, blockSize, chunk.size, chunk.position);
    {
        //TRACE_SCOPE("sort: ", chunk.no);
        std::sort(std::begin(seq), std::end(seq));
    }

    auto outFile = co_await seastar::open_file_dma(outName, IOFlags::wo);
    co_await storeBlockSeq(outFile, seq, blockSize, chunk.position);
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
    const auto blockSize{outFile.disk_write_dma_alignment()};
    /* pre-allocate output file, fill last block to get file size fixed
     * before concurrent writes */
    BlockSeq blanks(1, Block(blockSize, '?'));
    co_await outFile.allocate(0, inFileSize << 1);
    co_await storeBlockSeq( outFile, blanks, blockSize, (inFileSize << 1) - blockSize);
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

seastar::future<> pushBlocks(
    seastar::file file, Chunk &chunk,
    size_t blockSize, size_t preloadSize,
    BlockQueue &queue)
{
    //TRACE_SCOPE("chunk: ", chunk.no, ", preloadSize: ", preloadSize);
    if(0 == chunk.size) co_return;

    preloadSize = std::min(chunk.size, preloadSize - chunk.preloadSize);

    if(0 == preloadSize) co_return;

    ENSURE(0 == preloadSize % blockSize);

    auto seq = co_await loadBlockSeq(file, blockSize, preloadSize, chunk.position);

    for(auto i = std::begin(seq); std::end(seq) != i; ++i)
    {
        queue.push({std::move(*i), &chunk});
    }
    chunk.position += preloadSize;
    chunk.size -= preloadSize;
    chunk.preloadSize += preloadSize;
    co_return;
}

seastar::future<size_t> nWayMerge(
    std::string fileName,
    Merger::Range range,
    const size_t blockSize, const size_t maxSize,
    Position dstOffset)
{
    const auto chunkSize{ALIGN_TO(maxSize / range.length, blockSize)};
    const auto num{chunkSize / blockSize};

    TRACE_SCOPE(
        "range: ", range.length,
        ", begin: ", range.begin->no, ", end: ", range.begin->no + range.length,
        ", maxSize: ", maxSize,
        ", chunkSize: ", chunkSize,
        ", num: ", num);

    BlockQueue queue;
    auto file = co_await seastar::open_file_dma(fileName, IOFlags::rw);

    for(auto i = range.begin; range.end != i; ++i)
    {
        co_await pushBlocks(file, *i, blockSize, chunkSize, queue);
    }

    size_t size{0};

    while(!queue.empty())
    {
        auto stop{false};
        BlockSeq seq;
        seq.reserve(num << 1);

        while(!queue.empty() && !stop)
        {
            auto &top = queue.top();
            ENSURE(top.chunk);
            ENSURE(top.chunk->preloadSize >= blockSize);
            seq.push_back(std::move(top.block));
            top.chunk->preloadSize -= blockSize;
            stop = 0 == top.chunk->preloadSize;
            queue.pop();
        }
        co_await storeBlockSeq(file, seq, blockSize, dstOffset);
        dstOffset += seq.size() * blockSize;
        size += seq.size() * blockSize;

        for(auto i = range.begin; range.end != i; ++i)
        {
            co_await pushBlocks(file, *i, blockSize, chunkSize, queue);
        }
    }

    co_await file.flush();
    co_await file.close();
    co_return size;
}

seastar::future<> truncateOutFile(
    Size inFileSize, std::string name, size_t chunkSize, bool shift)
{
    auto file = co_await seastar::open_file_dma(name, IOFlags::rw);
    chunkSize = std::min(chunkSize, file.disk_write_max_length());

    TRACE_SCOPE("name: ", name, ", to: ", inFileSize, ", bufSize: ", chunkSize);

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
                    size_t stride, size_t blockSize, size_t chunkSize)
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
                            fileName, range, blockSize, chunkSize, offset);

                    range.begin->position = offset;
                    range.begin->size = size;
                    co_return std::move(range);
                };

            co_await
                merger.map_reduce(
                    std::move(reduce), std::move(exec),
                    std::string{fileName},
                    size_t{stride}, size_t{blockSize}, size_t{chunkSize});
            co_await merger.stop();
        }
    }

    ENSURE(1 == seq.size());
    co_await truncateOutFile(inFileSize, fileName, chunkSize, 0 != seq.front().position);
    co_return;
}
/*----------------------------------------------------------------------------*/
int error(const char *reason)
{
    if(reason) std::cout << "WARNING: " << reason << "\n";
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
