#include <cassert>
#include <chrono>
#include <exception>
#include <iostream>

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include "trace.h"

using IOFlags = seastar::open_flags;

seastar::future<uint64_t> fileSize(const std::string &name)
{
    ENSURE(!name.empty());
    return seastar::file_size(name);
}

seastar::future<std::string> load(
    seastar::file file, const uint64_t offset, const size_t size)
{
    std::string data(size, '\0');
    size_t n{0};

    while(size != n)
    {
        try
        {
            n += co_await file.dma_read( offset + n, data.data() + n, size - n);
        }
        catch(...) {ENSURE(false);}
    }
    co_return data;
}

seastar::future<std::string> load(
    const std::string &name, const uint64_t offset, const size_t size)
{
    TRACE_SCOPE(name, ", offset: ", offset, ", size: ", size);
    ENSURE(!name.empty());
    auto file = co_await seastar::open_file_dma(name, IOFlags::ro);
    auto data = co_await load(file, offset, size);
    co_await file.close();
    co_return data;
}

seastar::future<> file_istream_read0(std::string name)
{
    TRACE_SCOPE(name);
    const size_t blkSize{4096};
    const auto fSize = co_await fileSize(name);
    auto size = fSize & ~(blkSize - 1);
    TRACE("fSize: ", fSize, ", offset: ", 0, ", size: ", size);

    try
    {
        auto file = co_await seastar::open_file_dma(name, IOFlags::ro);
        auto fStream = seastar::make_file_input_stream(file, 0);
        auto data = co_await fStream.read_exactly(fSize);
        TRACE("rSize: ", data.size());
    }
    catch(...){ENSURE(false);}
    co_return;
}

seastar::future<> file_1read0(std::string name)
{
    TRACE_SCOPE(name);
    const size_t blkSize{4096};
    const auto fSize = co_await fileSize(name);
    auto size = fSize & ~(blkSize - 1);
    TRACE("fSize: ", fSize, ", offset: ", 0, ", size: ", size);
    co_await load(name, 0, size);
    co_return;
}

seastar::future<> file_xread0(std::string name, int n)
{
    TRACE_SCOPE(name);

    const size_t blkSize{4096};
    const auto size = co_await fileSize(name);
    auto xSize = (size / n) & ~(blkSize - 1);

    TRACE("fSize: ", size, ", xSize: ", xSize);

    co_await
        seastar::coroutine::parallel_for_each(
            boost::make_counting_iterator<int>(0),
            boost::make_counting_iterator<int>(n),
            [&](int i) -> seastar::future<>
            {
                (void)co_await load(name, i * xSize, xSize);
                co_return;
            });
    co_return;
}

seastar::future<> file_xread(std::string name, int n, const int p)
{
    const size_t blkSize{4096};
    const auto size = co_await fileSize(name);
    auto xSize = (size / n) & ~(blkSize - 1);
    uint64_t offset{0};

    TRACE_SCOPE(name, ", fSize: ", size, ", xSize: ", xSize, ", n: ", n, ", p: ", p);

    while(0 != n)
    {
        const auto pMax = std::min(n, p);

        TRACE("pMax: ", pMax, ", offset: ", offset);

        co_await
            seastar::coroutine::parallel_for_each(
                boost::make_counting_iterator<int>(0),
                boost::make_counting_iterator<int>(pMax),
                [&](int i) -> seastar::future<>
                {
                    co_await
                    seastar::smp::submit_to(
                        i,
                        [&]() -> seastar::future<>
                        {
                            (void)co_await load(name, offset + i * xSize, xSize);
                            co_return;
                        });
                    co_return;
                });
        offset += pMax * xSize;
        n -= pMax;
    }
    co_return;
}
/*----------------------------------------------------------------------------*/
int main(int argc, char* argv[])
{
    namespace options = boost::program_options;

    seastar::app_template app;

    app.add_options()("input", options::value<std::string>(), "input file");
    app.add_options()("n", options::value<int>(), "number of requests");
    app.add_options()("shards", options::value<unsigned int>(), "number of shards");

    return
        app.run(
            argc, argv,
            [&app]() -> seastar::future<>
            {
                auto &&cfg = app.configuration();
                if(!cfg.count("input")) co_return;
                if(!cfg.count("n")) co_return;
                const auto name{cfg["input"].template as<std::string>()};
                const auto n{cfg["n"].template as<int>()};
                const auto p{cfg.count("shards") ? cfg["shards"].template as<unsigned int>() : 1};
                if(0 == n && 1 == p) co_await file_istream_read0(name);
                else if(1 == n && 1 == p) co_await file_1read0(name);
                else if(1 == p) co_await file_xread0(name, n);
                else co_await file_xread(name, n, std::min(p, seastar::smp::count));
                co_return;
            });
}
