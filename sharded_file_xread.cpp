#include <vector>

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
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

struct LoadInfo
{
    uint64_t offset{0};
    uint64_t size{0};
};

using LoadInfoSeq = std::vector<LoadInfo>;

struct Loader
{
    std::string name;
    LoadInfoSeq seq;

    Loader(std::string name, uint64_t offset, const size_t stride, const int n):
        name{std::move(name)},
        seq(seastar::smp::count)
    {
        ENSURE(int(seastar::smp::count) >= n);
        int i{0};
        while(i != n) {seq[i] = {offset, stride}; offset += stride; ++i;}
    }

    Loader(const Loader &) = delete;
    Loader &operator=(const Loader &) = delete;
    seastar::future<> stop() {return seastar::make_ready_future<>();}
};

seastar::future<> file_xread(std::string name, int n, const int p)
{
    ENSURE(0 < p);
    ENSURE(int(seastar::smp::count) >= p);
    const size_t blkSize{4096};
    const auto size = co_await fileSize(name);
    auto xSize = (size / n) & ~(blkSize - 1);
    uint64_t offset{0};

    TRACE_SCOPE(name, ", fSize: ", size, ", xSize: ", xSize, ", n: ", n, ", p: ", p);

    while(0 != n)
    {
        seastar::sharded<Loader> loader;

        const auto pMax = std::min(n, p);
        co_await loader.start(name, offset, xSize, pMax);

        TRACE("pMax: ", pMax, ", offset: ", offset);

        auto exec =
            [](Loader &local) -> seastar::future<>
            {
                ENSURE(local.seq.size() == seastar::smp::count);
                auto info = local.seq[seastar::smp::count - 1];
                if(0 == info.size) co_return;
                co_await load(local.name, info.offset, info.size);
                co_return;
            };

        co_await loader.invoke_on_all(std::move(exec));
        co_await loader.stop();
        offset += pMax * xSize;
        n -= pMax;
    }
    co_return;
}

seastar::future<> file_xread_mr(std::string name, int n, const int p)
{
    ENSURE(0 < p);
    ENSURE(int(seastar::smp::count) >= p);
    const size_t blkSize{4096};
    const auto size = co_await fileSize(name);
    auto xSize = (size / n) & ~(blkSize - 1);
    uint64_t offset{0};

    TRACE_SCOPE(name, ", fSize: ", size, ", xSize: ", xSize, ", n: ", n, ", p: ", p);

    while(0 != n)
    {
        seastar::sharded<Loader> loader;

        const auto pMax = std::min(n, p);
        co_await loader.start(name, offset, xSize, pMax);

        TRACE("pMax: ", pMax, ", offset: ", offset);

        auto reducer =
            [](std::string data) -> seastar::future<>
            {
                co_return;
            };

        auto map =
            [](Loader &local, std::string name) -> seastar::future<std::string>
            {
                ENSURE(local.seq.size() == seastar::smp::count);
                auto info = local.seq[seastar::this_shard_id()];
                if(0 == info.size) co_return std::string{};
                auto data = co_await load(local.name, info.offset, info.size);
                co_return data;
            };

        co_await loader.map_reduce(std::move(reducer), std::move(map), std::string{name});
        co_await loader.stop();
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
    app.add_options()("mr", options::value<bool>(), "use map_reduce");

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
                const auto mr{cfg.count("mr") ? cfg["mr"].template as<bool>() : false};
                if(mr) co_await file_xread_mr(name, n, std::min(p, seastar::smp::count));
                else co_await file_xread(name, n, std::min(p, seastar::smp::count));
                co_return;
            });
}
