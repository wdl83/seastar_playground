#pragma once

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#include <libgen.h>
#include <unistd.h>

#define ENSURE(cond) assert((cond))

namespace trace {

using Clock = std::chrono::steady_clock;

struct Dbg
{
    const char *file{nullptr};
    const int line{-1};
    const char *function{nullptr};

    Dbg(const char *file, int line, const char *function):
        file{file}, line{line}, function{function}
    {}

    explicit
    operator bool() const {return -1 != line;}

    friend
    std::ostream &operator<< (std::ostream &os, const Dbg &dbg)
    {
        if(dbg.file) os << (::basename(const_cast<char *>(dbg.file)));
        if(-1 != dbg.line) os << ':' << dbg.line << ' ';
        if(dbg.function) os << ' ' << dbg.function;
        return os;
    }
};

struct Sink
{
    struct Hint
    {
        using address = void *;
        using difference = std::ptrdiff_t;
        address addr;
        difference diff;
    };

    virtual ~Sink() {}
    virtual void log(
        const Dbg &, const std::string &, int stackDepth, Hint) = 0;
    virtual void flush() = 0;
};

inline
std::ostream &operator<<(std::ostream &os, Sink::Hint hint)
{
    auto flags = os.flags();
    os
        << "0x" << std::hex << hint.addr
        << '/' << "0x" << std::hex << hint.diff
        << '(' << std::dec << hint.diff << ')';
    os.flags(flags);
    return os;
}

std::unique_ptr<Sink> makeSink();

namespace impl {

template <typename T>
void serialize(std::ostream &os, const T &value) {os << value;}

class Log
{
    std::unique_ptr<Sink> sink;

    void logImpl(std::ostream &) {}

    template <typename T, typename ...Tn>
    void logImpl(std::ostream &os, const T &value, const Tn &...tail)
    {
        serialize(os, value);
        logImpl(os, tail...);
    }
public:
    Log(): sink{makeSink()} {}
    ~Log(){if(sink) sink->flush();}
    Log(const Log &) = delete;
    Log &operator= (const Log &) = delete;

    template <typename ...Tn>
    void log(Sink::Hint hint, int stackDepth, const Dbg &dbg, const Tn &...tail)
    {
        if(!sink) return;
        std::ostringstream oss;
        logImpl(oss, tail...);
        sink->log(dbg, oss.str(), stackDepth, hint);
    }

    static
    Log &instance()
    {
        static Log log;
        return log;
    }
};

struct LogScope
{
    const Dbg dbg;
    Clock::time_point tp{Clock::now()};
    Sink::Hint::address prev{LogScope::addr()};

    static
    int &depth()
    {
        thread_local static int depth_{0};
        return depth_;
    }

    static
    Sink::Hint::address &addr()
    {
        thread_local static Sink::Hint::address addr_{nullptr};
        return addr_;
    }

    template <typename ...Tn>
    LogScope(const Dbg &dbg, const Tn &...tail): dbg{dbg}
    {
        auto diff =
            reinterpret_cast<const char *>(this)
            - static_cast<const char *>(prev);

        if(0 > diff) diff = 0;
        if(16 * 1024 < diff) diff = 0;

        Log::instance().log({this, diff}, depth(), dbg, "{ ", tail...);
        ++depth();
        addr() = this;
    }

    LogScope(const LogScope &) = delete;
    LogScope(LogScope &&) = delete;
    LogScope &operator=(const LogScope &) = delete;
    LogScope &operator=(LogScope &&) = delete;

    ~LogScope()
    {
        using namespace std::chrono;

        --depth();
        const auto diff = duration_cast<microseconds>(Clock::now() - tp);
        Log::instance().log(
            {this, 0},
            depth(), dbg, "} ", diff.count(), "us");
    }
};

} /* impl */

struct COutSink: public Sink
{
    void log(
        const Dbg &dbg, const std::string &msg, int depth, Hint hint) override final
    {
        std::cout
            << getpid() << ':' << gettid() << ' '
            << dbg
            << ' ' << depth << '@' << hint << ' ' << msg << '\n';
    }
    void flush() override final {std::cout << std::flush;}
};

} /* trace */


#define JOIN_TOKEN_IMPL(a, b) a ## b
#define JOIN_TOKEN(a, b) JOIN_TOKEN_IMPL(a, b)

#ifdef TRACE_ENABLED

#define TRACE(...) \
    do \
    { \
        trace::impl::Log::instance().log(\
            {nullptr, 0}, \
            trace::impl::LogScope::depth(),\
            trace::Dbg(__BASE_FILE__, __LINE__, __FUNCTION__),\
        __VA_ARGS__); \
    } while(false)

#define TRACE_SCOPE(...) \
    trace::impl::LogScope \
        JOIN_TOKEN(log_scope_, __LINE__)(\
            trace::Dbg(__BASE_FILE__, __LINE__, __FUNCTION__), __VA_ARGS__)

#else /* TRACE_ENABLED */

#define TRACE(...)
#define TRACE_SCOPE(...)

#endif /* TRACE_ENABLED */
