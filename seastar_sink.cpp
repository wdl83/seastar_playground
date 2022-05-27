#include "trace.h"

#include <seastar/util/log.hh>

seastar::logger logger{"sharded_test"};

namespace trace {
struct SeastarSink: public Sink
{
    void log(
        const Dbg &dbg,
        const std::string &msg,
        int stackDepth,
        Hint hint) override final
    {
        logger.info(
            "{}:{} {} {}@{} {}",
            ::basename(const_cast<char *>(dbg.file)), dbg.line, dbg.function,
            stackDepth, hint, msg);
    }
    void flush() override final {}
};
std::unique_ptr<Sink> makeSink() {return std::make_unique<trace::SeastarSink>();}
} /* trace */
