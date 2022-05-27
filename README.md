# Building seastar GCC 12

```console
./configure.py \
        --c++-standard=20 \
        --mode=debug \
        --without-tests \
        --compiler=/usr/bin/g++-12 \
        --cflags="-Wno-use-after-free -Wno-uninitialized -Wno-stringop-overread"
```

## classic.cpp

Implements synchronous big file (RAM limit with -m) lexicographical order block sorting

```console
# build release with traces
RELEASE=1 make -f classic.Makefile
# run
./obj/classic.elf -i 16Mx4K -o output  -m 8192 -b 4096
```

## file_xread.cpp
```console
# debug build
BUILD=debug SANITIZE=1 make -f file_xread.Makefile
# run
./obj_debug/file_xread.elf  --input 64x4K --n 8 --shards 4

# release build
BUILD=release  make -f file_xread.Makefile
# run
./obj_release/file_xread.elf  --input 64x4K --n 8 --shards 4
```

## sharded_file_xread.cpp (parallel file read)

```console
# release build
BUILD=release  make -f sharded_file_xread.Makefile
# run  with map_reduce
./obj_release/sharded_file_xread.elf  --input 64x4K --n 8 --shards 4 --mr 1
# run with invoke_on_all
./obj_release/sharded_file_xread.elf  --input 64x4K --n 8 --shards 4 --mr 0
```

## Generating random text files (aligned to specified boundary)

```console
# 100kB aligned to 4kB
SIZE_kB=100 \
        SIZE=$(((( $SIZE_kB * 1024 / 4096 ) ) * 4096 )); \
        base64 /dev/urandom | \
        head -c $SIZE > output.txt
```

## Debugging with ASAN

If you would like to use gdb with ASAN please define
```console
export ASAN_OPTIONS=abort_on_error=1
```
otherwise ASAN generates report and program exit, so gdb can not break on ASAN error.
