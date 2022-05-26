# Building seastar GCC-12

```console
./configure.py --c++-standard=20 --mode=debug   --without-tests --compiler=/usr/bin/g++-12 --cflags="-Wno-use-after-free -Wno-uninitialized -Wno-stringop-overread"
```

## classic.cpp
Implements synchronous big file (RAM limit with -m) lexicographical order block sorting
