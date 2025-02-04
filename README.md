![CMake workflow](
https://github.com/lasanthafdo/enjima/actions/workflows/cmake-single-platform.yml/badge.svg)
# Enjima
Efficient stream-aware data stream processing framework written in C++
## Building Enjima

### Installing pre-requisites

The build system uses CMake. In order to build this repository, you need C++20 or later and CMake 3.16 or later. It has currently being tested with only GCC 10.5 on Ubuntu. Install the required dependencies using the following command

```
sudo apt install build-essential gcc-10 g++-10 cmake
```

If there are multiple gcc versions installed, make sure gcc and g++ are configured to point to g++-10 or later. E.g. if both gcc/g++ 9 and 10 are installed, you can use update-alternatives as follows:

```
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 9
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-9 9
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 10
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-10 10
```

### Clone the repository

In order to clone the repository, use

```
git clone git@github.com:lasanthafdo/enjima-library.git
cd enjima-library
```

### Build using CMake

Follow the steps below to build Enjima in debug mode:

```
mkdir cmake-build-debug && cd cmake-build-debug
cmake -DCMAKE_BUILD_TYPE=Debug ..
cmake --build . -j 12
```

Follow the steps below to build Enjima in release mode:

```
mkdir cmake-build-release && cd cmake-build-release
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j 12
```
This will produce the binary for the static library as `libEnjimaRelease.a` under the directory `cmake-build-release`. 
Please copy and statically link this binary with your application. 

### Additional Notes

This library uses the following third-party dependencies.

| Name                                | Type                                         | URL                                           | Version / Commit ID | Included As        |
|-------------------------------------|----------------------------------------------|-----------------------------------------------|---------------------|--------------------|
| UXL oneTBB (formerly Intel TBB)     | Collection of concurrent data structures     | https://github.com/oneapi-src/oneTBB.git      | v2021.12.0          | CMake FetchContent |
| MPMCQueue                           | Multi-producer, multi-consumer bounded queue | https://github.com/rigtorp/MPMCQueue          | b9808ed             | Copied to source   |
| Xenium                              | Collection of concurrent data structures     | https://github.com/mpoeter/xenium             | e48ddbe             | Copied to source   |
| emhash7                             | Flat Hash Map / Table                        | https://github.com/ktprime/emhash             | 945a1b8             | Copied to source   |
| moodycamel::ConcurrentQueue         | Concurrent Queue                             | https://github.com/cameron314/concurrentqueue | v1.0.4              | Copied to source   |
| ankerl::unordered_dense::{map, set} | Unordered Dense Map / Set                    | https://github.com/martinus/unordered_dense   | v4.4.0              | Copied to source   |
| yaml-cpp                            | YAML Parser                                  | https://github.com/jbeder/yaml-cpp.git        | 0.8.0               | CMake FetchContent |


