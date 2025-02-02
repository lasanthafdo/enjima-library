![CMake workflow](
https://github.com/lasanthafdo/enjima/actions/workflows/cmake-single-platform.yml/badge.svg)
# Enjima
Efficient adaptive data stream processing framework written in C++
## Building Enjima

### Installing pre-requisites

The build system uses CMake. In order to build this repository, you need C++20 or later and CMake 3.22 or later. It has currently being tested with only GCC (>= 10.5) on Ubuntu. Install the required dependencies using the following command

```
sudo apt install build-essential gcc-10 g++-10
sudo snap install cmake --classic
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
git clone git@github.com:lasanthafdo/enjima.git
cd enjima
```

### Build using CMake

Follow the steps below to build Enjima in debug mode:

```
mkdir cmake-build-debug && cd cmake-build-debug
cmake -DCMAKE_BUILD_TYPE=Debug ..
cmake --build . -j 12
```





