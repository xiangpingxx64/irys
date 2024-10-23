# irys

You need submodules yo.

```cli
git submodule update --init --recursive --remote
```

minimum rustc version 1.82

```cli
rustc --version
rustup update
```

if you're stuck on a previous version of rustc try
```cli
 rustup default 1.82.0
 ```

other deps:
```
clang & a C/C++ build toolchain
gmp
pkg-config
```

Testing Block Serialization
```cli
cargo test -p irys-types  -- --nocapture
```