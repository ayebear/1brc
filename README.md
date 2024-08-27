# 1brc

Fast solution to [1brc](https://www.morling.dev/blog/one-billion-row-challenge/) in rust. Currently runs in ~1.22s on AMD Ryzen 9 5950X.

Also see an [even simpler (but slower) solution using a parallel iterator chain with rayon](https://github.com/ayebear/1brc-rayon).

## To run

Put `measurements.txt` in working directory.

```
time cargo run -r
```
