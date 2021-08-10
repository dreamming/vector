FROM rust:latest as cargo-build
WORKDIR /usr/src/myapp
COPY . .
RUN cargo build --release
RUN cargo install --path .

FROM alpine:latest
COPY --from=cargo-build /usr/local/cargo/bin/myvector /usr/local/bin/myapp
CMD ["myapp"]