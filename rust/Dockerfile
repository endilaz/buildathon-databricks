# A static binary on an empty image, like the C++ entry's. x86 builds target
# AVX2 (x86-64-v3) for the scorer's Xeons; arm64 builds (local testing) use
# the build machine's CPU. No dependencies, so the build needs no network.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY src ./src
RUN set -eux; \
    case "$(uname -m)" in \
      x86_64) cpu="-C target-cpu=x86-64-v3" ;; \
      *) cpu="-C target-cpu=native" ;; \
    esac; \
    triple="$(rustc -vV | sed -n 's/^host: //p')"; \
    RUSTFLAGS="$cpu -C target-feature=+crt-static" cargo build --release --offline --target "$triple"; \
    cp "target/$triple/release/shopping-sessions" /shopping-sessions

FROM scratch
COPY --from=build /shopping-sessions /shopping-sessions
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output
ENTRYPOINT ["/shopping-sessions"]
