# A static binary on an empty image. x86 builds target AVX2 (x86-64-v3) tuned
# for the scorer's Cascade Lake Xeons; arm64 builds (local testing) use the
# build machine's CPU.
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ make && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Makefile ./
COPY src ./src
RUN set -eux; \
    case "$(uname -m)" in \
      x86_64) arch="-march=x86-64-v3 -mtune=cascadelake" ;; \
      *) arch="-mcpu=native" ;; \
    esac; \
    make CXX=g++ ARCHFLAGS="$arch" CXXFLAGS="-O3 -DNDEBUG -flto=auto -fno-plt" LDFLAGS="-static"; \
    strip shopping-sessions

FROM scratch
COPY --from=build /src/shopping-sessions /shopping-sessions
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output
ENTRYPOINT ["/shopping-sessions"]
