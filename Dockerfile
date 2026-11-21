FROM python:3.11-slim-bookworm AS deps
RUN apt-get update && apt-get install -y --no-install-recommends g++ make && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyarrow==18.1.0

FROM deps AS build
WORKDIR /build
COPY Makefile .
COPY src src
RUN make && mkdir /runtime && cp /usr/local/lib/python3.11/site-packages/pyarrow/libarrow.so.1801 /usr/local/lib/python3.11/site-packages/pyarrow/libparquet.so.1801 /runtime/

FROM debian:bookworm-slim
COPY --from=build /runtime/ /usr/local/lib/
COPY --from=build /usr/local/lib/python3.11/site-packages/pyarrow-18.1.0.dist-info/LICENSE.txt /usr/share/doc/apache-arrow/LICENSE.txt
COPY --from=build /usr/local/lib/python3.11/site-packages/pyarrow-18.1.0.dist-info/NOTICE.txt /usr/share/doc/apache-arrow/NOTICE.txt
RUN ldconfig
COPY --from=build /build/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]
