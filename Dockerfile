FROM ubuntu:22.10 as base

RUN apt-get update && apt-get install -y cmake g++ libasan6 libcurl4-openssl-dev libssl-dev uuid-dev git pkg-config
WORKDIR cbdp
COPY *.cpp *.h CMakeLists.txt .
COPY cmake ./cmake
WORKDIR cmake-build-debug
RUN cmake .. && make
ENV CBDP_PORT 4242

FROM base as coordinator

ENV BUCKET_CNT 10
CMD exec ./coordinator https://db.in.tum.de/teaching/ws2223/clouddataprocessing/data/filelist.csv "$CBDP_PORT" "$BUCKET_CNT"

FROM base as worker

ENV CBDP_COORDINATOR coordinator
CMD echo "worker $CBDP_COORDINATOR $CBDP_PORT" && exec ./worker "$CBDP_COORDINATOR" "$CBDP_PORT"