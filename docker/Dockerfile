ARG TARGETARCH
FROM ovishpc/store-test:main-${TARGETARCH}

RUN <<EOF
set -e
set -x
cd /root/sos
git fetch origin
git checkout -b main origin/main
./autogen.sh
rm -rf build
mkdir -p build
pushd build
../configure --prefix /opt/ovis CFLAGS=-ggdb3
make && make install
EOF


RUN <<EOF
set -e
set -x
cd /root
git clone https://github.com/ovis-hpc/monbench
EOF

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
