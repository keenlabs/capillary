FROM openjdk:8
MAINTAINER Terry Horner <community@keen.io>

WORKDIR /capillary

# Copy in the artifact
COPY target/universal/capillary-*.tgz .

# Extract and link the artifact
RUN tar zxvf *.tgz && \
    rm -f *.tgz && \
    ln -s `ls` current

# Included container configuration
EXPOSE 8086
ENTRYPOINT ["/capillary/current/bin/capillary"]
CMD []
