FROM openjdk:8
MAINTAINER Terry Horner <community@keen.io>

# Copy in the artifact
COPY target/universal/capillary-*.tgz /capillary/

# Extract and link the artifact
WORKDIR /capillary
RUN tar zxvf *.tgz
RUN rm -f *.tgz
RUN ln -s `ls` current

# Included container configuration
EXPOSE 8086
ENTRYPOINT ["/capillary/current/bin/capillary"]
CMD []
