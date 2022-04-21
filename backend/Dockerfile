FROM bitnami/spark

USER root
RUN apt-get update && apt-get -y install ncat
RUN . /opt/bitnami/scripts/libcomponent.sh && component_unpack "python" "3.10.4-2" --checksum ccad1e4649c0f8f79eb007431d8fe96aa756bf1bc95c19feab7b2b4ecc1efac3
USER 1001
