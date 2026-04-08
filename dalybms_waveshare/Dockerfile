ARG BUILD_FROM
FROM ${BUILD_FROM}

ENV LANG=C.UTF-8

RUN apk add --no-cache python3 py3-pip jq && \
    python3 -m pip install --no-cache-dir --break-system-packages paho-mqtt==1.6.1

COPY monitor.py /monitor.py
COPY rootfs /
RUN chmod a+x /etc/services.d/dalybms/run
