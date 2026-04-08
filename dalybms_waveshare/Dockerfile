ARG BUILD_FROM
FROM ${BUILD_FROM}

ENV LANG=C.UTF-8

COPY requirements.txt /
RUN apk add --no-cache python3 py3-pip jq && \
    python3 -m pip install --no-cache-dir --break-system-packages -r /requirements.txt

COPY run.sh /
COPY monitor.py /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
