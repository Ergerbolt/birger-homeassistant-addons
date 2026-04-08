ARG BUILD_FROM
FROM $BUILD_FROM

ENV LANG C.UTF-8

COPY requirements.txt /
RUN apk add --no-cache python3 py3-pip jq && \
    python3 -m pip install --no-cache-dir -r /requirements.txt

COPY monitor.py /
COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
