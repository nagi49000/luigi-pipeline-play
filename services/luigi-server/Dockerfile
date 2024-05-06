FROM python:3.12

ENV APP_HOME=/home/app

RUN pip install luigi==3.5.0 && \
    mkdir ${APP_HOME} && \
    groupadd --system app && \
    useradd -g app --system app && \
    chown -R app:app ${APP_HOME} && \
    chmod -R 755 ${APP_HOME}

USER app
WORKDIR ${APP_HOME}

CMD ["luigid", "--port", "8082"]