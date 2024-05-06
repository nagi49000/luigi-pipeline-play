FROM python:3.12

ENV APP_HOME=/home/app

COPY requirements.txt .
RUN pip install -r requirements.txt && \
    mkdir ${APP_HOME} && \
    groupadd --system app && \
    useradd -g app --system app && \
    chown -R app:app ${APP_HOME} && \
    chmod -R 755 ${APP_HOME}

USER app
WORKDIR ${APP_HOME}

COPY --chown=app:app src src
COPY --chown=app:app test_luigi_examples test_luigi_examples
RUN python -m pytest --cov=./src

WORKDIR ${APP_HOME}/src

CMD ["python", "-m", "luigi", "--module", "luigi_examples.random_user_pipeline", "AllSinks", "--workdir", "foo"]
