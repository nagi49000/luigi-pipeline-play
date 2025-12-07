# luigi-pipeline-play
Repo for playing with data pipelines in Luigi

### Environment set up

For running on bare metal, one can set up a conda environment with a [yaml file](./python/environment.yml), by running in the terminal
```bash
conda env create --file python/environment.yml
conda activate luigi-env
```

## Luigi

### Simple set up

The pipeline can be instantiated on the command line
```bash
# in ./python/luigi-examples/src
python -m luigi --module luigi_examples.random_user_pipeline  AllSinks --local-scheduler --workdir file-outputs
```

### Scheduler set up

For running on bare metal and through a scheduler, one can instantiate the pipeline and run through a scheduler.

One can bring up a scheduler with
```
luigid --port 8082
```
and the scheduler will be available on http://localhost:8082. The pipeline can be instantiated on the command line (and dropping the --local-scheduler flag; by default the tasks will be sent to a scheduler listening on 8082/tcp) with
```bash
# in ./python/luigi-examples/src
python -m luigi --module luigi_examples.random_user_pipeline  AllSinks --workdir file-outputs
```
The resultant DAG (and success of stages) will then be available in the scheduler UI.

### Docker setup

For running in a containerized environment, one can
- configure a scheduler container for the scheduler
- configure a worker container for each, or a set, of pipelines

An example of this is in the `docker-compose.yaml`. The two containers can be brought up with
```bash
# in root of repo
docker compose up --build
```
The scheduler UI will be available on http://localhost:18082. The completed files can be viewed in the worker container by docker exec-ing into the worker container
```bash
docker exec -it luigi-pipeline-play-random-users-pipeline-1 bash
```
and looking under the folder `foo`.

## Luigi

### Simple set up

The pipeline can be instantiated on the command line
```bash
# in ./python/luigi-examples/src
python -m luigi_examples.prefect_random_user_pipeline
```

### Server set up

One can configure the job to be submitted to a server, which can then manage scheduling etc.

If a Prefect server is not running, one can start a server on any machine, e.g. your local dev machine, with
```bash
prefect server start
```

If you need to clear the prefect database (e.g. for dev), then run
```bash
prefect server database reset
```

The prefect server (by default) will be running on `localhost:4200`. Prefect operations that can use a server can pick up the server by environment variable, e.g. settig
```bash
export PREFECT_API_URL="http://localhost:4200/api"
```
This will most likely run in its own terminal, since it is a process.

Next, the server needs to know about a [work pool](https://docs.prefect.io/v3/how-to-guides/deployment_infra/manage-work-pools) that can run flows.

```bash
prefect worker start --pool local-dev-pool --type process
```
This will most likely run in its own terminal, since it is a process. Since the flow will be running on the worker, in that terminal, we can also see the output of jobs that are being run.

Now the pipeline can be submitted to the server to manage and run. More precisely, one can create a Prefect [deployment](https://docs.prefect.io/v3/how-to-guides/deployments/create-deployments) on the server, which can then submit Prefect [flows](https://docs.prefect.io/v3/concepts/flows) to the worker.
```bash
prefect deploy --cron "20 * * * *" --pool local-dev-pool --name random_users_dep --version 0.1  luigi_examples/prefect_random_user_pipeline.py:random_users_etl
```
This command may ask for some interactive [y/n] answers, and if requested can write out the deployment as a yaml, and will the exit.

### References

- [luigi github and docs](https://github.com/spotify/luigi)
- [luigi docs pages](https://luigi.readthedocs.io/en/stable/running_luigi.html)
- [luigi digitalocean example](https://www.digitalocean.com/community/tutorials/how-to-build-a-data-processing-pipeline-using-luigi-in-python-on-ubuntu-20-04)
- [prefect docs pages(https://docs.prefect.io/v3/get-started)