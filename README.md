# luigi-pipeline-play
Repo for playing with data pipelines in Luigi

### Simple set up

For running on bare metal, one can set up a conda environment with a [yaml file](./python/environment.yml), by running in the terminal
```bash
conda env create --file python/environment.yml
conda activate luigi-env
```

the pipeline can then be instantiated on the command line
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
and the scheduler will be available on http://localhost:8082. The pipeline can be instantiated on the command line (and dropping the --local-scheduler flag) with
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

### References

- [github and docs](https://github.com/spotify/luigi)
- [docs pages](https://luigi.readthedocs.io/en/stable/running_luigi.html)
- [digitalocean example](https://www.digitalocean.com/community/tutorials/how-to-build-a-data-processing-pipeline-using-luigi-in-python-on-ubuntu-20-04)