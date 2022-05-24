# Dummy MLFlow project

## Repository structure

```
/
|-- etc/
|      |-- envs/   ## conda environment configuraton files; the file must be named <enviornment>.yaml
|-- var/
|      |-- envs/   ## conda environments
|-- MLproject.yaml ## MLFlow project configuration file
```

## Create, update, and activate, conda environments

An environment configuration must be placed in the `etc/envs/<environment>.yaml` file. 

```shell
make var/env/<environment>
conda activate var/env/<environment> [--stack]
```

Use `--stack` to activate one environment on top of the currently active one.
It is useful to add testing or development dependencies on top of the runtime dependencies.

```shell
make var/env/runtime
make var/env/development

conda activate var/env/runtime 
conda activate var/env/development --stack
```

Update a specific environment.

```shell
make var/env/<environment>/update
```

## Distribute project

Create distribution environment configuration file.


```shell
make dist/env/<environment>.yaml
```