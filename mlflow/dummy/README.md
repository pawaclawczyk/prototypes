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
make var/envs/<environment>
conda activate var/envs/<environment> [--stack]
```

Use `--stack` to activate one environment on top of the currently active one.
It is useful to add testing or development dependencies on top of the runtime dependencies.

```shell
make var/envs/runtime
make var/envs/development

conda activate var/envs/runtime 
conda activate var/envs/development --stack
```

Update a specific environment.

```shell
make var/envs/<environment>/update
```

## Distribute project

Create distribution environment configuration file.


```shell
make dist/envs/<environment>.yaml
```