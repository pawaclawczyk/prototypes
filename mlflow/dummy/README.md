# Dummy MLFlow project

## Repository structure

```
/
|-- dummy/                       # source code directory
|-- etc/
|      |-- conda.yaml            ## conda production environment configuraton
|      |-- requirements.txt      ## production environment PyPI requirements file use by the `conda.yaml`
|      |-- requirements.dev.txt  ## development environment PyPI dependecies
|-- var/                         
|      |-- envs/                 ## conda environments:
|              |-- build/        ## - build environment
|              |-- prod/         ## - work environment (also used as development environment)
|
|-- conda.yaml                   ## conda envirnment configiration file used by the `MLproject` file
|                                ## this file is generated automatically
|
|-- MLproject                    ## mlflow project configuration file
|-- Makefile                     ## automated codebase operations
```

## Working with the environments

```shell
make conda.yaml
```

The work environment relies on the `conda.yaml` environment configuration file.
It is the same configuration as the one used by the `mlflow` project.
The file depends on the `etc/conda.yaml` and `etc/requirements.txt` which describe the main project dependencies.
The file is created directly with Makefile, or as the dependency of other targets.
The file generation happens in an isolated environment,
so it is not affected to the changes done in the work environment.

```shell
make install
make install/dev
```

The installation targets create the work environment.
The production dependencies are defined in the auto-generated `conda.yaml`.
The development dependencies are defined in the `etc/requirements.dev.txt`.
The installation of the development dependencies adds new packages to the existing environment.
It does not create separate environment.

To add a new dependency or change the version of existing one,
modify the configuration file in the `etc/` directory.
After that call the desired `Makefile` target,
all the dependent target affected by the change will be executed.

```shell
conda activate var/envs/prod
```

Now, you can activate the work environment in your shell with `conda activate`,
or use it in your IDE.
The paths are assumed convention and can be overwritten.
For more details review the `Makefile`.

```shell
make conda.yaml/update
```

It forces to update of the dependencies in the `conda.yaml` configuration file.
As the target only updates the `conda.yaml` file,
You should also re-create the work environment and run the tests in the project.

## Working with the `mlflow`

```shell
make mlflow/run
```

Running the project with `mlflow` requires the `conda.yaml` configuration file
and the work environment with production dependencies.
The `Makefile` target will build all the dependencies if not existing or changed.

```shell
make mlflow/ui
```

Running the `mlfow` UI requires the development dependencies to be installed.
The `Makefile` target will build all the dependencies if not existing or changed.

## Remaining utility targets and general notes

```shell
make test
```

The `test` target will run all the test suites in the project.

The `clean` target will delete all the files and directories created by the runtime or for its purpose.

The targets define their dependencies, so the "end-targets" can be used without bothering about the right order
of target calls during a manual setup process.

> Q: Why `conda.yaml` is generated in an isolated environment?

The `conda.yaml` is used as dependency lock file, and must contain only main and derived production dependencies.
It should not contain development dependencies, or manually installed packages with `conda install` or `pip install`.
The work environment is created from the `conda.yaml` configuration, so it represents the same environment
as the one created internally by the `mlflow`.

> Q: Why the work environment is patched with the development dependencies?
> Q: Why conda environment stacking is not used to overlay development dependencies?

The was no reason do separate work environments for production and development one,
because it is usually just a development environment,
unless the case does not require to install development dependencies.

It does not use conda environment stacking (`conda activate <higher-level-env> --stack`),
because the "higher-level" environment would contain only the development dependencies.
So, it will not be useful in some IDEs.
