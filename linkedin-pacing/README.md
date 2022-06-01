# Simulation of ad server workload with LinkedIn pacing algorithm

## Development

There are two types of environments: production (`prod`) and development (`dev`).
The difference is the set of dependencies being installed.
`dev` contains all `prod` dependencies, described in `requirements.txt`,
and additional ones, described in `requirements.dev.txt`.  

The `Makefile` by default uses the `dev` environment.
The environment can be overwritten with `ENV` variable,
e.g. `ENV=prod dagit` will run Dagit in the production environment.

The main targets in the `Makefile` are:

```shell
make develop  ## creates the development environment

make dagit    ## starts the Dagit web application

make clean    ## removes all runtime artifacts
```

In general, you don't have to create the environment, before running other targets, it will be created automatically.
