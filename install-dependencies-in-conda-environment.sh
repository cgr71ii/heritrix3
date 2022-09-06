#/bin/bash

if [[ -z "$CONDA_EXE" ]]; then
  >&2 echo "Conda seems not to be installed: CONDA_EXE envvar is not defined"

  exit 1
fi

if [[ -z "$CONDA_DEFAULT_ENV" ]]; then
  >&2 echo "No conda environment activated"

  exit 1
fi

if [[ "$CONDA_DEFAULT_ENV" == "base" ]]; then
  >&2 echo "WARNING: 'base' conda environment activated: this is not adviced: waiting 10 seconds before proceed"

  sleep 10
fi

echo "Installing dependencies: $CONDA_DEFAULT_ENV"
sleep 2

dependencies=$(conda list | grep -v "^#" | wc -l)

if [[ "$dependencies" != "0" ]]; then
  >&2 echo "There are dependencies installed ($dependencies): new dependencies are going to be installed what might break the environment: waiting 10 seconds before proceed"

  sleep 10
fi

conda install -c conda-forge openjdk=11 maven -y

if [[ "$?" != "0" ]]; then
  >&2 echo "Something wrong happened when dependencies where being installed..."
fi
