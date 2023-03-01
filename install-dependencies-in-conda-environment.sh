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

conda install -c conda-forge openjdk==11.0.15=h1e1ecb3_1 maven==3.8.6=ha770c72_0 libffi==3.4.2=h7f98852_5 -y \
  || >&2 echo "Something wrong happened when dependencies where being installed..."

# CLD2
if [[ ! -d "cld2_for_heritrix3" ]]; then
  git clone https://github.com/CLD2Owners/cld2.git "cld2_for_heritrix3"
  pushd cld2_for_heritrix3/internal/
  CFLAGS="-Wno-narrowing -O3" ./compile_and_test_all.sh
  cp -f ./libcld2* "$CONDA_PREFIX/lib"
  popd
fi

# CLD2 Java wrapper
if [[ ! -d "language-detection-cld2" ]]; then
  git clone https://github.com/commoncrawl/language-detection-cld2
  pushd language-detection-cld2
  mvn install
  popd
fi
