# start the entire system, both compute and storage (if needed)

# import util
pushd "$(dirname "$0")" > /dev/null
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"
util_func_path=$(pwd)"/util_func.sh"
source "$util_func_path"

# stop the system first in case not yet
./stop.sh
printf "\n"

# storage
if [ "${use_fpdb_store}" = true ]; then
  echo "[Start FPDB store]"
  ./fpdb-store/start.sh
  printf "\n"
fi

# compute
echo "[Start compute layer]"
./compute/start.sh
popd > /dev/null
