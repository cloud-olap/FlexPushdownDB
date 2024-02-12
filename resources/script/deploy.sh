# Script to spread built package to all nodes (both compute and storage, if needed)

# import util
pushd "$(dirname "$0")" > /dev/null
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"
util_func_path=$(pwd)"/util_func.sh"
source "$util_func_path"

# storage
if [ "${use_fpdb_store}" = true ]; then
  echo "[Deploy FPDB store]"
  ./fpdb-store/deploy.sh
  printf "\n"
fi

# compute
echo "[Deploy compute layer]"
./compute/deploy.sh
popd > /dev/null
