# Script to spread built package to all fpdb-store cluster nodes

# import
pushd "$(dirname "$0")" > /dev/null
cd ..
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"
util_func_path=$(pwd)"/util_func.sh"
source "$util_func_path"
deploy_func_path=$(pwd)"/deploy_func.sh"
source "$deploy_func_path"
popd > /dev/null

# call deploy()
deploy false
