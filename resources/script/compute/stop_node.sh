# stop server on the current compute node

# import
pushd "$(dirname "$0")" > /dev/null
cd ..
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"
util_func_path=$(pwd)"/util_func.sh"
source "$util_func_path"
stop_node_func_path=$(pwd)"/stop_node_func.sh"
source "$stop_node_func_path"
popd > /dev/null

# call stop_node()
stop_node true
