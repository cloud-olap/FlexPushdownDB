# start server on the current fpdb-store node

# import
pushd "$(dirname "$0")" > /dev/null
cd ..
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"
util_func_path=$(pwd)"/util_func.sh"
source "$util_func_path"
start_node_func_path=$(pwd)"/start_node_func.sh"
source "$start_node_func_path"
popd > /dev/null

# call start_node()
start_node false
