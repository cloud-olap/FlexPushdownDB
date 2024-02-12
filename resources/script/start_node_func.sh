# start server on the current node

# Need to import util by the caller first
# $1: true for compute, false for fpdb-store
function start_node() {
  # input params
  is_compute=$1

  # export lib paths to LD_LIBRARY_PATH
  lib_deploy_dir="$deploy_dir""/libs"
  export LD_LIBRARY_PATH="$lib_deploy_dir"/aws-cpp-sdk_ep/install/lib:"$lib_deploy_dir"/caf_ep/install/lib:\
"$lib_deploy_dir"/graphviz_ep/install/lib:"$lib_deploy_dir"/graphviz_ep/install/lib/graphviz

  # start server
  if [ "${is_compute}" = true ]; then
    exe_dir_name="$compute_exe_dir_name"
    server_exe_name="fpdb-main-server"
    server_pid_name="$compute_server_pid_name"
  else
    exe_dir_name="$fpdb_store_exe_dir_name"
    server_exe_name="fpdb-store-server-executable"
    server_pid_name="$fpdb_store_server_pid_name"
  fi
  server_deploy_dir="$deploy_dir"/"$exe_dir_name"
  server_pid_path="$temp_deploy_dir"/"$server_pid_name"

  pushd "$(dirname "$0")" > /dev/null
  cd "$server_deploy_dir"
  nohup "./""$server_exe_name" >/dev/null 2>&1 &
  echo $! > "$server_pid_path"
  popd > /dev/null
}
