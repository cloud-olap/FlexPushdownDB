# stop server on the current node

# Need to import util by the caller first
# $1: true for compute, false for fpdb-store
function stop_node() {
  # input params
  is_compute=$1

  # stop server
  if [ "${is_compute}" = true ]; then
    server_pid_name="$compute_server_pid_name"
  else
    server_pid_name="$fpdb_store_server_pid_name"
  fi
  server_pid_path="$temp_deploy_dir"/"$server_pid_name"
  if [ -e "$server_pid_path" ]; then
    kill -9 "$(cat "$server_pid_path")"
    rm "$server_pid_path"
  fi
}
