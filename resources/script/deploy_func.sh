# Script to spread built package to all cluster nodes

# Need to import util by the caller first
# $1: true for compute, false for fpdb-store
function deploy() {
  # input params
  is_compute=$1

  # 1. organize executables, resources and required libraries
  rm -rf "$deploy_dir"
  echo "Copying built files..."
  mkdir -p "$deploy_dir"

  # executables
  if [ "${is_compute}" = true ]; then
    exe_dir_name="$compute_exe_dir_name"
  else
    exe_dir_name="$fpdb_store_exe_dir_name"
  fi
  exe_dir="$build_dir"/"$exe_dir_name"
  exe_deploy_dir="$deploy_dir"/"$exe_dir_name"
  cp -r "$exe_dir"/ "$exe_deploy_dir"/

  # calcite, only do for compute
  if [ "${is_compute}" = true ]; then
    calcite_jar_path="$root_dir"/"$calcite_dir_name""/target/""$calcite_jar_name"
    calcite_deploy_jar_path="$deploy_dir"/"$calcite_dir_name""/target/""$calcite_jar_name"
    mkdir -p "$(dirname "${calcite_deploy_jar_path}")"
    cp -r "$calcite_jar_path" "$calcite_deploy_jar_path"
  fi

  # resources
  resource_deploy_dir="$deploy_dir""/resources/"
  mkdir -p "$(dirname "${resource_deploy_dir}")"
  cp -r "$resource_dir"/ "$resource_deploy_dir"/

  # libs
  lib_names=("aws-cpp-sdk_ep" "caf_ep" "graphviz_ep")
  lib_suffix="install/lib"
  lib_root_dir="$build_dir""/_deps"
  lib_deploy_root_dir="$deploy_dir""/libs"

  for lib_name in "${lib_names[@]}"
  do
    lib_dir="$lib_root_dir"/"$lib_name"/"$lib_suffix"
    lib_deploy_dir="$lib_deploy_root_dir"/"$lib_name"/"$lib_suffix"
    mkdir -p "$(dirname "${lib_deploy_dir}")"
    cp -r "$lib_dir"/ "$lib_deploy_dir"/
  done

  # create a temp directory
  mkdir -p "$temp_deploy_dir"

  echo -e "done\n"

  # 2. deploy organized package for each node
  echo "Sending built files to cluster nodes..."

  if [ "${is_compute}" = true ]; then
    node_ips=("${compute_ips[@]}")
  else
    node_ips=("${fpdb_store_ips[@]}")
  fi
  for node_ip in "${node_ips[@]}"
  do
    echo -n "  Sending to ""$node_ip""... "
    check_or_add_to_known_hosts "$node_ip"
    run_command "$pem_path" "$node_ip" rm -rf "$deploy_dir"
    scp -rqi "$pem_path" "$deploy_dir"/ ubuntu@"$node_ip":"$deploy_dir"/
    echo "  done"
  done

  echo "done"
}
