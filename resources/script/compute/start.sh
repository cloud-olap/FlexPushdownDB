# start the system on all compute cluster nodes

# import
pushd "$(dirname "$0")" > /dev/null
cd ..
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"
util_func_path=$(pwd)"/util_func.sh"
source "$util_func_path"
popd > /dev/null

# start calcite server on master
echo "Starting calcite server on master node..."

calcite_jar_path="$deploy_dir"/"$calcite_dir_name""/target/""$calcite_jar_name"
calcite_pid_path="$temp_deploy_dir"/"$calcite_pid_name"
java -jar "$calcite_jar_path" &
echo $! > "$calcite_pid_path"

echo "done"

# start server on each slave node
echo "Starting server on slave nodes..."

for node_ip in "${compute_ips[@]}"
do
  echo -n "  Starting ""$node_ip""... "
  check_or_add_to_known_hosts "$node_ip"
  run_command "$pem_path" "$node_ip" "$deploy_dir""/resources/script/compute/start_node.sh"
  echo "  done"
done

echo "done"
