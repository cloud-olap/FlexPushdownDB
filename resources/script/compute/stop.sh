# stop the system on all compute cluster nodes

# import
pushd "$(dirname "$0")" > /dev/null
cd ..
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"
util_func_path=$(pwd)"/util_func.sh"
source "$util_func_path"
popd > /dev/null

# stop calcite server on master
echo "Stopping calcite server on master node..."

calcite_pid_path="$temp_deploy_dir"/"$calcite_pid_name"
if [ -e "$calcite_pid_path" ]; then
	kill -9 "$(cat "$calcite_pid_path")"
	rm "$calcite_pid_path"
fi

echo "done"

# stop server on each slave node
echo "Stopping server on slave nodes..."

for node_ip in "${compute_ips[@]}"
do
  echo -n "  Stopping ""$node_ip""... "
  check_or_add_to_known_hosts "$node_ip"
  run_command "$pem_path" "$node_ip" "$deploy_dir""/resources/script/compute/stop_node.sh"
  echo "  done"
done

echo "done"
