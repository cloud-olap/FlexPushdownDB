# start the system on all fpdb-store cluster nodes

# import
pushd "$(dirname "$0")" > /dev/null
cd ..
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"
util_func_path=$(pwd)"/util_func.sh"
source "$util_func_path"
popd > /dev/null

# start server on each fpdb-store node
echo "Starting fpdb-store-server on cluster nodes..."

for node_ip in "${fpdb_store_ips[@]}"
do
  echo -n "  Starting ""$node_ip""... "
  check_or_add_to_known_hosts "$node_ip"
  run_command "$pem_path" "$node_ip" "$deploy_dir""/resources/script/fpdb-store/start_node.sh"
  echo "  done"
done

echo "done"
