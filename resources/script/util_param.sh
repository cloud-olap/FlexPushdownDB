# parameters used by all system scripts
# need to cd into the parent directory before importing this

# configurable parameters
export install_dependency=false
export clean=false
export build_parallel=8
export build_dir_name="build"
export deploy_dir_name="FPDB-build"
export temp_dir_name="temp"
export pem_path="$HOME""/.aws/yifei-cloudbank-aws.pem"
export use_fpdb_store=true

# fixed parameters
export compute_targets=("fpdb-main-server" "fpdb-main-test" "fpdb-main-bench")
export compute_exe_dir_name="fpdb-main"
export compute_server_pid_name="FPDB-server.pid"
export fpdb_store_targets=("fpdb-store-server-executable")
export fpdb_store_exe_dir_name="fpdb-store-server"
export fpdb_store_server_pid_name="FPDB-store-server.pid"
export calcite_jar_name="flexpushdowndb.thrift.calcite-1.0-SNAPSHOT.jar"
export calcite_dir_name="fpdb-calcite/java"
export calcite_pid_name="calcite-server.pid"

# directories
script_dir=$(pwd)
resource_dir="$(dirname "${script_dir}")"
root_dir="$(dirname "${resource_dir}")"
build_dir="${root_dir}"/"${build_dir_name}"
deploy_dir=$HOME/$deploy_dir_name
temp_deploy_dir="$deploy_dir"/"$temp_dir_name"
export script_dir resource_dir root_dir build_dir deploy_dir temp_deploy_dir

# ips for compute cluster (coordinator excluded)
this_ip="$(curl -s ifconfig.me)"
compute_ips_path="${resource_dir}""/config/cluster_ips"
while IFS= read -r line || [[ -n "$line" ]];
do
  if [ "$line" != "$this_ip" ]; then
    compute_ips+=("$line")
  fi
done < "$compute_ips_path"
export compute_ips

# ips for fpdb-store cluster
fpdb_store_ips_path="${resource_dir}""/config/fpdb-store_ips"
while IFS= read -r line || [[ -n "$line" ]];
do
  fpdb_store_ips+=("$line")
done < "$fpdb_store_ips_path"
export fpdb_store_ips
