# script to run a set of tests in a test suite
# servers in the cluster nodes are started only once, so this is helpful when bug-free is ensured

# configurable parameters
test_exe="fpdb-main-bench"
test_suite="no-pred-trans-tpch-sf100-dist"

pushd "$(dirname "$0")" > /dev/null

# import util
cd ..
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"

# start the system
cd "$deploy_dir"
./resources/script/start.sh
sleep 1

# run test suite
cd "$compute_exe_dir_name"
./"$test_exe" -ts="$test_suite"

# stop the system
cd "$deploy_dir"
./resources/script/stop.sh

popd > /dev/null