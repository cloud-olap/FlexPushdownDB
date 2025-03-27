# script to run a set of test_cases
# currently we cannot run an entire test suite (unknown bugs still occasionally), so we have to run each test case
# with a set of brand-new opened cluster servers (i.e. restart servers for each test case)

# need to adjust the exec accordingly here (e.g., "fpdb-main-test", "fpdb-main-bench")
test_exe="fpdb-main-bench"
tests_file="test_cases_tpch"

pushd "$(dirname "$0")" > /dev/null

# read tests
tests=()
while IFS= read -r line || [[ -n "$line" ]];
do
  tests+=("$line")
done < "$tests_file"

# import util
cd ..
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"

# run tests
cd "$deploy_dir"/"$compute_exe_dir_name"
for test in "${tests[@]}"
do
  # start the system
  ../resources/script/start.sh
  sleep 1

  # run
  ./"$test_exe" -tc="$test"

  # stop the system
  ../resources/script/stop.sh
  sleep 2
done

popd > /dev/null