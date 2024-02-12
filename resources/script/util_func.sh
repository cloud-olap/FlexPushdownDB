# common functions
# catch ctrl_c
trap ctrl_c INT
function ctrl_c() {
  echo "*** Trapped CTRL-C, exit"
  popd > /dev/null
  exit 0
}

# check if ip is added to ~/.ssh/known_hosts, if not, add first
function check_or_add_to_known_hosts() {
  ip=$1
  SSHKey=$(ssh-keygen -F "$ip")
  if [ -z "$SSHKey" ]; then
    SSHKey=$(ssh-keyscan -H "$ip" 2> /dev/null)
    echo "$SSHKey" >> ~/.ssh/known_hosts
  fi
}

# run command remotely
function run_command() {
  pem_path=$1
  ip=$2
  shift 2
  ssh -i "$pem_path" ubuntu@"$ip" "$@"
}