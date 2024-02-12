# script to prepare data in storage nodes, when a new instance is started
# including mounting the nvme-ssd storage and downloading data from S3
# need aws credentials already set

# input parameters
data_relative_dirs=("$@")       # can take multiple dirs, e.g. ("tpch-sf0.01" "tpch-sf10)

# configurable parameters
bucket="flexpushdowndb"
mount_point_prefix="/fpdb-store"
drive_names=("/dev/nvme1n1" "/dev/nvme2n1")     # for r5d.4x, this should correspond to "NUM_DRIVES" in
                                                # "fpdb-store.conf" correctly
temp_dir="$HOME""/""temp"

# mount nvme-ssd to the mount point and set access
for ((i=0; i<${#drive_names[@]}; i++));
do
  drive_name=${drive_names[i]}
  mount_point="$mount_point_prefix""-""$i"

  sudo umount "$drive_name"
  sudo mkdir -p "$mount_point"

  sudo mkfs.ext4 -E nodiscard "$drive_name"
  sudo mount "$drive_name" "$mount_point"

  bucket_dir="$mount_point""/""$bucket"
  sudo mkdir -p "$bucket_dir"
  sudo chown -R ubuntu:ubuntu "$mount_point"
done

# download data from S3
for data_relative_dir in "${data_relative_dirs[@]}"
do
  aws s3 cp --recursive "s3://""$bucket""/""$data_relative_dir" "$temp_dir""/""$data_relative_dir"
  for ((i=0; i<${#drive_names[@]}; i++));
  do
    mount_point="$mount_point_prefix""-""$i"
    bucket_dir="$mount_point""/""$bucket"
    mkdir -p "$(dirname "$bucket_dir""/""$data_relative_dir")"
    cp -rf "$temp_dir""/""$data_relative_dir" "$bucket_dir""/""$data_relative_dir"
  done
  rm -rf "$temp_dir"
done
