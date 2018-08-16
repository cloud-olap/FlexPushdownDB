from s3filter.util.constants import *
from collections import defaultdict
from s3filter.plan.cost_estimator_enum import EC2InstanceType, EC2InstanceOS, AWSRegion
import os
import multiprocessing

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class EC2Instance:
    """
    EC2 instance information
    """
    ec2_instances = defaultdict(lambda: defaultdict(dict))

    @staticmethod
    def load_instances_info():
        if len(EC2Instance.ec2_instances) > 0:
            return

        proj_dir = os.environ['PYTHONPATH'].split(":")[0]
        resources_dir = os.path.join(proj_dir, 'resources')
        instances_price_path = os.path.join(resources_dir, 'ec2_pricing.csv')

        from psutil import virtual_memory

        EC2Instance.ec2_instances[AWSRegion.Any][EC2InstanceOS.Any][EC2InstanceType.not_ec2] = \
            EC2Instance(
                region=AWSRegion.Any,
                os_type=EC2InstanceOS.Any,
                category="General Purpose",
                name=EC2InstanceType.not_ec2,
                cpus=multiprocessing.cpu_count(),
                memory=virtual_memory().total * BYTE_TO_GB,
                storage="",
                storage_type="",
                price=0.0)

        with open(instances_price_path, 'r') as ec2_info_file:
            first_line = True

            for line in ec2_info_file:
                if first_line:
                    first_line = False
                    continue
                else:
                    lc = line.split(',')
                    os_type = lc[0].strip()
                    category = lc[1].strip()
                    name = lc[2].strip()
                    cpus = lc[3].strip()
                    memory = lc[4].strip()
                    storage = lc[5].strip()
                    storage_type = lc[6].strip()
                    price = float(lc[7].strip())

                    EC2Instance.ec2_instances[AWSRegion.Default][os_type][name] = EC2Instance(
                                                                           region=AWSRegion.Default,
                                                                           os_type=os_type,
                                                                           category=category,
                                                                           name=name,
                                                                           cpus=cpus,
                                                                           memory=memory,
                                                                           storage=storage,
                                                                           storage_type=storage_type,
                                                                           price=price)

    def __init__(self, region, os_type, category, name, cpus, memory, storage, storage_type, price):
        self.region = region
        self.os_type = os_type
        self.category = category
        self.instance_name = name
        self.cpus = cpus
        self.memory = memory
        self.storage = storage
        self.storage_type = storage_type
        self.price = price

    def __repr__(self):
        return {
            'region': self.region,
            'os_type': self.os_type,
            'category': self.category,
            'instance_name': self.instance_name,
            'cpus': self.cpus,
            'memory': self.memory,
            'storage': self.storage,
            'storage_type': self.storage_type,
            'price': "${} per Hour".format(self.price)
        }.__repr__()

    @staticmethod
    def get_instance_info(os_type=EC2InstanceOS.Any, name=EC2InstanceType.r48xlarge,
                          region=AWSRegion.Any):
        # EC2 instances info lazy loading
        EC2Instance.load_instances_info()

        if region in EC2Instance.ec2_instances and \
                os_type in EC2Instance.ec2_instances[region] and \
                name in EC2Instance.ec2_instances[region][os_type]:
            return EC2Instance.ec2_instances[region][os_type][name]
        else:
            raise Exception('Invalid info for EC2 instance {}, {}, {}'.format(region, os_type, name))


class CostEstimator:
    """
    Estimate operation cost based on aws S3 & EC2 pricing pages
    S3: https://aws.amazon.com/s3/pricing/
    EC2: https://aws.amazon.com/ec2/pricing/on-demand/
    """

    # These amounts vary by region, but for simplicity, let's assume it's a flat rate
    COST_S3_DATA_RETURNED_PER_GB = 0.0007
    COST_S3_DATA_SCANNED_PER_GB = 0.002
    REQUEST_PRICE = 0.0004 / 1000.0
    DATA_TRANSFER_PRICE_PER_GB = 0.09

    def __init__(self, table_scan_metrics):
        import requests
        try:
            r = requests.get('http://169.254.169.254/latest/meta-data/instance-type', verify=False, timeout=1)
            if r.status_code == 200:
                instance_type = r.text
            else:
                instance_type = EC2InstanceType.not_ec2

            r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document", verify=False,
                             timeout=1)
            response_json = r.json()
            region = response_json.get('region')
        except requests.ConnectionError:
            instance_type = EC2InstanceType.not_ec2

        import platform
        pltfrm = platform.system()
        if pltfrm == 'Linux':
            dist = platform.linux_distribution()
            if "RedHat".lower() in dist or "Red Hat".lower() in dist:
                os_type = EC2InstanceOS.RedHat
            elif "SUSE".lower() in dist:
                os_type = EC2InstanceOS.SUSE
            else:
                os_type = EC2InstanceOS.Linux
        elif pltfrm == "Windows":
            os_type = EC2InstanceOS.SUSE
        else:
            os_type = EC2InstanceOS.Any

        self.ec2_instance = EC2Instance.get_instance_info(os_type, instance_type, AWSRegion.Default)#region)
        self.table_scan_metrics = table_scan_metrics

    def estimate_cost(self):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated cost of the table scan operation
        """

        computation_cost = 0#self.table_scan_metrics.elapsed_time() * SEC_TO_HOUR * self.ec2_instance.price
        data_transfer_cost = 0
        # Assuming the data transfer cost is charged only in case the data is going out to the internet not to an EC2
        # instance
        if self.ec2_instance.region != AWSRegion.Default:
            data_transfer_cost = self.table_scan_metrics.bytes_returned * BYTE_TO_GB * \
                                 CostEstimator.DATA_TRANSFER_PRICE_PER_GB
        s3_select_cost = self.table_scan_metrics.bytes_returned * BYTE_TO_GB * \
                         CostEstimator.COST_S3_DATA_RETURNED_PER_GB + \
                         self.table_scan_metrics.bytes_scanned * BYTE_TO_GB * CostEstimator.COST_S3_DATA_SCANNED_PER_GB
        request_cost = self.table_scan_metrics.num_http_get_requests * CostEstimator.REQUEST_PRICE

        return computation_cost + data_transfer_cost + s3_select_cost + request_cost
