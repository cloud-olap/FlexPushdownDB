from s3filter.util.constants import *
from collections import defaultdict
from s3filter.plan.cost_estimator_enum import EC2InstanceType, EC2InstanceOS, AWSRegion
import os
import multiprocessing
import warnings

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class EC2Instance:
    """
    EC2 instance information
    """
    ec2_instances = defaultdict(lambda: defaultdict(dict))
    local_machine = None

    @staticmethod
    def load_instances_info():
        if len(EC2Instance.ec2_instances) > 0:
            return

        proj_dir = os.environ['PYTHONPATH'].split(":")[0]
        resources_dir = os.path.join(proj_dir, 'resources')
        instances_price_path = os.path.join(resources_dir, 'ec2_pricing.csv')

        from psutil import virtual_memory

        EC2Instance.ec2_instances[AWSRegion.NOT_AWS][EC2InstanceOS.Any][EC2InstanceType.not_ec2] = \
            EC2Instance.local_machine = \
            EC2Instance(
                region=AWSRegion.NOT_AWS,
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

            all_regions = AWSRegion.get_all_regions()

            for line in ec2_info_file:
                if first_line:
                    first_line = False
                    continue
                else:
                    for region in all_regions:
                        lc = line.split(',')
                        os_type = lc[0].strip()
                        category = lc[1].strip()
                        name = lc[2].strip()
                        cpus = lc[3].strip()
                        memory = lc[4].strip()
                        storage = lc[5].strip()
                        storage_type = lc[6].strip()
                        price = float(lc[7].strip())

                        EC2Instance.ec2_instances[region][os_type][name] = EC2Instance(
                            region=region,
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
    def get_instance_info(os_type=EC2InstanceOS.Any, ins_type=EC2InstanceType.r48xlarge,
                          region=AWSRegion.NOT_AWS):
        # EC2 instances info lazy loading
        EC2Instance.load_instances_info()

        if region == AWSRegion.NOT_AWS:
            os_type = EC2InstanceOS.Any
            ins_type = EC2InstanceType.not_ec2

        if region in EC2Instance.ec2_instances and \
                os_type in EC2Instance.ec2_instances[region] and \
                ins_type in EC2Instance.ec2_instances[region][os_type]:
            return EC2Instance.ec2_instances[region][os_type][ins_type]
        else:
            warnings.warn('Could not find a match for EC2 instance. You are probably running from local machine',
                          RuntimeWarning)
            return EC2Instance.local_machine


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
    DATA_TRANSFER_PRICE_OTHER_REGION_PER_GB = 0.02

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
        region = AWSRegion.NOT_AWS

    import boto3
    client = boto3.client('s3')
    try:
        response = client.get_bucket_location(Bucket=S3_BUCKET_NAME)
        s3_region = response['LocationConstraint']
        if s3_region is None:
            s3_region = AWSRegion.ANY
    except Exception as e:
        s3_region = AWSRegion.NOT_AWS

    import platform
    pltfrm = platform.system()
    if pltfrm == 'Linux':
        dist = platform.linux_distribution()[0].lower()
        if "RedHat".lower() in dist or "Red Hat".lower() in dist:
            os_type = EC2InstanceOS.RedHat
        elif "SUSE".lower() in dist:
            os_type = EC2InstanceOS.SUSE
        else:
            os_type = EC2InstanceOS.Linux
    elif pltfrm == "Windows":
        os_type = EC2InstanceOS.Windows
    else:
        os_type = EC2InstanceOS.Any

    def __init__(self, table_scan_metrics):
        print('creating cost estimator with region {}, instance type {} and os type {}'.format(CostEstimator.region,
                                                                                               CostEstimator.instance_type,
                                                                                               CostEstimator.os_type))
        self.ec2_instance = EC2Instance.get_instance_info(CostEstimator.os_type, CostEstimator.instance_type,
                                                          CostEstimator.region)
        self.s3_region = CostEstimator.s3_region
        self.table_scan_metrics = table_scan_metrics

    def estimate_cost(self):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated cost of the table scan operation
        """

        return self.estimate_computation_cost() + \
               self.estimate_data_cost() + \
               self.estimate_request_cost()

    def estimate_data_cost(self, ec2_region=None, s3_region=None):
        """
        Estimate the cost of: S3 data scan and return + the data transfer cost if exists
        :return: the estimated data handling cost
        """
        if ec2_region is None:
            ec2_region = self.ec2_instance.region

        if s3_region is None:
            s3_region = self.s3_region

        data_transfer_cost = 0
        # In case the computation instance is not located at the same region as the s3 data region or
        # data is transferred outside aws to the internet
        if ec2_region == AWSRegion.NOT_AWS:
            data_transfer_cost = self.table_scan_metrics.bytes_returned * BYTE_TO_GB * \
                                 CostEstimator.DATA_TRANSFER_PRICE_PER_GB
        # data moved within aws services but to another region
        elif s3_region != AWSRegion.ANY and s3_region != ec2_region:
            data_transfer_cost = self.table_scan_metrics.bytes_returned * BYTE_TO_GB * \
                                 CostEstimator.DATA_TRANSFER_PRICE_OTHER_REGION_PER_GB

        s3_select_cost = self.table_scan_metrics.bytes_returned * BYTE_TO_GB * \
                         CostEstimator.COST_S3_DATA_RETURNED_PER_GB + \
                         self.table_scan_metrics.bytes_scanned * BYTE_TO_GB * CostEstimator.COST_S3_DATA_SCANNED_PER_GB

        return data_transfer_cost + s3_select_cost

    def estimate_request_cost(self):
        """
        Estimate the cost of the http GET requests
        :return: the estimated http GET request cost for this particular operation
        """
        return self.table_scan_metrics.num_http_get_requests * CostEstimator.REQUEST_PRICE

    def estimate_computation_cost(self, running_time=None, ec2_instance_type=None, os_type=None):
        """
        Estimate the cost of this operation to run on an EC2 machine
        :return: the estimated computation cost for this operation based on AWS EC2 cost model
        """
        if running_time is None:
            running_time = self.table_scan_metrics.elapsed_time()

        ec2_instance = self.ec2_instance
        if ec2_instance_type is not None and os_type is not None:
            ec2_instance = EC2Instance.get_instance_info(os_type, ec2_instance_type, self.ec2_instance.region)

        return running_time * SEC_TO_HOUR * ec2_instance.price

    def estimate_cost_for_config(self, ec2_region=AWSRegion.DEFAULT, s3_region=AWSRegion.DEFAULT):
        """
        Estimates the hypothetical cost given ec2 instance region and s3 region of the scan operation based on
        S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated cost of the table scan operation
        """

        return self.estimate_computation_cost() + \
               self.estimate_data_cost(ec2_region, s3_region) + \
               self.estimate_request_cost()
