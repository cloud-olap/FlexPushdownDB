from enum import Enum

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class AWSRegion(Enum):
    """
    Listing aws regions
    """
    Any = "Any"
    US_EAST_1 = 'us-east-1'
    US_EAST_2 = 'us-east-2'
    US_WEST_1 = 'us-west-1'
    US_WEST_2 = 'us-west-2'
    EU_CENTRAL_1 = 'eu-central-1'
    CANADA_CENTRAL_1 = 'ca-central-1'
    Default = US_EAST_1

    @staticmethod
    def get_aws_region(region_name):
        return


class EC2InstanceOS(Enum):
    """
    Listing EC2 OS type for different instances
    """
    Any = "Any"
    Linux = "Linux/UNIX"
    RedHat = "Red Hat Enterprise Linux"
    SUSE = "SUSE Linux Enterprise Server"
    Windows = "Windows"


class EC2InstanceType(Enum):
    """
    listing of EC2 instance types
    """
    not_ec2 = 'not_ec2'
    c42xlarge = 'c4.2xlarge'
    c44xlarge = 'c4.4xlarge'
    c48xlarge = 'c4.8xlarge'
    c4large = 'c4.large'
    c4xlarge = 'c4.xlarge'
    c518xlarge = 'c5.18xlarge'
    c52xlarge = 'c5.2xlarge'
    c54xlarge = 'c5.4xlarge'
    c59xlarge = 'c5.9xlarge'
    c5large = 'c5.large'
    c5xlarge = 'c5.xlarge'
    c5d18xlarge = 'c5d.18xlarge'
    c5d2xlarge = 'c5d.2xlarge'
    c5d4xlarge = 'c5d.4xlarge'
    c5d9xlarge = 'c5d.9xlarge'
    c5dlarge = 'c5d.large'
    c5dxlarge = 'c5d.xlarge'
    d22xlarge = 'd2.2xlarge'
    d24xlarge = 'd2.4xlarge'
    d28xlarge = 'd2.8xlarge'
    d2xlarge = 'd2.xlarge'
    g316xlarge = 'g3.16xlarge'
    g34xlarge = 'g3.4xlarge'
    g38xlarge = 'g3.8xlarge'
    h116xlarge = 'h1.16xlarge'
    h12xlarge = 'h1.2xlarge'
    h14xlarge = 'h1.4xlarge'
    h18xlarge = 'h1.8xlarge'
    i316xlarge = 'i3.16xlarge'
    i32xlarge = 'i3.2xlarge'
    i34xlarge = 'i3.4xlarge'
    i38xlarge = 'i3.8xlarge'
    i3large = 'i3.large'
    i3metal = 'i3.metal'
    i3xlarge = 'i3.xlarge'
    m410xlarge = 'm4.10xlarge'
    m416xlarge = 'm4.16xlarge'
    m42xlarge = 'm4.2xlarge'
    m44xlarge = 'm4.4xlarge'
    m4large = 'm4.large'
    m4xlarge = 'm4.xlarge'
    m512xlarge = 'm5.12xlarge'
    m524xlarge = 'm5.24xlarge'
    m52xlarge = 'm5.2xlarge'
    m54xlarge = 'm5.4xlarge'
    m5large = 'm5.large'
    m5xlarge = 'm5.xlarge'
    m5d12xlarge = 'm5d.12xlarge'
    m5d24xlarge = 'm5d.24xlarge'
    m5d2xlarge = 'm5d.2xlarge'
    m5d4xlarge = 'm5d.4xlarge'
    m5dlarge = 'm5d.large'
    m5dxlarge = 'm5d.xlarge'
    p216xlarge = 'p2.16xlarge'
    p28xlarge = 'p2.8xlarge'
    p2xlarge = 'p2.xlarge'
    p316xlarge = 'p3.16xlarge'
    p32xlarge = 'p3.2xlarge'
    p38xlarge = 'p3.8xlarge'
    r416xlarge = 'r4.16xlarge'
    r42xlarge = 'r4.2xlarge'
    r44xlarge = 'r4.4xlarge'
    r48xlarge = 'r4.8xlarge'
    r4large = 'r4.large'
    r4xlarge = 'r4.xlarge'
    r512xlarge = 'r5.12xlarge'
    r524xlarge = 'r5.24xlarge'
    r52xlarge = 'r5.2xlarge'
    r54xlarge = 'r5.4xlarge'
    r5large = 'r5.large'
    r5xlarge = 'r5.xlarge'
    r5d12xlarge = 'r5d.12xlarge'
    r5d24xlarge = 'r5d.24xlarge'
    r5d2xlarge = 'r5d.2xlarge'
    r5d4xlarge = 'r5d.4xlarge'
    r5dlarge = 'r5d.large'
    r5dxlarge = 'r5d.xlarge'
    t22xlarge = 't2.2xlarge'
    t2large = 't2.large'
    t2medium = 't2.medium'
    t2micro = 't2.micro'
    t2nano = 't2.nano'
    t2small = 't2.small'
    t2xlarge = 't2.xlarge'
    x116xlarge = 'x1.16xlarge'
    x132xlarge = 'x1.32xlarge'