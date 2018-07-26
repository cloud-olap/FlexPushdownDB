# import boto3
#
# s3 = boto3.client('s3')
#
# for table in ['customer.csv', 'lineitem.csv', 'nation.csv', 'orders.csv', 'part.csv', 'partsupp.csv', 'region.csv',
#               'supplier.csv']:
#     print(table)
#     r = s3.select_object_content(
#         Bucket='s3filter',
#         Key=table,  # 'customer.csv',
#         ExpressionType='SQL',
#         Expression="select * from s3object s limit 1;",
#         # Expression="select * from s3object s where CAST(c_custkey AS int) < 7;",
#         # Expression="select count(*) from s3object s where CAST(c_custkey AS int) < 7;",
#         InputSerialization={'CSV': {"FileHeaderInfo": "Use", "FieldDelimiter": "|"}},
#         OutputSerialization={'CSV': {}},
#     )
#     for event in r['Payload']:
#         if 'Records' in event:
#             records = event['Records']['Payload']  #:.decode('utf-8')
#             print(records)
#         elif 'Stats' in event:
#             statsDetails = event['Stats']['Details']
#             print("Stats details bytesScanned: ")
