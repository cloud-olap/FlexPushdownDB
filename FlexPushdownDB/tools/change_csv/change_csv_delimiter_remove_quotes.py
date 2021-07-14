import csv
import multiprocessing as mp
import math


def transform(oldPath, newPath):
    writer = csv.writer(open(newPath, "wt"), quoting=csv.QUOTE_NONE, delimiter='|', lineterminator='\n')
    reader = csv.reader(open(oldPath, "rt"), delimiter=',')
    # next(reader, None)
    writer.writerows(reader)


def transformThread(pathVec):
    for pathPair in pathVec:
        transform(pathPair[0], pathPair[1])
        print("Transformed:", pathPair[0], pathPair[1])


def parallelTransform(pathVec, degree):
    threads = []
    for i in range(0, len(pathVec), math.floor(len(pathVec) / degree)):
        j = i + math.floor(len(pathVec) / degree)
        if j > len(pathVec):
            j = len(pathVec)
        t = mp.Process(target=transformThread, args=(pathVec[i: j],))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()


def transformForPrestoLineorder():
    years = [1992, 1993, 1994, 1995, 1996, 1997, 1998]
    starts = [0, 60, 121, 182, 242, 303, 364]
    ends = [60, 121, 182, 242, 303, 364, 399]
    pathVec = []

    for i in range(len(years)):
        year = years[i]
        for shardId in range(starts[i], ends[i] + 1):
            oldPath = "lineorder/year=" + str(year) + "/lineorder.tbl." + str(shardId)
            newPath = "lineorder1/year=" + str(year) + "/lineorder.tbl." + str(shardId)
            pathVec.append([oldPath, newPath])

    parallelTransform(pathVec, 32)


def transformForFPDB():
    pathVec = [["csv_150MB_initial_format/supplier.tbl", "csv_150MB/supplier.tbl"],
               ["csv_150MB_initial_format/date.tbl", "csv_150MB/date.tbl"],
               ["csv_150MB_initial_format/part.tbl", "csv_150MB/part.tbl"],
               ["csv_150MB_initial_format/customer.tbl", "csv_150MB/customer.tbl"]]
    for i in range(400):
        pathVec.append(["csv_150MB_initial_format/lineorder_sharded/lineorder.tbl." + str(i), \
                        "csv_150MB/lineorder_sharded/lineorder.tbl." + str(i)])
    parallelTransform(pathVec, 32)


transformForFPDB()
