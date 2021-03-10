import os
import time
import subprocess
import sys

metrics_file = 'math_model_metrics'
oneMinuteInSeconds = 60


def expVary_h():
    os.system('rm math_model_metrics')
    # make parameters
    r_c_pairs = [[0.1, 5], [0.5, 5], [0.5, 10], [0.8, 10], [0.9, 15], [0.9, 17]]
    network = 0
    hs = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

    parameter_sets = []
    for r_c_pair in r_c_pairs:
        parameter_set = []
        for h in hs:
            parameter_set.append({'h': h, 'r': r_c_pair[0], 'nCol': r_c_pair[1], 'network': network})
        parameter_sets.append(parameter_set)

    # execute
    for (r_c_pair, parameter_set) in zip(r_c_pairs, parameter_sets):
        for param in parameter_set:
            with open('math_model_metrics', 'a') as file:
                file.write('\nh:' + str(param['h']) + ' r:' + str(param['r']) + ' nCol:' + str(param['nCol']) + '\n')
            os.system('./normal-ssb-query-generate-file 5 ' + \
                      str(param['h']) + ' ' + str(param['r']) + ' ' + str(param['nCol']))
            # os.system('./normal-ssb-experiment -m ' + str(param['network']))
            while True:
                command = ["./normal-ssb-experiment", '-m', str(param['network'])]
                print("Running: " + " ".join(command))
                # p = subprocess.Popen('./normal-ssb-experiment -m ' + str(param['network']))
                p = subprocess.Popen(command)
                success = True
                try:
                    p.wait(oneMinuteInSeconds)
                except subprocess.TimeoutExpired:
                    p.kill()
                    time.sleep(5)
                    print('retry')
                    success = False
                if success: break
        os.system('mv math_model_metrics math_model_metrics_vary-h_' + 'r-' + \
                  str(r_c_pair[0]) + '_nCol-' + str(r_c_pair[1]) + '_network-' + str(network))


def expVary_network():
    os.system('rm math_model_metrics')
    # make parameters
    r_c_pairs = [[0.1, 5], [0.5, 10], [0.9, 17]]
    networks = [25, 22.5, 20, 17.5, 15, 12.5, 10, 7.5, 5]
    hs = [0.2, 0.5, 0.8]

    parameter_sets = []
    for r_c_pair in r_c_pairs:
        for h in hs:
            parameter_set = []
            for network in networks:
                parameter_set.append({'h': h, 'r': r_c_pair[0], 'nCol': r_c_pair[1], 'network': network})
            parameter_sets.append(parameter_set)

    # execute
    for parameter_set in parameter_sets:
        for param in parameter_set:
            with open('math_model_metrics', 'a') as file:
                file.write('\nh:' + str(param['h']) + ' r:' + str(param['r']) + ' nCol:' + str(param['nCol']) + '\n')
            os.system('./normal-ssb-query-generate-file 5 ' + \
                      str(param['h']) + ' ' + str(param['r']) + ' ' + str(param['nCol']))
            # os.system('./normal-ssb-experiment -m ' + str(param['network']))
            while True:
                command = ["./normal-ssb-experiment", '-m', str(param['network'])]
                print("Running: " + " ".join(command))
                # p = subprocess.Popen('./normal-ssb-experiment -m ' + str(param['network']))
                p = subprocess.Popen(command)
                success = True
                try:
                    p.wait(oneMinuteInSeconds * (25 / param['network']))
                except subprocess.TimeoutExpired:
                    p.kill()
                    time.sleep(5)
                    print('retry')
                    success = False
                if success: break
        h = parameter_set[0]['h']
        r = parameter_set[0]['r']
        nCol = parameter_set[0]['nCol']
        os.system('mv math_model_metrics math_model_metrics_vary-network_' + 'h-' + str(h) \
                  + '_r-' + str(r) + '_nCol-' + str(nCol))


if len(sys.argv) < 2:
    exit("Lack parameter")

if sys.argv[1] == '-h':
    expVary_h()
elif sys.argv[1] == '-n':
    expVary_network()
else:
    exit("Bad parameter: " + sys.argv[1])
