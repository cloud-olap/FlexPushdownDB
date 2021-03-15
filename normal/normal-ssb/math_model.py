import os
import time
import subprocess
import sys

metrics_file = 'math_model_metrics'
repeatTimes = 2
oneMinuteInSeconds = 60
oldNumLines, newNumLines = 0, 0


def num_file_lines(filePath):
    res = subprocess.run(['wc', '-l', filePath], stdout=subprocess.PIPE)
    output = res.stdout.decode('utf-8')
    strs = output.split()
    return int(strs[0])


def expVary_h():
    global oldNumLines, newNumLines

    os.system('rm math_model_metrics')
    # make parameters
    r_c_pairs = [[0.1, 5], [0.5, 5], [0.5, 10], [0.9, 10], [0.9, 17]]
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
            while True:
                command = ["./normal-ssb-experiment", '-m', str(param['network']), str(repeatTimes)]
                print("Running: " + " ".join(command))
                p = subprocess.Popen(command)
                # catch timeout
                timeout = False
                try:
                    p.wait(oneMinuteInSeconds * repeatTimes * 2)
                except subprocess.TimeoutExpired:
                    p.kill()
                    time.sleep(5)
                    timeout = True
                # catch crash during execution
                newNumLines = num_file_lines(metrics_file)
                print("!!!! OLDNUMLINES:", oldNumLines)
                print("!!!! NEWNUMLINES:", newNumLines)
                # if newNumLines - oldNumLines == 10 and not timeout:
                if not timeout:
                    break
                else:
                    print('retry')
            oldNumLines = newNumLines
        os.system('mv math_model_metrics math_model_metrics_vary-h_' + 'r-' + \
                  str(r_c_pair[0]) + '_nCol-' + str(r_c_pair[1]) + '_network-' + str(network))


def expVary_network():
    global oldNumLines, newNumLines

    os.system('rm math_model_metrics')
    # make parameters
    r_c_pairs = [[0.1, 5], [0.5, 10], [0.9, 17]]
    networks = [25, 22.5, 20, 17.5, 15, 12.5, 10]
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
            while True:
                command = ["./normal-ssb-experiment", '-m', str(param['network']), str(repeatTimes)]
                print("Running: " + " ".join(command))
                p = subprocess.Popen(command)
                # catch timeout
                timeout = False
                try:
                    p.wait(oneMinuteInSeconds * (25 / param['network']) * repeatTimes * 2)
                except subprocess.TimeoutExpired:
                    p.kill()
                    time.sleep(5)
                    timeout = True
                # catch crash during execution
                newNumLines = num_file_lines(metrics_file)
                print("!!!! OLDNUMLINES:", oldNumLines)
                print("!!!! NEWNUMLINES:", newNumLines)
                # if newNumLines - oldNumLines == 10 and not timeout:
                if not timeout:
                    break
                else:
                    print('retry')
        h = parameter_set[0]['h']
        r = parameter_set[0]['r']
        nCol = parameter_set[0]['nCol']
        os.system('mv math_model_metrics math_model_metrics_vary-network_' + 'h-' + str(h) \
                  + '_r-' + str(r) + '_nCol-' + str(nCol))


if len(sys.argv) < 2:
    exit("Lack parameter, at least 1")

if sys.argv[1] == '-h':
    expVary_h()
elif sys.argv[1] == '-n':
    expVary_network()
else:
    print(num_file_lines("math_model.py"))
    exit("Bad parameter: " + sys.argv[1])

os.system("sudo shutdown -h now")
