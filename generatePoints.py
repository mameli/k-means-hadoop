from random import uniform
import sys


def generate(p, n, r, i):
    string = ""
    lParam = []
    print "input/data_set_" + str(i) + ".txt"
    file = open("input/data_set_" + str(i) + ".txt", "w")
    for i in range(p):
        lParam.append(0.0)
    for k in range(n):
        for e in lParam:
            e = uniform(-r, r)
            string += str(e) + ';'
        string += '\n'
    file.write(string)
    file.close()


if __name__ == "__main__":
    args = sys.argv
    print args
    numFiles = int(args[2]) / 10000
    if numFiles == 0:
        numFiles = 1
    print numFiles
    print int(args[2]) / numFiles
    for i in range(numFiles):
        generate(int(args[1]), int(args[2]) / numFiles, int(args[3]), i)
