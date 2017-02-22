from random import uniform
import sys


def generate(p, n, r):
    string = ""
    lParam = []
    for i in range(p):
        lParam.append(0.0)
    for k in range(n):
        for e in lParam:
            e = uniform(0.0, r)
            string += str(e) + ';'
        string += '\n'
    file = open("input/data_set.txt", "w")
    file.write(string)
    file.close()

if __name__ == "__main__":
    args = sys.argv
    print args
    generate(int(args[1]), int(args[2]), int(args[3]))
