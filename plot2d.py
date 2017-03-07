import matplotlib.pyplot as plt
import numpy as np
import os
import math
from matplotlib.backends.backend_pdf import PdfPages
import re


def returnColor(parameter):
    value = int(parameter)
    colors = ['blue', 'yellow', 'red', 'cyan', 'magenta',
              'green', 'black', 'white', 'pink', 'brown']
    return colors[value % len(colors)]


if __name__ == "__main__":
    for file in os.listdir("output"):
        if(file == "part-r-00000"):
            print(file)
            file_content = open("output/" + file, "r")
            file_content = file_content.readlines()
            trimmed_file = []
            for index, item in enumerate(file_content):
                file_content[index] = item.replace('\t', '').rstrip(';\n')
            for index, item in enumerate(file_content):
                file_content[index] = item.split(';')
            x = []
            y = []
            cluster = []
            xCluster = []
            yCluster = []
            for el in file_content:
                cluster.append(float(el[0]) + 999)
                xCluster.append(float(el[1]))
                yCluster.append(float(el[2]))
                x.append(float(el[3]))
                y.append(float(el[4]))
            print("Punti: " + str(len(x)))
            colors = np.random.rand(50)
            plt.scatter(x, y, s=50, c=cluster, alpha=0.5)
            plt.scatter(xCluster, yCluster, s=70, c=cluster, alpha=0.1, marker='*')
            plt.show()
