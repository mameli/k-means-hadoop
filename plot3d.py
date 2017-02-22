import importlib
importlib.import_module('mpl_toolkits.mplot3d').__path__
import matplotlib.pyplot as plt
import numpy as np
import os
from mpl_toolkits.mplot3d import axes3d


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
            z = []
            cluster = []
            xCluster = []
            yCluster = []
            zCluster = []
            for el in file_content:
                cluster.append(int(el[0]))
                xCluster.append(float(el[1]))
                yCluster.append(float(el[2]))
                zCluster.append(float(el[3]))
                x.append(float(el[4]))
                y.append(float(el[5]))
                z.append(float(el[6]))
            print("Punti: " + str(len(x)))
            colors = np.random.rand(50)

            fig = plt.figure()
            ax = fig.add_subplot(111, projection='3d')
            ax.scatter(x, y, z, c=cluster)
            ax.scatter(xCluster, yCluster, zCluster, s=50, c=cluster, alpha=0.7, marker='*')

            ax.set_xlabel('X Label')
            ax.set_ylabel('Y Label')
            ax.set_zlabel('Z Label')

            plt.show()
