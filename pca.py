import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
from sklearn.decomposition import PCA


def plot(file_name, n, method):
    input_exp = 'inputs/' + file_name + '.txt'
    input_clusters = 'outputs/{}_{}.txt'.format(file_name, method)
    # input_clusters = 'outputs/cho_dbscan.txt'
    title = '{}_{}.txt'.format(file_name, method)

    X = np.loadtxt(input_exp)[:, 2:]

    y = np.loadtxt(input_clusters)[:, 1]

    n_components = min(X.shape[0], X.shape[1])

    clusters = range(1, n + 1)
    names = ['Cluster ' + str(i) for i in clusters]
    colors = iter(cm.rainbow(np.linspace(0, 1, len(clusters))))

    pca = PCA(n_components=n_components)
    X_r = pca.fit(X).transform(X)

    plt.figure()
    for c, i, target_name in zip(colors, clusters, names):
        plt.scatter(X_r[y == i, 0], X_r[y == i, 1], c=c, label=target_name)

    plt.legend()
    plt.title('PCA of ' + title)
    plt.show()


plot('new_data_1', 3, 'kmeans')
