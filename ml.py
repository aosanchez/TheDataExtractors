# Author:   Jay Huang <askjayhuang@gmail.com>
# Created:  2017-11-12T01:52:51.908Z

"""A module for machine learning."""

################################################################################
# Imports
################################################################################

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from yellowbrick.cluster import KElbowVisualizer
from mpl_toolkits.mplot3d import Axes3D
import seaborn as sns
import time
import pickle
from tqdm import tqdm

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import Normalizer, StandardScaler, MinMaxScaler
from sklearn.decomposition import PCA, FastICA
from sklearn.cluster import KMeans, AffinityPropagation, SpectralClustering, AgglomerativeClustering, DBSCAN, Birch, MeanShift
from sklearn.mixture import GaussianMixture
from sklearn.manifold import TSNE
from sklearn import metrics
from sklearn.metrics import silhouette_samples, silhouette_score, calinski_harabaz_score

################################################################################
# Functions
################################################################################


def gmm_final(df, nc, n_components):
    """GMM clustering on PCA-reduced data with silhouette scores."""

    df_tr = StandardScaler().fit_transform(df)

    # Reduce features via PCA
    pca = PCA(n_components=n_components).fit(df_tr)
    reduced_data = pca.transform(df_tr)

    # reduced_data = FastICA(n_components=n_components).fit_transform(df_tr)

    model = GaussianMixture(n_components=nc, covariance_type='diag')
    model.fit(reduced_data)
    cluster_labels = model.predict(reduced_data)
    dfq['Cluster Labels'] = cluster_labels


def gmm(df, nc, n_components=2):
    """GMM clustering on PCA-reduced data with silhouette scores."""

    print('GMM clustering on PCA-reduced (' + str(n_components) + ' components) ' + 'data')

    # Standardize data
    df_tr = StandardScaler().fit_transform(df)

    # Plot explained variance ratio graph
    pca = PCA().fit(df_tr)
    plt.plot(np.cumsum(pca.explained_variance_ratio_) * 100)
    plt.xlabel('Number of components')
    plt.ylabel('Cumulative explained variance in %')
    plt.title('Cumulative Explained Variance')
    plt.show()

    # Reduce features via PCA
    pca = PCA(n_components=n_components).fit(df_tr)
    reduced_data = pca.transform(df_tr)

    # Plot PCA components composition
    plt.matshow(pca.components_, cmap='viridis')
    plt.yticks([0, 1], ["First component", "Second component"])
    plt.colorbar()
    plt.xticks(range(len(df.columns)),
               df.columns, rotation=60, ha='left')
    plt.xlabel("Features")
    plt.ylabel("Principal components")
    plt.show()

    # covariances = ['full', 'tied', 'diag', 'spherical']
    covariances = ['diag']

    for covariance in covariances:
        print('\nCovariance type: ' + covariance)

        silhouette_scores = []

        range_n_clusters = range(2, 50)
        for n_clusters in range_n_clusters:
            # Initialize the clusterer
            model = GaussianMixture(n_components=n_clusters, covariance_type=covariance)
            model.fit(reduced_data)
            cluster_labels = model.predict(reduced_data)
            probs = model.predict_proba(reduced_data)

            # The silhouette_score gives the average value for all the samples.
            silhouette_avg = silhouette_score(reduced_data, cluster_labels)
            print("For n_clusters =", n_clusters,
                  "The average silhouette_score is :", silhouette_avg)

            silhouette_scores.append(silhouette_avg)

            if n_clusters == nc:
                # df['Cluster Labels'] = cluster_labels
                if n_components == 2 or n_components == 3:
                    vis(model, reduced_data, cluster_labels, probs,
                        n_components, n_clusters, silhouette_avg)

    # Plot elbow graph
    plt.plot(range_n_clusters, silhouette_scores, 'bx-')
    plt.xlabel('Number of clusters')
    plt.ylabel('Silhouette score')
    plt.title('The Elbow Method')
    plt.show()


def kmeans(df, n_components=2, v=False):
    """K-Means clustering on PCA-reduced data with silhouette scores."""

    print('K-Means clustering on PCA-reduced (' + str(n_components) + ' components) ' + 'data')

    # Standardize data
    df_tr = StandardScaler().fit_transform(df)

    # Plot explained variance ratio graph
    pca = PCA().fit(df_tr)
    plt.plot(np.cumsum(pca.explained_variance_ratio_))
    plt.xlabel('Number of components')
    plt.ylabel('Cumulative explained variance')
    plt.show()

    # Reduce features via PCA
    pca = PCA(n_components=n_components).fit(df_tr)
    reduced_data = pca.transform(df_tr)

    # Plot PCA components composition
    plt.matshow(pca.components_, cmap='viridis')
    plt.yticks([0, 1, 2, 3, 4], ["First component", "Second component",
                                 "Third component", "Fourth component", "Fifth component"])
    plt.colorbar()
    plt.xticks(range(len(df.columns)),
               df.columns, rotation=60, ha='left')
    plt.xlabel("Feature")
    plt.ylabel("Principal components")
    plt.show()

    silhouette_scores = []

    range_n_clusters = range(2, 76)
    for n_clusters in range_n_clusters:
        # Initialize the clusterer
        model = KMeans(n_clusters=n_clusters)
        model.fit(reduced_data)
        cluster_labels = model.predict(reduced_data)

        # The silhouette_score gives the average value for all the samples.
        silhouette_avg = silhouette_score(reduced_data, cluster_labels)
        print("For n_clusters =", n_clusters,
              "The average silhouette_score is :", silhouette_avg)

        silhouette_scores.append(silhouette_avg)

        if n_clusters == 20:
            dfq['Cluster Labels'] = cluster_labels
            vis(model, reduced_data, cluster_labels, n_components, n_clusters, silhouette_avg)

    # Plot elbow graph
    plt.plot(range_n_clusters, silhouette_scores, 'bx-')
    plt.xlabel('Number of Clusters')
    plt.ylabel('Silhouette Score')
    plt.title('The Elbow Method showing the optimal number of clusters for ' +
              str(n_components) + ' n_components')
    plt.show()


def kmeans_elbow(df):
    """Display a 3-D graph showing silheouette score vs number of components vs number of clusters."""
    # Standardize data
    df_tr = StandardScaler().fit_transform(df)

    # Plot explained variance ratio graph
    pca = PCA().fit(df_tr)
    plt.plot(np.cumsum(pca.explained_variance_ratio_))
    plt.xlabel('Number of components')
    plt.ylabel('Cumulative explained variance')
    plt.show()

    # Initialize elbow graph
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')

    # Initalize range of n_components
    range_n_components = range(2, 15)
    silhouette_scores_all = []

    # Iterate through range of n_components
    for n_components in tqdm(range_n_components):
        # Reduce features via PCA
        pca = PCA(n_components=n_components).fit(df_tr)
        reduced_data = pca.transform(df_tr)

        # Initalize silhouette scores array for a specific n_component
        silhouette_scores = []

        # Iterate through range of n_clusters
        range_n_clusters = range(2, 40)
        for n_clusters in tqdm(range_n_clusters):
            # Initialize clusterer and fit/predict data
            model = KMeans(n_clusters=n_clusters)
            model.fit(reduced_data)
            cluster_labels = model.predict(reduced_data)

            # The silhouette_score gives the average value for all the samples.
            silhouette_avg = silhouette_score(reduced_data, cluster_labels)

            # Append silhouette score
            silhouette_scores.append(silhouette_avg)

        silhouette_scores_all.append(silhouette_scores)
        xs = range_n_clusters
        ys = silhouette_scores
        zs = n_components
        ax.plot(xs, ys, zs, 'bx-')

    ax.set_xlabel('Number of Clusters')
    ax.set_ylabel('Silhouette Score')
    ax.set_zlabel('N_Components in PCA Reduction')
    plt.title('The Elbow Method Showing the Optimal Number of Clusters and N_Components in PDA Reduction')
    plt.show()


def vis(model, reduced_data, cluster_labels, probs, n_components, n_clusters, silhouette_avg):
    """Visualize PCA reduced data."""
    sns.set()

    # Create a subplot based on n_components
    if n_components == 2:
        fig, (ax1, ax2) = plt.subplots(1, 2)
    elif n_components == 3:
        fig = plt.figure()
        ax1 = fig.add_subplot(121, sharex=None, sharey=None)
        ax2 = fig.add_subplot(122, sharex=None, sharey=None, projection='3d')

    # The 1st subplot is the silhouette plot
    # The silhouette coefficient can range from -1, 1 but in this example all
    # lie within [-0.1, 1]
    ax1.set_xlim([-0.1, 1])
    # The (n_clusters+1)*10 is for inserting blank space between silhouette
    # plots of individual clusters, to demarcate them clearly.
    ax1.set_ylim([0, len(reduced_data) + (n_clusters + 1) * 10])

    # Compute the silhouette scores for each sample
    sample_silhouette_values = silhouette_samples(reduced_data, cluster_labels)

    y_lower = 10
    for i in range(n_clusters):
        # Aggregate the silhouette scores for samples belonging to
        # cluster i, and sort them
        ith_cluster_silhouette_values = sample_silhouette_values[cluster_labels == i]

        ith_cluster_silhouette_values.sort()

        size_cluster_i = ith_cluster_silhouette_values.shape[0]
        y_upper = y_lower + size_cluster_i

        color = cm.spectral(float(i) / n_clusters)
        ax1.fill_betweenx(np.arange(y_lower, y_upper),
                          0, ith_cluster_silhouette_values,
                          facecolor=color, edgecolor=color, alpha=0.7)

        # Label the silhouette plots with their cluster numbers at the middle
        ax1.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))

        # Compute the new y_lower for next plot
        y_lower = y_upper + 10  # 10 for the 0 samples

    ax1.set_title("The Silhouette Plot for the Various Clusters.")
    ax1.set_xlabel("The silhouette coefficient values")
    ax1.set_ylabel("Cluster label")

    # The vertical line for average silhouette score of all the values
    ax1.axvline(x=silhouette_avg, color="red", linestyle="--")

    ax1.set_yticks([])  # Clear the yaxis labels / ticks
    ax1.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])

    # if n_components == 2:
    #     # Create a meshgrid to visualize the decision boundary
    #     h = .01
    #     x_min, x_max = reduced_data[:, 0].min() - 1, reduced_data[:, 0].max() + 1
    #     y_min, y_max = reduced_data[:, 1].min() - 1, reduced_data[:, 1].max() + 1
    #     xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))
    #
    #     # Obtain labels for meshgrid
    #     Z = model.predict(np.c_[xx.ravel(), yy.ravel()])
    #
    #     # Plot decision boundary
    #     Z = Z.reshape(xx.shape)
    #     ax2.imshow(Z, interpolation='nearest',
    #                extent=(xx.min(), xx.max(), yy.min(), yy.max()),
    #                cmap=plt.cm.Paired,
    #                aspect='auto', origin='lower')

    # 2nd Plot showing the actual clusters formed
    size = 30 * probs.max(1) ** 2
    colors = cm.spectral(cluster_labels.astype(float) / n_clusters)
    if n_components == 2:
        ax2.scatter(reduced_data[:, 0], reduced_data[:, 1], marker='.', s=size, lw=0, alpha=0.7,
                    c=colors, edgecolor='k')
    elif n_components == 3:
        ax2.scatter(reduced_data[:, 0], reduced_data[:, 1], reduced_data[:, 2], marker='.', s=30, lw=0, alpha=0.7,
                    c=colors, edgecolor='k')

    # # Labeling the clusters
    # centers = model.cluster_centers_
    # # Draw white circles at cluster centers
    # ax2.scatter(centers[:, 0], centers[:, 1], marker='o',
    #             c="white", alpha=1, s=200, edgecolor='k')
    #
    # for i, c in enumerate(centers):
    #     ax2.scatter(c[0], c[1], marker='$%d$' % i, alpha=1,
    #                 s=50, edgecolor='k')

    ax2.set_title("Visualization of the Clustered Data")
    ax2.set_xlabel("Feature space for the 1st PCA component")
    ax2.set_ylabel("Feature space for the 2nd PCA component")
    if n_components == 3:
        ax2.set_zlabel("Feature space for the 3rd PCA component")

    plt.suptitle(("Silhouette Analysis for GMM Clustering "
                  "with n_clusters = %d" % n_clusters),
                 fontsize=14, fontweight='bold')

    plt.show()


def vis_tsne(model, reduced_data, cluster_labels, n_components, n_clusters, silhouette_avg):
    """Visualize data using TSNE."""
    fig, (ax1, ax2) = plt.subplots(1, 2)

    # The 1st subplot is the silhouette plot
    # The silhouette coefficient can range from -1, 1 but in this example all
    # lie within [-0.1, 1]
    ax1.set_xlim([-0.1, 1])
    # The (n_clusters+1)*10 is for inserting blank space between silhouette
    # plots of individual clusters, to demarcate them clearly.
    ax1.set_ylim([0, len(reduced_data) + (n_clusters + 1) * 10])

    # Compute the silhouette scores for each sample
    sample_silhouette_values = silhouette_samples(reduced_data, cluster_labels)

    y_lower = 10
    for i in range(n_clusters):
        # Aggregate the silhouette scores for samples belonging to
        # cluster i, and sort them
        ith_cluster_silhouette_values = sample_silhouette_values[cluster_labels == i]

        ith_cluster_silhouette_values.sort()

        size_cluster_i = ith_cluster_silhouette_values.shape[0]
        y_upper = y_lower + size_cluster_i

        color = cm.spectral(float(i) / n_clusters)
        ax1.fill_betweenx(np.arange(y_lower, y_upper),
                          0, ith_cluster_silhouette_values,
                          facecolor=color, edgecolor=color, alpha=0.7)

        # Label the silhouette plots with their cluster numbers at the middle
        ax1.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))

        # Compute the new y_lower for next plot
        y_lower = y_upper + 10  # 10 for the 0 samples

    ax1.set_title("The silhouette plot for the various clusters.")
    ax1.set_xlabel("The silhouette coefficient values")
    ax1.set_ylabel("Cluster label")

    # The vertical line for average silhouette score of all the values
    ax1.axvline(x=silhouette_avg, color="red", linestyle="--")

    ax1.set_yticks([])  # Clear the yaxis labels / ticks
    ax1.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])

    X_embedded = TSNE(n_components=2).fit_transform(reduced_data)
    colors = cm.spectral(cluster_labels.astype(float) / n_clusters)
    ax2.scatter(X_embedded[:, 0], X_embedded[:, 1], marker='.', s=30, lw=0, alpha=0.7,
                c=colors, edgecolor='k')

    plt.show()


def vis_prob(reduced_data, cluster_labels, probs, n_components, n_clusters, silhouette_avg):
    """Visualize data with probability values."""
    sns.set()

    # Create a subplot based on components
    if n_components == 2:
        fig, (ax1, ax2) = plt.subplots(1, 2)
    elif n_components == 3:
        fig = plt.figure()
        ax1 = fig.add_subplot(121, sharex=None, sharey=None)
        ax2 = fig.add_subplot(122, sharex=None, sharey=None, projection='3d')

    # The 1st subplot is the silhouette plot
    # The silhouette coefficient can range from -1, 1 but in this example all
    # lie within [-0.1, 1]
    ax1.set_xlim([-0.1, 1])
    # The (n_clusters+1)*10 is for inserting blank space between silhouette
    # plots of individual clusters, to demarcate them clearly.
    ax1.set_ylim([0, len(reduced_data) + (n_clusters + 1) * 10])

    # Compute the silhouette scores for each sample
    sample_silhouette_values = silhouette_samples(reduced_data, cluster_labels)

    y_lower = 10
    for i in range(n_clusters):
        # Aggregate the silhouette scores for samples belonging to
        # cluster i, and sort them
        ith_cluster_silhouette_values = sample_silhouette_values[cluster_labels == i]

        ith_cluster_silhouette_values.sort()

        size_cluster_i = ith_cluster_silhouette_values.shape[0]
        y_upper = y_lower + size_cluster_i

        color = cm.spectral(float(i) / n_clusters)
        ax1.fill_betweenx(np.arange(y_lower, y_upper),
                          0, ith_cluster_silhouette_values,
                          facecolor=color, edgecolor=color, alpha=0.7)

        # Label the silhouette plots with their cluster numbers at the middle
        ax1.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))

        # Compute the new y_lower for next plot
        y_lower = y_upper + 10  # 10 for the 0 samples

    ax1.set_title("The silhouette plot for the various clusters.")
    ax1.set_xlabel("The silhouette coefficient values")
    ax1.set_ylabel("Cluster label")

    # The vertical line for average silhouette score of all the values
    ax1.axvline(x=silhouette_avg, color="red", linestyle="--")

    ax1.set_yticks([])  # Clear the yaxis labels / ticks
    ax1.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])

    # 2nd Plot showing the actual clusters formed
    size = 50 * probs.max(1) ** 2
    colors = cm.spectral(cluster_labels.astype(float) / n_clusters)
    if n_components == 2:
        ax2.scatter(reduced_data[:, 0], reduced_data[:, 1], marker='.', s=size, lw=0, alpha=0.7,
                    c=colors, edgecolor='k')
    elif n_components == 3:
        ax2.scatter(reduced_data[:, 0], reduced_data[:, 1], reduced_data[:, 2], marker='.', s=size, lw=0, alpha=0.7,
                    c=colors, edgecolor='k')

    ax2.set_title("The visualization of the clustered data.")
    ax2.set_xlabel("Feature space for the 1st PCA component")
    ax2.set_ylabel("Feature space for the 2nd PCA component")
    if n_components == 3:
        ax2.set_zlabel("Feature space for the 3rd PCA component")

    plt.suptitle(("Silhouette analysis for clustering on sample data "
                  "with n_clusters = %d" % n_clusters),
                 fontsize=14, fontweight='bold')

    plt.show()


def spectral(df, n_components=2):
    """Spectral clustering on PCA-reduced data with silhouette scores."""

    print('Spectral clustering on PCA-reduced data')

    # Standardize data
    df_tr = StandardScaler().fit_transform(df)

    # Plot explained variance ratio graph
    pca = PCA().fit(df_tr)
    plt.plot(np.cumsum(pca.explained_variance_ratio_))
    plt.xlabel('Number of components')
    plt.ylabel('Cumulative explained variance')
    plt.show()

    # Reduce features via PCA
    pca = PCA(n_components=n_components).fit(df_tr)
    reduced_data = pca.transform(df_tr)

    # Plot PCA components composition
    plt.matshow(pca.components_, cmap='viridis')
    plt.yticks([0, 1, 2, 3, 4], ["First component", "Second component",
                                 "Third component", "Fourth component", "Fifth component"])
    plt.colorbar()
    plt.xticks(range(len(df.columns)),
               df.columns, rotation=60, ha='left')
    plt.xlabel("Feature")
    plt.ylabel("Principal components")
    plt.show()

    range_n_clusters = range(2, 35)
    for n_clusters in range_n_clusters:
        # Initialize the clusterer
        model = SpectralClustering(n_clusters=n_clusters)
        cluster_labels = model.fit_predict(reduced_data)

        # The silhouette_score gives the average value for all the samples.
        silhouette_avg = silhouette_score(reduced_data, cluster_labels)
        print("For n_clusters =", n_clusters,
              "The average silhouette_score is :", silhouette_avg)

        if n_clusters == 29:
            dfq['Cluster Labels'] = cluster_labels
            vis_tsne(model, reduced_data, cluster_labels, n_components, n_clusters, silhouette_avg)


def affinity(df, n_components=5):
    """Affinity Propagation clustering on PCA-reduced data with silhouette scores."""

    print('Affinity Propagation clustering on PCA-reduced (' +
          str(n_components) + ' components) ' + 'data')

    # Standardize data
    df_tr = StandardScaler().fit_transform(df)

    # Plot explained variance ratio graph
    pca = PCA().fit(df_tr)
    plt.plot(np.cumsum(pca.explained_variance_ratio_))
    plt.xlabel('Number of components')
    plt.ylabel('Cumulative explained variance')
    plt.show()

    # Reduce features via PCA
    pca = PCA(n_components=n_components).fit(df_tr)
    reduced_data = pca.transform(df_tr)

    # Plot PCA components composition
    plt.matshow(pca.components_, cmap='viridis')
    plt.yticks([0, 1, 2, 3, 4], ["First component", "Second component",
                                 "Third component", "Fourth component", "Fifth component"])
    plt.colorbar()
    plt.xticks(range(len(df.columns)),
               df.columns, rotation=60, ha='left')
    plt.xlabel("Feature")
    plt.ylabel("Principal components")
    plt.show()

    # Initialize the clusterer
    model = AffinityPropagation()
    cluster_labels = model.fit_predict(reduced_data)

    labels_unique = np.unique(cluster_labels)
    n_clusters_ = len(labels_unique)

    # The silhouette_score gives the average value for all the samples.
    silhouette_avg = silhouette_score(reduced_data, cluster_labels)
    print("For n_clusters =", n_clusters_,
          "The average silhouette_score is :", silhouette_avg)


def agglomerative(df, n_components=5, v=False):
    """Agglomerative clustering on PCA-reduced data with silhouette scores."""

    print('Agglomerative clustering on PCA-reduced data')

    # Standardize data
    df_tr = StandardScaler().fit_transform(df)

    # Plot explained variance ratio graph
    pca = PCA().fit(df_tr)
    plt.plot(np.cumsum(pca.explained_variance_ratio_))
    plt.xlabel('Number of components')
    plt.ylabel('Cumulative explained variance')
    plt.show()

    # Reduce features via PCA
    pca = PCA(n_components=n_components).fit(df_tr)
    reduced_data = pca.transform(df_tr)

    # Plot PCA components composition
    plt.matshow(pca.components_, cmap='viridis')
    plt.yticks([0, 1, 2, 3, 4], ["First component", "Second component",
                                 "Third component", "Fourth component", "Fifth component"])
    plt.colorbar()
    plt.xticks(range(len(df.columns)),
               df.columns, rotation=60, ha='left')
    plt.xlabel("Feature")
    plt.ylabel("Principal components")
    plt.show()

    # linkages = ['ward', 'complete', 'average']
    linkages = ['ward']
    for linkage in linkages:
        print('Linkage: ' + linkage)

        range_n_clusters = range(2, 75)
        for n_clusters in range_n_clusters:
            # Initialize the clusterer
            model = AgglomerativeClustering(n_clusters=n_clusters, linkage=linkage)
            cluster_labels = model.fit_predict(reduced_data)

            # The silhouette_score gives the average value for all the samples.
            silhouette_avg = silhouette_score(reduced_data, cluster_labels)
            print("For n_clusters =", n_clusters,
                  "The average silhouette_score is :", silhouette_avg)

            if n_clusters == 30:
                # vis(model, reduced_data, cluster_labels, n_components, n_clusters, silhouette_avg)
                dfq['Cluster Labels'] = cluster_labels


def dbscan(df, n_components=5):
    """DBSCAN clustering on PCA-reduced data with silhouette scores."""

    print('DBSCAN clustering on PCA-reduced (' + str(n_components) + ' components) ' + 'data')

    # Standardize data
    df_tr = StandardScaler().fit_transform(df)

    # Plot explained variance ratio graph
    pca = PCA().fit(df_tr)
    plt.plot(np.cumsum(pca.explained_variance_ratio_))
    plt.xlabel('Number of components')
    plt.ylabel('Cumulative explained variance')
    plt.show()

    # Reduce features via PCA
    pca = PCA(n_components=n_components).fit(df_tr)
    reduced_data = pca.transform(df_tr)

    # Plot PCA components composition
    plt.matshow(pca.components_, cmap='viridis')
    plt.yticks([0, 1, 2, 3, 4], ["First component", "Second component",
                                 "Third component", "Fourth component", "Fifth component"])
    plt.colorbar()
    plt.xticks(range(len(df.columns)),
               df.columns, rotation=60, ha='left')
    plt.xlabel("Feature")
    plt.ylabel("Principal components")
    plt.show()

    # Initialize the clusterer
    model = DBSCAN()
    cluster_labels = model.fit_predict(reduced_data)

    labels_unique = np.unique(cluster_labels)
    n_clusters_ = len(labels_unique)

    # The silhouette_score gives the average value for all the samples.
    silhouette_avg = silhouette_score(reduced_data, cluster_labels)
    print("For n_clusters =", n_clusters_,
          "The average silhouette_score is :", silhouette_avg)


def birch(df, n_components=5):
    """Birch clustering on PCA-reduced data with silhouette scores."""

    print('Birch clustering on PCA-reduced data')

    # Standardize data
    df_tr = StandardScaler().fit_transform(df)

    # Plot explained variance ratio graph
    pca = PCA().fit(df_tr)
    plt.plot(np.cumsum(pca.explained_variance_ratio_))
    plt.xlabel('Number of components')
    plt.ylabel('Cumulative explained variance')
    plt.show()

    # Reduce features via PCA
    pca = PCA(n_components=n_components).fit(df_tr)
    reduced_data = pca.transform(df_tr)

    # Plot PCA components composition
    plt.matshow(pca.components_, cmap='viridis')
    plt.yticks([0, 1, 2, 3, 4], ["First component", "Second component",
                                 "Third component", "Fourth component", "Fifth component"])
    plt.colorbar()
    plt.xticks(range(len(df.columns)),
               df.columns, rotation=60, ha='left')
    plt.xlabel("Feature")
    plt.ylabel("Principal components")
    plt.show()

    range_n_clusters = range(2, 76)
    for n_clusters in range_n_clusters:
        # Initialize the clusterer
        model = Birch(n_clusters=n_clusters)
        cluster_labels = model.fit_predict(reduced_data)

        # The silhouette_score gives the average value for all the samples.
        silhouette_avg = silhouette_score(reduced_data, cluster_labels)
        print("For n_clusters =", n_clusters,
              "The average silhouette_score is :", silhouette_avg)


def meanshift(df, n_components=5):
    """Mean Shift clustering on PCA-reduced data with silhouette scores."""

    print('Mean Shift clustering on PCA-reduced data')

    # Standardize data
    df_tr = StandardScaler().fit_transform(df)

    # Plot explained variance ratio graph
    pca = PCA().fit(df_tr)
    plt.plot(np.cumsum(pca.explained_variance_ratio_))
    plt.xlabel('Number of components')
    plt.ylabel('Cumulative explained variance')
    plt.show()

    # Reduce features via PCA
    pca = PCA(n_components=n_components).fit(df_tr)
    reduced_data = pca.transform(df_tr)

    # Plot PCA components composition
    plt.matshow(pca.components_, cmap='viridis')
    plt.yticks([0, 1, 2, 3, 4], ["First component", "Second component",
                                 "Third component", "Fourth component", "Fifth component"])
    plt.colorbar()
    plt.xticks(range(len(df.columns)),
               df.columns, rotation=60, ha='left')
    plt.xlabel("Feature")
    plt.ylabel("Principal components")
    plt.show()

    # Initialize the clusterer
    model = MeanShift()
    cluster_labels = model.fit_predict(reduced_data)

    labels_unique = np.unique(cluster_labels)
    n_clusters_ = len(labels_unique)

    # The silhouette_score gives the average value for all the samples.
    silhouette_avg = silhouette_score(reduced_data, cluster_labels)
    print("For n_clusters =", n_clusters_,
          "The average silhouette_score is :", silhouette_avg)

################################################################################
# Execution
################################################################################


if __name__ == '__main__':
    sns.set()

    # df = pickle.load(open("data/ml.p", "rb"))
    # gmm(df, 30, n_components=2)

    # Create empty DataFrame
    dfq = pickle.load(open("data/q.p", "rb"))
    df_diff_final = dfq[dfq.index.get_level_values(0) == '2011-01'].drop(['Population Q-Score', 'Poverty Q-Score', 'Income Q-Score', 'Home Q-Score', 'Crime Q-Score'], axis=1)
    df_diff_final.index = df_diff_final.index.droplevel(0)
    df_diff_final.index.rename(['Neighborhood #', 'Neighborhood'], inplace=True)
    df_diff_final.columns = ['Q']
    df_diff_final.Q = 0

    # Number of iterations to run the ML pipeline
    iterations = 1000
    # Number of clusters
    nc = 30

    for i in tqdm(range(iterations)):
        df = pickle.load(open("data/ml.p", "rb"))
        dfq = pickle.load(open("data/q.p", "rb"))

        gmm_final(df, nc, n_components=2)

        meanq = pd.Series()

        # Score each cluster by the mean of the instance's scores
        for cluster in range(nc):
            mask = dfq['Cluster Labels'] == cluster
            mask_index = dfq[mask].index

            dfq.loc[mask_index, 'Mean Q-Scores'] = dfq[mask]['Total Q-Score'].mean()

            meanq = meanq.set_value('Cluster ' + str(cluster), dfq[mask]['Total Q-Score'].mean())

        meanq.sort_values(ascending=False, inplace=True)
        dfq = dfq['Cluster Labels']

        # Convert Series with cluster scores to DataFrame
        meanq = pd.DataFrame(meanq)
        meanq.reset_index(inplace=True)
        meanq.columns = ['Cluster', 'Q']
        meanq.Cluster = meanq.Cluster.apply(lambda x: x[8:]).astype(int)

        # Create DataFrames that contain January 2011 instances and December 2015 instances
        dfq = pd.DataFrame(dfq)
        dfq.index.rename(['Date', 'Neighborhood #', 'Neighborhood'], inplace=True)
        dfq.columns = ['Cluster']
        df_11 = dfq[dfq.index.get_level_values(0) == '2011-01']
        df_15 = dfq[dfq.index.get_level_values(0) == '2015-12']

        df_11 = df_11.reset_index().merge(meanq).set_index(['Date', 'Neighborhood #', 'Neighborhood'])
        df_11.index = df_11.index.droplevel(0)
        df_15 = df_15.reset_index().merge(meanq).set_index(['Date', 'Neighborhood #', 'Neighborhood'])
        df_15.index = df_15.index.droplevel(0)

        # Calculate difference of scores between January 2011 and December 2015
        df_diff = df_15 - df_11
        df_diff.drop('Cluster', axis=1, inplace=True)

        # Add iteration results
        df_diff_final += df_diff

    # Find mean of iteration results
    df_diff_final /= iterations
    df_diff_final.sort_values('Q', ascending=False, inplace=True)
    print(df_diff_final.apply(round, args=(4,), axis=1))
