"""
Tony Sanchez
11/26/2017
Georgetown Cohort 10: The Data Extractors Team

Description:
This is Python code applies ML clusters.

"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from mpl_toolkits.mplot3d import Axes3D
import seaborn as sns
import time

from sklearn.decomposition import PCA
from sklearn.cluster import KMeans, SpectralClustering, AgglomerativeClustering
from sklearn.mixture import GaussianMixture
from sklearn import metrics
from sklearn.metrics import silhouette_samples, silhouette_score, calinski_harabaz_score
import loadCrimeIncidents as lc

################################################################################
# Functions
################################################################################


def kmeans(df, n_components, v=False):
    """Visualization of K-Means clustering on PCA-reduced data with silhouette scores."""

    print('K-Means clustering on PCA-reduced (' + str(n_components) + ' components) ' + 'data')

    range_n_clusters = range(2, 21)
    for n_clusters in range_n_clusters:
        # Initialize the clusterer
        reduced_data = PCA(n_components=n_components).fit_transform(df)
        model = KMeans(init='k-means++', n_clusters=n_clusters, n_init=10)
        model.fit(reduced_data)
        cluster_labels = model.predict(reduced_data)

        # The silhouette_score gives the average value for all the samples.
        # This gives a perspective into the density and separation of the formed
        # clusters
        silhouette_avg = silhouette_score(reduced_data, cluster_labels)
        print("For n_clusters =", n_clusters,
              "The average silhouette_score is :", silhouette_avg)

        # calinski = calinski_harabaz_score(reduced_data, cluster_labels)
        # print("For n_clusters =", n_clusters,
        #       "The Calinkski-Harabaz index is :", calinski)

        if v:
            vis_db(model, reduced_data, cluster_labels, n_components, n_clusters, silhouette_avg)


def vis_db(model, reduced_data, cluster_labels, n_components, n_clusters, silhouette_avg):
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

    ax1.set_title("The silhouette plot for the various clusters.")
    ax1.set_xlabel("The silhouette coefficient values")
    ax1.set_ylabel("Cluster label")

    # The vertical line for average silhouette score of all the values
    ax1.axvline(x=silhouette_avg, color="red", linestyle="--")

    ax1.set_yticks([])  # Clear the yaxis labels / ticks
    ax1.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])

    if n_components == 2:
        # Create a meshgrid to visualize the decision boundary
        h = 10
        x_min, x_max = reduced_data[:, 0].min() - 1, reduced_data[:, 0].max() + 1
        y_min, y_max = reduced_data[:, 1].min() - 1, reduced_data[:, 1].max() + 1
        xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))

        # Obtain labels for meshgrid
        Z = model.predict(np.c_[xx.ravel(), yy.ravel()])

        # Plot decision boundary
        Z = Z.reshape(xx.shape)
        ax2.imshow(Z, interpolation='nearest',
                   extent=(xx.min(), xx.max(), yy.min(), yy.max()),
                   cmap=plt.cm.Paired,
                   aspect='auto', origin='lower')

    # 2nd Plot showing the actual clusters formed
    colors = cm.spectral(cluster_labels.astype(float) / n_clusters)
    if n_components == 2:
        ax2.scatter(reduced_data[:, 0], reduced_data[:, 1], marker='.', s=30, lw=0, alpha=0.7,
                    c=colors, edgecolor='k')
    elif n_components == 3:
        ax2.scatter(reduced_data[:, 0], reduced_data[:, 1], reduced_data[:, 2], marker='.', s=30, lw=0, alpha=0.7,
                    c=colors, edgecolor='k')

    # Labeling the clusters
    centers = model.cluster_centers_
    # Draw white circles at cluster centers
    ax2.scatter(centers[:, 0], centers[:, 1], marker='o',
                c="white", alpha=1, s=200, edgecolor='k')

    for i, c in enumerate(centers):
        ax2.scatter(c[0], c[1], marker='$%d$' % i, alpha=1,
                    s=50, edgecolor='k')

    ax2.set_title("The visualization of the clustered data.")
    ax2.set_xlabel("Feature space for the 1st PCA component")
    ax2.set_ylabel("Feature space for the 2nd PCA component")
    if n_components == 3:
        ax2.set_zlabel("Feature space for the 3rd PCA component")

    plt.suptitle(("Silhouette analysis for clustering on sample data "
                  "with n_clusters = %d" % n_clusters),
                 fontsize=14, fontweight='bold')

    plt.show()


def kmeans_old(df):
    """Visualization of K-Means clustering on PCA-reduced data."""

    # Time beginning of modeling
    t0 = time.time()

    # Reduce data with PCA and fit reduced data into KMeans
    reduced_data = PCA(n_components=2).fit_transform(df)
    kmeans = KMeans(init='k-means++', n_clusters=5, n_init=10)
    kmeans.fit(reduced_data)

    # Create a meshgrid to visualize the decision boundary
    h = 10
    x_min, x_max = reduced_data[:, 0].min() - 1, reduced_data[:, 0].max() + 1
    y_min, y_max = reduced_data[:, 1].min() - 1, reduced_data[:, 1].max() + 1
    xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))

    # Obtain labels for meshgrid
    Z = kmeans.predict(np.c_[xx.ravel(), yy.ravel()])

    # Time ending of modeling and calculate time elapsed
    t1 = time.time()
    print('Time elapsed in seconds:', t1 - t0)

    # Plot decision boundary
    Z = Z.reshape(xx.shape)
    plt.figure(1)
    plt.clf()
    plt.imshow(Z, interpolation='nearest',
               extent=(xx.min(), xx.max(), yy.min(), yy.max()),
               cmap=plt.cm.Paired,
               aspect='auto', origin='lower')

    # Plot the reduced data
    plt.plot(reduced_data[:, 0], reduced_data[:, 1], 'k.', markersize=2)

    # Plot the centroids
    centroids = kmeans.cluster_centers_
    plt.scatter(centroids[:, 0], centroids[:, 1],
                marker='x', s=169, linewidths=3,
                color='w', zorder=10)

    # Finalize plot
    plt.title('K-means clustering on the dcgent dataset (PCA-reduced data)\n'
              'Centroids are marked with white cross')
    plt.xlim(x_min, x_max)
    plt.ylim(y_min, y_max)
    plt.xticks(())
    plt.yticks(())
    plt.show()


def spectral(df):
    """Visualization of spectral clustering on PCA-reduced data with silhouette scores."""

    print('Spectral clustering on PCA-reduced data')

    range_n_clusters = [3, 4, 5, 6, 7]
    for n_clusters in range_n_clusters:
        # Initialize the clusterer
        reduced_data = PCA(n_components=2).fit_transform(df)
        model = SpectralClustering(n_clusters=n_clusters)
        cluster_labels = model.fit_predict(df)

        # Create a subplot with 1 row and 2 columns
        fig, (ax1, ax2) = plt.subplots(1, 2)

        # The 1st subplot is the silhouette plot
        # The silhouette coefficient can range from -1, 1 but in this example all
        # lie within [-0.1, 1]
        ax1.set_xlim([-0.1, 1])
        # The (n_clusters+1)*10 is for inserting blank space between silhouette
        # plots of individual clusters, to demarcate them clearly.
        ax1.set_ylim([0, len(reduced_data) + (n_clusters + 1) * 10])

        # The silhouette_score gives the average value for all the samples.
        # This gives a perspective into the density and separation of the formed
        # clusters
        silhouette_avg = silhouette_score(reduced_data, cluster_labels)
        print("For n_clusters =", n_clusters,
              "The average silhouette_score is :", silhouette_avg)

        calinski = calinski_harabaz_score(reduced_data, cluster_labels)
        print("For n_clusters =", n_clusters,
              "The Calinkski-Harabaz index is :", calinski)

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
        colors = cm.spectral(cluster_labels.astype(float) / n_clusters)
        ax2.scatter(reduced_data[:, 0], reduced_data[:, 1], marker='.', s=30, lw=0, alpha=0.7,
                    c=colors, edgecolor='k')

        ax2.set_title("The visualization of the clustered data.")
        ax2.set_xlabel("Feature space for the 1st PCA component")
        ax2.set_ylabel("Feature space for the 2nd PCA component")

        plt.suptitle(("Silhouette analysis for clustering on sample data "
                      "with n_clusters = %d" % n_clusters),
                     fontsize=14, fontweight='bold')

        plt.show()


def gmm(df, n_components, v=False):
    """Visualization of GMM clustering on PCA-reduced data with silhouette scores."""

    print('GMM clustering on PCA-reduced (' + str(n_components) + ' components) ' + 'data')

    range_n_clusters = range(2, 21)
    for n_clusters in range_n_clusters:
        # Initialize the clusterer
        reduced_data = PCA(n_components=n_components).fit_transform(df)
        model = GaussianMixture(n_components=n_clusters, covariance_type='tied')
        model.fit(reduced_data)
        cluster_labels = model.predict(reduced_data)
        probs = model.predict_proba(reduced_data)

        # Print the average silhouette score
        silhouette_avg = silhouette_score(reduced_data, cluster_labels)
        print("For n_clusters =", n_clusters,
              "The average silhouette_score is :", silhouette_avg)

        # calinski = calinski_harabaz_score(reduced_data, cluster_labels)
        # print("For n_clusters =", n_clusters,
        #       "The Calinkski-Harabaz index is :", calinski)

        if v:
            vis_prob(reduced_data, cluster_labels, probs, n_components, n_clusters, silhouette_avg)


def vis_prob(reduced_data, cluster_labels, probs, n_components, n_clusters, silhouette_avg):
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

################################################################################
# Execution
################################################################################
if __name__ == '__main__':
    print("Starting Visualizations...")
    path = 'Insert DB path here'
    frm = '2010-01'
    to = '2017-01'
    """" Three calls to get final processed Crime DataFrame """
    #returns full date range expected
    drDF = lc.getDateRangeDF(frm, to)
    #returns full crime_incidents DF with n_cluster DF
    baseDF, nhDF = lc.getBaseDF(path, frm, to)
    #pass in both DFs to get final edited DF
    df = lc.insertEmptyRows(baseDF, nhDF, drDF)
    #remove columns
    del df['n_cluster']
    del df['year_month']

    kmeans(df, 3, v=True)
