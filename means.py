import os
path ="C:/Users/kersc/Desktop/Masters/Year 2/Analytics Assignment"
os.chdir(path)
os.getcwd()
import pandas
import pylab as pl
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA

# Generate elbow curve to know how many clusters

variables = pandas.read_csv("kmeans2.csv")
Y  = variables[['Price']]
X  = variables[['Average']]

# normalize data
X_norm = (X - X.mean()) / (X.max() - X.min())
Y_norm = (Y - Y.mean()) / (Y.max() - Y.min())

# Elbow curve
Nc = range(1,20)
kmeans = [KMeans(n_clusters=i) for  i in Nc]
kmeans
score = [kmeans[i].fit(Y).score(Y) for i in range(len(kmeans))]
score
pl.plot(Nc,score)
pl.xlabel("Number of Clusters")
pl.ylabel("Score")
pl.title("Elbow curve")
pl.show()

pca = PCA(n_components=1).fit(Y_norm)
pca_d = pca.transform(Y_norm)
pca_c = pca.transform(X_norm)

pca_d
pca_c


kmeans = KMeans(n_clusters=5)
kmeansoutput = kmeans.fit(Y_norm)
kmeansoutput
pl.figure('4 Cluster K-Means')
pl.scatter(pca_c[:,0],pca_d[:,0],c=kmeansoutput.labels_)
pl.xlabel('Dividend Yield')
pl.ylabel('Returns')
pl.title('4 Cluster K-means')
pl.show()
