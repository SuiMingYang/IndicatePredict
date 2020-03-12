
#葡萄酒数据集+PCA
import matplotlib.pyplot as plt#画图工具
from mpl_toolkits.mplot3d import Axes3D
from sklearn import datasets
data=datasets.load_wine()
X=data['data']
y=data['target']


from sklearn.decomposition import PCA
pca = PCA(n_components=2)
X_p =pca.fit(X).transform(X)
ax = plt.figure()
for c, i, target_name in zip("rgb", [0, 1, 2], data.target_names):
    plt.scatter(X_p[y == i, 0], X_p[y == i, 1], c=c, label=target_name)
plt.xlabel('Dimension1')
plt.ylabel('Dimension2')
plt.title("文本分类")
plt.legend(['物流','退换货','尺码'])
plt.show()
