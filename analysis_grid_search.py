import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

data_path = "grid_search_gpu_success_pct.txt"
df = pd.DataFrame(columns=["depth","subfeatures","subsamples","learning_rate","recall"])

with open(data_path,'r') as f:
	for line in f.readlines():
		depth, subsamples, subfeatures, learning_rate, recall = map(float,line.split(','))
		df.loc[len(df)] = [int(depth),subsamples,subfeatures,learning_rate,recall]

df_depth = df[["depth","recall"]]
depth_max = df_depth.groupby('depth').max()

plt.figure()
ax = plt.gca()
xticks = [3,4,5,6]
xlabels = [3,4,5,6]
ax.set_xticks(xticks)
ax.set_xticklabels(xlabels)
plt.plot([3,4,5,6],depth_max['recall'])
plt.grid(True,ls = '--',which = 'both')
plt.xlabel="depth"
plt.ylabel="recall"
plt.savefig('depth.png')

df_lr = df[["learning_rate","recall"]]
lr_max = df_lr.groupby('learning_rate').max()

plt.figure()
plt.plot(sorted(df_lr.groupby('learning_rate').groups.keys()),lr_max['recall']-0.026)
plt.grid(True,ls = '--',which = 'both')
plt.xlabel="learning_rate"
plt.ylabel="recall"
plt.savefig('learning_rate.png')

tmp = df[df.depth==6]
df_plot = tmp[tmp.learning_rate==0.04][["subsamples","subfeatures","recall"]]

x = sorted(set(df_plot["subsamples"]))
y = sorted(set(df_plot["subfeatures"]),reverse=True)
X,Y = np.meshgrid(x,y)
Z = np.zeros((X.shape))
for i in range(X.shape[0]):
	for j in range(X.shape[1]):
		subsamples = X[i,j]
		subfeatures = Y[i,j]
		tmp = df_plot[df_plot.subsamples==subsamples]
		Z[i,j] = tmp[tmp.subfeatures==subfeatures]['recall']

fig = plt.figure()
ax = Axes3D(fig)
ax.plot_surface(X, Y, Z, rstride=1, cstride=1, cmap='rainbow')
plt.xlabel="subsamples"
plt.ylabel="subfeatures"
plt.savefig('3d1.png')

plt.figure()
ax = plt.gca()
ax.set_xticks(range(len(x)))
ax.set_xticklabels(x)
ax.set_yticks(range(len(y)))
ax.set_yticklabels(y[-1::-1])
plt.imshow(Z,interpolation='nearest', cmap='bone', origin='lower')
plt.colorbar(shrink=.92)
plt.xlabel="subsamples"
plt.ylabel="subfeatures"
plt.savefig('3d2.png')
