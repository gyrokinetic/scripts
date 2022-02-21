import numpy as np

print("np array and matrix\n")

print("1d array of 15 elements")
a = np.arange(15) 
print(a, "\n\nReshape to (3,5) 2d array")

a=a.reshape(3,5)
print(a, "\n\nTranspose to (5,3) 2d array")

a=a.T
print(a, "\n\nNormalize (5,3) 2d array")

norm = np.linalg.norm(a, axis=1)
num=len(a)
an = a/norm.reshape(num,1)
print(an, "\n\nCross correlation (5,3) 2d array")

arr = []
for ict in range(num-1):
   cor = np.inner(an[ict], an[ict+1:])
   jdx = np.where(cor > 0.96)[0]
   jmx = len(jdx)
   if(jmx > 0):
      idx = np.full(jmx, ict)
      val = cor[jdx]
      vec = np.array([idx, jdx+ict+1, val]).T
      arr += list(vec)
   fn=str(ict) + '-[' + str(ict+1) + ':' + str(num-1) + ']'
   print(fn, arr)
print(arr)
np.save("data.npy", a)
np.save("corr.npy", arr)

a=np.load("data.npy")
arr=np.load("corr.npy")
print(a)
print(arr)
