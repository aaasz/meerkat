import sys


nkeys = int(sys.argv[1])
alpha = float(sys.argv[2])


zipf = [0] * nkeys
c = 0.0

for i in range(nkeys):
    c = c + (1.0 / (i+1)**alpha)


c = 1 / c
sum = 0.0


for i in range(nkeys):
    sum += (c / (i+1)**alpha)
    zipf[i] = sum

print zipf[0:10]
