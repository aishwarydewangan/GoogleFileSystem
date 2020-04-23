from numpy.random import shuffle

seq = [ i for i in range(1000)]

shuffle(seq)

with open("in.txt", "w") as output:
	for s in seq:
		output.write(str(s) + ' ')

print("Done")