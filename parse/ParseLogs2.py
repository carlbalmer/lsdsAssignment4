import re

inputFile = open("logs-finger-run-1", "r")
beginning = False
matches = []
for line in inputFile:
    if re.search(r"hops=", line) is not None:
        split = line.split("=")
        matches.append(int(split[len(split)-1]))
inputFile.close()

dict = {}

for match in set(matches):
    dict[match] = 0

for match in matches:
    dict[match] += 1

print(dict)

output_file = open("output.txt", "w")
for k in dict.keys():
    output_file.write(str(k)+" "+str(dict[k]) +"\n")