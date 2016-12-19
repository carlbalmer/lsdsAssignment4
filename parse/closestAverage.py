import re

inputFile = open("closestAverage_massive_churn", "r")
beginning = False
matches = []
for line in inputFile:
    if re.search(r"ClosestAverage", line) is not None:
        split = line.split(" ")
        matches.append([int(split[2]),float(split[4])])
inputFile.close()

dict = {}

for i in range(1, 50):
    dict[i] = {'sum':0, 'count':0}

for match in matches:
    dict[match[0]]["sum"] += match[1]
    dict[match[0]]["count"] += 1

print(dict)


output_file = open("output.txt", "w")
for k in dict.keys():
    output_file.write(str(k)+" "+str(dict[k]["sum"]/dict[k]["count"]) +"\n")
