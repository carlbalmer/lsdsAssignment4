filename = "local_run2"

def timestampToS(timestamp):
    times = timestamp.split(":")
    return int(times[0])*3600+int(times[1])*60+int(times[2])

split_lines = []
input_file = open(filename, "r")

for line in input_file:
    split_lines.append(line.split())
input_file.close()


infected = {}
for line in split_lines:
#to parse the differnt files I just changed this if statment. (It's a lazy solution but works...)
    if line[len(line)-1] == "i_am_infected":
        if timestampToS(line[1]) in infected:
            infected[timestampToS(line[1])] += 1
        else:
            infected[timestampToS(line[1])] = 1

total = sum(infected.values())
print(total)
sum = 0
startTime = sorted(infected.keys())[0]

output_file = open("out.txt", 'w')

for timestamp in sorted(infected.keys()):
    sum +=infected[timestamp]
    print(timestamp-startTime, sum, sum/total)
    output_file.write(str(timestamp-startTime)+ " " +str(sum) + " " + str(sum/total)+"\n")
