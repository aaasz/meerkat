core = {}

proc = -1
for line in open("/proc/cpuinfo"):
  line = line.strip().split(':')

  if line[0].strip() == "processor":
    proc = int(line[1].strip())
    core[proc] = {}

  if line[0].strip() == "physical id":
    core[proc]["physid"] = line[1].strip()

  if line[0].strip() == "core id":
    core[proc]["coreid"] = line[1].strip()
  
  if line[0].strip() == "cpu cores":
    core[proc]["cores"] = line[1].strip()

  if line[0].strip() == "apicid":
    core[proc]["apicid"] = line[1].strip()

  if line[0].strip() == "initial apicid":
    core[proc]["iapicid"] = line[1].strip()

  if line[0].strip() == "siblings":
    core[proc]["siblings"] = line[1].strip()

print "core#, phyid, coreid, #cores, apicid, iapicid, sib"
for c in sorted(core):
  print c, core[c]["physid"], core[c]["coreid"], core[c]["cores"], core[c]["apicid"], core[c]["iapicid"], core[c]["siblings"]
