##################################################################3
## Create Fake Log using positive Priority data and random choices
##
nLines = 300;
nfx = 3;
print("fake_data.py -- start, printing nLines=" + str(nLines))
import json;  import os; import numpy as np;
jhome = None;
if 'config_jsons' in os.listdir(os.getcwd()) :
  jhome = os.getcwd() + "/config_jsons";
elif 'config_jsons' in os.listdir(os.getcwd() + "/..") :
  jhome = os.getcwd() + "/../" + "config_jsons";
if jhome is None :
  print("FAIL, did not find config_jsons"); exit();
with open(jhome + '/fix42.json', 'r') as file:
  fx2d = json.load(file)

schM = fx2d['schema'];
fix2M = fx2d['fix_fields'];
fixK = dict();
for kk in fix2M.keys() :
  if (fix2M[kk]['priority']> 0) :
    fixK[kk] = fix2M[kk];
print(" -- We have read fixK of length " + str(len(fixK)));
def rand_fix(nfx=3, ngrp=[]) :
  aKeys = [x for x in list(fixK.keys()) if str(x) not in ngrp];
  cKeys = np.random.choice(aKeys, size=nfx);
  iL = [];
  for ii in range(len(cKeys)) :
    ik = cKeys[ii];
    if 'encode' in list(fixK[ik].keys()) :
      iL.append( '"' + str(ik) +  '" : "' + rand_ipt(fixK[ik]['fixtitle'], fixK[ik]['typ'] , list(fixK[ik]['encode'].keys())) + '"')
    elif (fixK[ik]['typ'] in ['Decimal','decimal']) :
      iL.append( '"' + str(ik) + '" : ' + rand_ipt(fixK[ik]['fixtitle'], typ='Decimal',scale = fixK[ik]['scale'], width=fixK[ik]['width']))
    else :
      iL.append( '"' + str(ik) + '" : "' + rand_ipt(fixK[ik]['fixtitle'], typ=fixK[ik]['typ']) + '"')
  return( [str(x) for x in cKeys], "{" + ",".join(iL) + "}");

def rand_i32(low=0,high=25000) :
  return(np.random.randint(low=low, high=high,size=1,dtype=np.int32)[0]);
def rand_i64(low=0,high=25000) :
  return(np.random.randint(low=low, high=high,size=1,dtype=np.int64)[0]);
def rand_datetime(in_dt = '2025-09-15') :
  dtt = np.random.randint(0,60*60*24*1000000000, size=1, dtype=np.int64)[0];
  stt = str(dtt.astype('datetime64[ns]')).replace('1970-01-01T','');
  return(('' if in_dt=='' else (in_dt + ' ')) + stt);
def rand_dealer() :
  return(str(np.random.choice(['dealer1','dealer2'],size=1)[0]))
def rand_sender() :
  return(str(np.random.choice(['sender1','sender2','sender3'],size=1)[0]))

def rand_ipt(nm, typ, fixlist=[], width=10,scale=0) :
  if (nm == 'dealer') :
    return(str(rand_dealer()));
  elif (nm == 'sender') :
    return(str(rand_sender()));
  elif ((typ == 'tms') or (typ=='tns') or (typ=='tus')) :
    return(str(rand_datetime()));
  elif typ == 'i32' :
    return(str(rand_i32()));
  elif typ == 'Decimal' :
    scale = int(scale);
    return(str(rand_i64() / 10**scale));
  elif typ == 'Decimal(15,4)' :
    return(str(rand_i64() / 10**4));
  elif len(fixlist) > 0 :
    return(str(np.random.choice(fixlist,size=1)[0]));
  elif typ == 'str' :
    return(str(np.random.choice(['hello','boy','what','joe','me','ok'], size=1)[0]))
  else :
    return(str(rand_i32()));

exdir = jhome + "/../example";
exfile = exdir + "/ex_random.csv";
print("fake_data.py -- we are about to write to exfile = \"" + exfile + "\"");
sK = list(schM.keys());
with open(exfile, 'w') as wfile :
  for ii in range(nLines) :
    if ((ii+1) % 1000 == 0) :
      print(" --- -- on line ii=" + str(ii) + "/" + str(nLines));
    myL = list(); onfixgrp = [];
    for jj in range(len(sK)) :
      onS = schM[sK[jj]];
      if (onS['typ'] == 'fix42') :
        newKeys, nf = rand_fix(nfx, onfixgrp)
        onfixgrp = list(set(list(onfixgrp) + newKeys));
        myL.append(nf)
      elif (onS['typ'] in ['Decimal','decimal']) :
        myL.append(rand_ipt(sK[jj], onS['typ'], fixlist=[], scale = onS['scale'], width=onS['width']));
      else :
        myL.append(rand_ipt(sK[jj], onS['typ'], fixlist=[]));
    wfile.write( ",".join(myL) + "\n");

print(" all done writing " + str(nLines) + " lines.");
