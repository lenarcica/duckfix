##################################################################3
## Create Fake Log using positive Priority data and random choices
##
## Alan Lenarcic
## Janaury 2026
##
##

nLines = 300;
nfx = 3;
print("fake_data.py -- start, printing nLines=" + str(nLines))
import json;  import os; import numpy as np;  from pathlib import Path;
import sys, os;
config_dir = None; 

def load_config_file(config_file:str="fix42.json", config_dir:str="") :
  import os; from pathlib import Path;
  jhome = None;
  gtcwd = os.getcwd();
  print("load_config_file: starting with config_file = " + 
    ("None" if config_file is None else "Not string" if not isinstance(config_file,str) else config_file));
  if (config_file is None) or (isinstance(config_file,str)==False) :
    config_file = "fix42.json";
  print("load_config_file: now config_file = " + 
    ("None" if config_file is None else "Not string" if not isinstance(config_file,str) else config_file));
  od1 = Path(gtcwd) / "config_jsons";
  od2 = Path(gtcwd).parent / "config_jsons";
  jbome = None;
  config_file = config_file.replace("'","");
  if ((config_dir is not None) and (isinstance(config_dir, str)) and 
      (os.path.isdir(config_dir)) and os.access(config_dir, os.R_OK) and
      (config_file in os.listdir(config_dir))) :
    jhome = str(config_dir);
    print("jhome is directory supplied.");
  elif (('config_jsons' in os.listdir(gtcwd)) and
       (os.path.isdir(od1)) and 
       (os.access(od1,os.R_OK )) and
       ( (config_file in os.listdir(od1)) or 
         ("fix42.json" in os.listdir(od1)) or 
         ('fix42_t2.json' in os.listdir(od1))
       )) :
    jhome = str(od1);   print("jhome is od1 with contents[" + ",".join(os.listdir(od1)) + "]");
    if config_file in os.listdir(od1) :
      print("config_file " + config_file + " is in listdir od1");
    else :
      print("Fail to find config_file + \"" + config_file + "\" in od1");
    config_file = config_file if (config_file in os.listdir(od1)) else "fix42.json";
  elif (('config_jsons' in os.listdir(gtcwd)) and
       (os.path.isdir(od2)) and 
       (os.access(od2, os.R_OK)) and
       (str(config_file) in os.listdir(od2)) and
        ("fix42.json" in os.listdir(od2))) :
    jhome = str(od2); 
    config_file = config_file if config_file in os.listdir(os1) else "fix42.json";
  if jhome is None :
    print("FAIL, did not find config_jsons"); exit();
  jhome = str(jhome);
  print("  We now have jhome = " + jhome);
  print("  We have config_file = " + config_file);
  print("  load_config_file() opening " + jhome + "/" + config_file);
  with open(jhome + '/' + config_file, 'r') as file:
    fx2d = json.load(file)
  schM = fx2d['schema'];
  for key in list(schM.keys()) :
    schM[key]['nm'] = key;
  fix2M = fx2d['fix_fields'];
  general_sep = ',' if ('general_sep' not in fx2d.keys()) else fx2d['general_sep'];
  fixK = dict();
  for kk in fix2M.keys() :
    if (fix2M[kk]['priority']> 0) :
      fixK[kk] = fix2M[kk];
  print(" -- We have read fixK of length " + str(len(fixK)));
  return(jhome, fx2d, fixK, schM, fix2M, general_sep);

def rand_fix(nfx=3, fixK=dict(), ngrp=[], general_sep=' ', fixequal=':', fixstyle="fixjson") :
  aKeys = [x for x in list(fixK.keys()) if str(x) not in ngrp];
  cKeys = np.random.choice(aKeys, size=nfx, replace=False);
  iL = [];
  for ii in range(len(cKeys)) :
    ik = cKeys[ii];
    if 'encode' in list(fixK[ik].keys()) :
      iL.append( '"' + str(ik) +  '"' + fixequal + '"' + rand_ipt(fixK[ik]['fixtitle'], fixK[ik]['typ'] , 
        list(fixK[ik]['encode'].keys()), colname=fixK[ik]['nm']) + '"');
    elif (fixK[ik]['typ'] in ['Decimal','decimal']) :
      iL.append( '"' + str(ik) + '"' + fixequal + '' + rand_ipt(fixK[ik]['fixtitle'], 
        typ='Decimal',scale = fixK[ik]['scale'], width=fixK[ik]['width'],colname=fixK[ik]['nm']))
    else :
      iL.append( '"' + str(ik) + '"' + fixequal + '"' + rand_ipt(fixK[ik]['fixtitle'], 
        typ=fixK[ik]['typ'], colname=fixK[ik]['nm'], fmttime=('YYYY-mm-dd HH:MM:SS.fffffffff') if ('fmt' not in fixK[ik].keys()) else (fixK[ik]['fmt'])) +'"')
  if fixstyle == "fix2end" :
    return([str(x) for x in cKeys], general_sep.join(iL).replace('"',""));
  return( [str(x) for x in cKeys], "{" + general_sep.join(iL) + "}");

def rand_i32(low=0,high=25000) :
  return(np.random.randint(low=low, high=high,size=1,dtype=np.int32)[0]);
def rand_i64(low=0,high=25000) :
  return(np.random.randint(low=low, high=high,size=1,dtype=np.int64)[0]);
def rand_datetime(in_dt = '2025-09-15', fmt='YYYY-mm-dd HH:MM:SS.FFFFFFF') :
  dtt = np.random.randint(0,60*60*24*1000000000, size=1, dtype=np.int64)[0];
  stt = str(dtt.astype('datetime64[ns]')).replace('1970-01-01T','');
  sep = ' ';
  if (fmt in ['YYYY-mm-ddTHH:MM:SS.FFFFFF','YYYY-MM-DDTHH:MM:SS.FFFFFF','YYYY-mm-ddTHH:MM:SS.FFFFFFFFF',
              'YYYY-mm-ddTHH:MM:SS.FFFFFFFFF', '%Y-%m-%dT%M.%S:%S.%f',
              '%Y-%m-%dT%H:%M:%S.%f', '%Y-%M-%DT%H:%M:%S.%f']) :
    sep='T';
  elif (fmt in ['YYYY-mm-ddDHH:MM:SS.FFFFFF','YYYY-MM-DDDHH:MM:SS.FFFFFF','YYYY-mm-ddDHH:MM:SS.FFFFFFFFF',
                 'YYYY.mm.ddDHH:MM:SS.FFFFFF']) :
    sep='D';
  elif (fmt in ['YYYY-mm-dd HH:MM:SS.FFFFFF','YYYY-MM-DD HH:MM:SS.FFFFFF','YYYY-mm-dd HH:MM:SS.FFFFFFFFF',
                 'YYYY.mm.dd HH:MM:SS.FFFFFF', '%Y-%m-%d %H:%M:%S.%f']) :
    sep=' ';
  elif (fmt in ['HH:MM:SS.FFFFFF', '%H:%M:%S.%f', '%H:%M:%S.ffffff','%H:%M:%S.fffffffff',
                'HH:MM:SS.%f', '%h:%m:%s.%f', '%H:%M:%S.%F']) :
    return(stt);
  return(('' if in_dt=='' else (in_dt + sep)) + stt);
def rand_dealer() :
  return(str(np.random.choice(['dealer1','dealer2'],size=1)[0]))
def rand_sender() :
  return(str(np.random.choice(['sender1','sender2','sender3'],size=1)[0]))

def rand_ipt(nm, typ, fixlist=[], width=10,scale=0, colname="rnd", in_dt='2025-09-15',fmttime='YYYY-mm-dd HH:MM:SS.FFFFFF') :
  if (nm == 'dealer') :
    return(str(rand_dealer()));
  elif (nm == 'sender') :
    return(str(rand_sender()));
  elif ((typ == 'tms') or (typ=='tns') or (typ=='tus')) :
    return(str(rand_datetime(in_dt=in_dt, fmt=fmttime)));
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
    if (colname is not None) and (isinstance(colname,str)) :
      if colname in ['ABs', 'AorB','AorBs','AB', 'AORB'] :
        return(str(np.random.choice(['{A:}','{B:}'])));
      elif colname in ['bsentry', 'bsentry1'] :
        return(str(np.random.choice(['Z','C','D','E'])));
      elif colname == 'nbz' :
        return(str(np.random.choice(['n','b','z','c','d'])))
      elif colname == 'stock' :
        return(str(np.random.choice(['SPY','QQQ','ZZZ'])));
      elif colname == 'dealer' :
        return(rand_dealer());
      elif colname == 'sender' :
        return(rand_sender());
    return(str(np.random.choice(['hello','boy','what','joe','me','ok'], size=1)[0]))
  else :
    return(str(rand_i32()));

example_exdir =  "/example";
example_exfile = "ex_random.csv";

schM=[]; fixK=[];
def create_random_file(exdir=example_exdir, exfile=example_exfile, verbose=1,
  schM=schM, fixK=fixK, general_sep=",", nfx=3, nLines=300) :
  vstr = "create_random_file("+exdir+","+exfile+",v="+str(verbose)+"):";
  if (verbose >= 1) :
    print(vstr + "fake_data.py -- we are about to write to exfile = \"" + exfile + "\"");
  sK = list(schM.keys());
  with open(exdir + "/" + exfile, 'w') as wfile :
    for ii in range(nLines) :
      if ((verbose >=1) and ((ii+1) % 1000 == 0)) :
        print(vstr+" --- -- on line ii=" + str(ii) + "/" + str(nLines));
      myL = list(); onfixgrp = [];
      for jj in range(len(sK)) :
        onS = schM[sK[jj]];
        if (onS['typ'] == 'fix42') :
            newKeys, nf = rand_fix(nfx, fixK=fixK, ngrp=onfixgrp, general_sep=general_sep, fixequal = ':' if 'fixequal' not in onS else onS['fixequal'], fixstyle="fixjson");
            onfixgrp = list(set(list(onfixgrp) + newKeys));
            myL.append(nf)
        elif (onS['typ'] == 'fix2end') :
            newKeys, nf = rand_fix(nfx, fixK=fixK, ngrp=onfixgrp, general_sep=general_sep, fixequal = ':' if 'fixequal' not in onS else onS['fixequal'], fixstyle="fix2end");
            onfixgrp = list(set(list(onfixgrp)))
            myL.append(nf);
        elif (onS['typ'] in ['Decimal','decimal']) :
            myL.append(rand_ipt(sK[jj], onS['typ'], fixlist=[], scale = onS['scale'], width=onS['width'], colname=onS['nm']));
        else :
            myL.append(rand_ipt(sK[jj], onS['typ'], fixlist=[], colname=onS['nm'], fmttime='YYYY-mm-dd HH:MM:SS.ffffff' if 'fmt' not in onS.keys() else onS['fmt']));
      wfile.write( general_sep.join(myL) + "\n");
  if verbose >= 1 :
    print(vstr + " all done writing " + str(nLines) + " lines.");

if __name__ == '__main__' :
  print("Launch Data Generator: arguments len " + str(len(sys.argv)) + "[" + ",".join([str(x)+":"+sys.argv[x] for x in range(len(list(sys.argv)))]) + "]");
  config_file = 'fix42.json' if len(sys.argv) < 2 else str(sys.argv[1]);
  config_dir = '' if len(sys.argv) < 4 else str(sys.argv[3]);
  verbose = 2 if (len(sys.argv) < 6) else int(str(sys.argv[5]));
  nLines = nLines if (len(sys.argv) < 7) else int(str(sys.argv[6]));
  nfx = nfx if (len(sys.argv) < 8) else int(str(sys.argv[7]));
  print(" -- running load_config_file=" + config_file + ": nLines=" + str(nLines) + ", nfx=" + str(nfx));
  #jhome, fx2d, fixK, schM, fix2M, general_sep)
  jhome, fx2d, fixK, schM, fix2M, general_sep = load_config_file(config_file=config_file, config_dir=config_dir);
  print(" --  Are we ready jhome = " + jhome);
  example_exdir = jhome + "/../example";
  example_exfile = example_exdir + "/ex_random.csv";
  exfile = example_exfile if len(sys.argv) < 3 else str(sys.argv[2]);
  exdir = example_exdir if len(sys.argv) < 5 else str(sys.argv[4]);
  print(" -- Create_random_file now with " + str(exdir) + ", exfile=" + str(exfile) + ", len(schM) = " + str(len(schM)) + ", to exdir=" + exdir);
  create_random_file(exdir=exdir, exfile=exfile, verbose=verbose, schM=schM, fixK=fixK, general_sep=general_sep, nLines=nLines, nfx=nfx);

# python py/fake_data.py 'fix42_t2.json' ex_random2.csv config_jsons example 2 400 3 
