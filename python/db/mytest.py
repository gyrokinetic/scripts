import subprocess

with open("/etc/resolv.conf", 'r') as fi:
   for aline in fi:
       if('nameserver' in aline):
          ip = aline.replace(r'nameserver', "").strip()
          print(ip)
