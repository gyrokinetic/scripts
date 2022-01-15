import sys
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS

class Photometa:

   def __init__(self, fname='test2.jpg'):
      self.fname = fname
      self.lon = 99999.
      self.lat = 99999.
      self.created = '1900-01-01 00:00:00'
      self.lon_ref = 'F'
      self.lat_ref = 'F'

      try:
         img = Image.open(fname)
   
         img_exif = img.getexif()
   
         for key, value in img_exif.items():
            name = TAGS.get(key, key)
            img_exif[name] = img_exif.pop(key)
            if(name == 'DateTimeOriginal'):
                self.created = str(value.replace(":", "-", 2))
            #print(name, value)
   
         if "GPSInfo" in img_exif:
            for key in img_exif['GPSInfo'].keys():
               name = GPSTAGS.get(key, key)
               img_exif['GPSInfo'][name] = img_exif['GPSInfo'].pop(key)
   
            self.exif = img_exif
      
            GPSJson = img_exif['GPSInfo']
      
            if('GPSLongitude' in GPSJson):
               self.lon = self.GetDegree(GPSJson['GPSLongitude'])
            if('GPSLatitude' in GPSJson):
               self.lat = self.GetDegree(GPSJson['GPSLatitude'])
            if('GPSLongitudeRef' in GPSJson):
               self.lon_ref = str(GPSJson['GPSLongitudeRef']).replace("'","")
            else:
               self.lon_ref = 'N'
            if('GPSLatitudeRef' in GPSJson):
               self.lat_ref = str(GPSJson['GPSLatitudeRef']).replace("'", "")
            else:
               self.lat_ref = 'E'

         else:
            self.lon = 99999.
            self.lat = 99999.
            self.lon_ref = 'M'
            self.lat_ref = 'M'

      except Exception as e:
         self.lon = 99999.
         self.lat = 99999.
         self.lon_ref = 'F'
         self.lat_ref = 'F'
         print("error=", str(e))

   def GetDegree(self, ang):
      deg = float(ang[0][0])/ang[0][1]
      min = float(ang[1][0])/ang[1][1]/60.
      sec = float(ang[2][0])/ang[2][1]/3600.
      return deg + min + sec

   def PrintMeta(self):
      print("time=", self.created)
      print("lat=", self.lat, self.lat_ref)
      print("lon=", self.lon, self.lon_ref)

if(len(sys.argv) > 1):
   fname = sys.argv[1]
else:
   fname = "test1.jpg"

photo = Photometa(fname)

photo.PrintMeta()
