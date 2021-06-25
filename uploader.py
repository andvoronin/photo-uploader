#!/usr/bin/env python

#import argparse
import os
import sqlite3 as sq
import datetime
#import time
#from tkinter import *
import hashlib
import exifread
import shutil



def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
     """
     Call in a loop to create terminal progress bar
     @params:
         iteration   - Required  : current iteration (Int)
         total       - Required  : total iterations (Int)
         prefix      - Optional  : prefix string (Str)
         suffix      - Optional  : suffix string (Str)
         decimals    - Optional  : positive number of decimals in percent complete (Int)
         length      - Optional  : character length of bar (Int)
         fill        - Optional  : bar fill character (Str)
     """
     percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
     filledLength = int(length * iteration // total)
     bar = fill * filledLength + '-' * (length - filledLength)
     print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
     # Print New Line on Complete
     if iteration == total: 
         print()
 
def exiftag(fname,tagnames):
    '''
    Get exif tag values with certain names
    
    Parameters
    ----------
    fname : str
        path to file.
    tagnames : list(str)
        list of tags to be extracted.

    Returns
    -------
    tuple
        tag values in order provided in tagnames parameter

    '''
    with open(fname, 'rb') as f:
        tags = exifread.process_file(f)
        res = [tags.get(tag) for tag in tagnames]
        return tuple(tagval.values if not tagval is None else None for tagval in res)


def md5(fname):
    '''
    Get md5 hash of a file

    Parameters
    ----------
    fname : string
        path to file.
    '''
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

class Dbhelper():
    
    LAUNCHES_TAB_NAME = "LaunchesHistory"
    DEST_FILES_TAB_NAME = "AllFilesInfo"
    FILES_FOR_MOVING_TAB_NAME = "FilesChangingQueue"
    DB_FILENAME = "uploaderhistory.db"
    

    def __init__(self,destfolder):
        #self.conn = sq.connect(Dbhelper.DB_FILENAME)
        #TO-DO use createconnection method?
        self.rootfolder = destfolder
        self.filename = os.path.join(destfolder,Dbhelper.DB_FILENAME)
        self.connection =  sq.connect(self.filename)

        '''Creating launches history table'''

        query = '''CREATE TABLE if not exists {} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    Executedate DateTime NOT NULL,
                    Params Text NOT NULL)'''.format(Dbhelper.LAUNCHES_TAB_NAME)

        self.connection.execute(query)
        
        '''
            Main table, that stores information about all files in folder
        '''
        query = '''CREATE TABLE if not exists {} (
                    file_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    filename VARCHAR NOT NULL,
                    extension VARCHAR NULL,
                    size INTEGER NOT NULL,
                    filehash VARCHAR NOT NULL,
                    creationdate DATETIME NOT NULL,
                    creationdatesource VARCHAR NOT NULL CHECK (creationdatesource in ('Exif','File_creation_date','Manual')),
                    subfolder VARCHAR NULL, 
                    lastmodifiedbylaunch INTEGER NULL
                    )'''.format(Dbhelper.DEST_FILES_TAB_NAME)
        #           
        #creationdatesource VARCHAR NOT NULL CHECK (creationdatesource in 'Exif','File_creation_date','Manual'),
        
        self.connection.execute(query)

        '''
            Creating table with filenames that should be renamed/moved
        '''
        query = '''CREATE TABLE if not exists {} (
                    row_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    launch_id INTEGER NOT NULL,
                    file_id INTEGER NULL,
                    source_file_id INTEGER NULL,
                    action VARCHAR NOT NULL CHECK (action in ('Move','Upload')),
                    sourcefilepath VARCHAR NOT NULL,
                    subfolder VARCHAR NOT NULL,
                    destfilename VARCHAR NOT NULL
                    )'''.format(Dbhelper.FILES_FOR_MOVING_TAB_NAME)

        self.connection.execute(query)

        self.connection.commit()
        #self.connection.close()

    

    def commit(self):
        self.connection.commit()



    def create_launch(self,params):
        query = "INSERT INTO {} (Executedate, Params) Values (?, ?)".format(Dbhelper.LAUNCHES_TAB_NAME)
        self.connection.execute(query,(datetime.datetime.now(),str(params)))
        launchidquery = "SELECT last_insert_rowid()"
        cursor = self.connection.cursor()
        cursor.execute(launchidquery)

        launchid = cursor.fetchone()[0]
        '''Сreating table for storing single launch data'''
        tabname = self.__launchtabname__(launchid)

        query = '''CREATE TABLE if not exists {} (
                    source_file_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    sourcefilepath VARCHAR NOT NULL,
                    extension VARCHAR NULL,
                    size INTEGER NOT NULL,
                    filehash VARCHAR NOT NULL,
                    creationdate DATETIME NOT NULL,
                    creationdatesource VARCHAR NOT NULL CHECK (creationdatesource in ('Exif','File_creation_date')),
                    isduplicate INT DEFAULT(0),
                    subfolder VARCHAR NOT NULL
                    )'''.format(tabname)
        self.connection.execute(query)
        
        
        
        self.connection.commit()
        #self.connection.close()
        return launchid

    def __launchtabname__(self, launchid):
        return "DataFromLaunch_{}".format(launchid)

    
    
 
    def insertnewfileinfo(self, launchid, filename, extension, size, filehash, creationdate, creationdatesource,subfolder):

            query = "INSERT INTO {} (filename,extension,size,filehash,creationdate,creationdatesource,subfolder,lastmodifiedbylaunch) Values(?,?,?,?,?,?,?,?)".format(Dbhelper.DEST_FILES_TAB_NAME)
            connection = self.connection
            cursor=connection.cursor()
            cursor.execute(query,(filename, extension, size, filehash, creationdate, creationdatesource,subfolder,launchid))
            lastid = cursor.lastrowid
            connection.commit()
            #connection.close()
            return lastid

    def insertsourcefileinfo(self, launchid, filepath, extension, size,  filehash, creationdate, creationdatesource):

            subfolder = os.path.join(creationdate.strftime("%Y"),creationdate.strftime("%m"))
            query = "INSERT INTO {} (sourcefilepath,extension, size, filehash,creationdate,creationdatesource,subfolder) Values(?,?,?,?,?,?,?)".format(self.__launchtabname__(launchid))
            connection = self.connection
            cursor=connection.cursor()
            cursor.execute(query,(filepath, extension, size, filehash, creationdate, creationdatesource, subfolder))
            lastid = cursor.lastrowid
            #connection.commit()
            #connection.close()
            return lastid

    def changefiledestination(self, fileid, subfolder, filename):
           query = "UPDATE {} SET subfolder = ?, filename = ? WHERE file_id =?".format(Dbhelper.DEST_FILES_TAB_NAME)
           self.connection.execute(query,(subfolder,filename, fileid))

    def getsourcefileinfo(self, launchid, source_file_id):
        '''
        gets file information from table that stores info about files we want to upload
        '''
        sourcetabname = self.__launchtabname__(launchid)
        query = 'SELECT  sourcefilepath, extension, size,filehash,creationdate, creationdatesource FROM {} WHERE source_file_id = ? AND isduplicate = 0'.format(sourcetabname)
        cursor = self.connection.cursor()
        cursor.execute(query,(source_file_id,))
        return cursor.fetchone()

    def getfileinfo(self, file_id):
        '''
        gets file information from table  with info about files in datastore 
        '''
        query = 'SELECT  filename, subfolder, extension, size, filehash, creationdate, creationdatesource FROM {} WHERE file_id = ?'.format(Dbhelper.DEST_FILES_TAB_NAME)
        cursor = self.connection.cursor()
        cursor.execute(query,(file_id,))
        return cursor.fetchone()



    def markduplicatesinsource(self,launchid):
        sourcetabname = self.__launchtabname__(launchid)
        query = '''UPDATE {0} SET isduplicate = 1
                   WHERE EXISTS(SELECT * FROM {1} WHERE {0}.size = {1}.size AND {0}.filehash = {1}.filehash);
                '''.format(sourcetabname, Dbhelper.DEST_FILES_TAB_NAME)
        self.connection.execute(query)
        self.connection.commit()


    def fillqueue(self,launchid):
        '''
        Fill files list that should moved or uploaded
        '''
        sourcetabname = self.__launchtabname__(launchid)
        query = "SELECT DISTINCT SubFolder FROM {}".format(sourcetabname)
        connection = self.connection
        
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        '''
            using temptable to set order number of file
        '''
        for subFoldername in results:
            subfoldernamestr = subFoldername[0]
            temptabname = "tmp{}".format(subfoldernamestr.replace(os.sep,""))
            createtablequery = '''CREATE TEMPORARY TABLE {} (
                    NPP INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    source_file_id INTEGER NULL,
                    file_id INTEGER NULL,
                    extension VARCHAR NOT NULL,
                    action VARCHAR NOT NULL,
                    subfolder VARCHAR NOT NULL,
                    filename VARCHAR NULL
                    )'''.format(temptabname)
            connection.execute(createtablequery)

            # Ordering files by creation date 
            # Two files can have exactly the same creation date, and can switch from launch to launch
            # To minimize impact on performance ordering also by filehash
            fileNPPquery = '''INSERT INTO {}(source_file_id, file_id, extension, action,subfolder)
                  SELECT source_file_id, file_id, extension, action,subfolder FROM (
                  SELECT source_file_id, NULL As file_id, creationdate, extension, 'Upload' as action, subfolder, filehash FROM {} WHERE isduplicate = 0 AND subfolder = ?
                  UNION ALL
                  SELECT NULL, file_id, creationdate, extension, 'Move', subfolder, filehash FROM {} WHERE subfolder = ?
                  ) s
                  ORDER BY s.creationdate, s.filehash
                  '''.format(temptabname,sourcetabname,Dbhelper.DEST_FILES_TAB_NAME)
            #print(fileNPPquery)
            connection.execute(fileNPPquery,(subfoldernamestr,subfoldernamestr))
            updatequery = "UPDATE {} SET filename = '{}'||case when length(NPP)>5 then NPP else substr('00000'||NPP,-5,5) end||extension".format(temptabname, "F")
            connection.execute(updatequery)
            connection.commit()
            '''
            adding files to queue
            '''
            '''
                1. Files to be moved
            '''
            insertquery = '''INSERT INTO {} (launch_id, file_id,  action, sourcefilepath, subfolder, destfilename)
                             SELECT ?,ExistedFiles.file_id, tmp.action, ?||ExistedFiles.filename, tmp.subfolder,tmp.filename
                             FROM {} AS tmp INNER JOIN {} as ExistedFiles ON tmp.file_id = ExistedFiles.file_id
                             WHERE tmp.action = 'Move' AND (tmp.subfolder||tmp.filename)<>(ExistedFiles.subfolder||ExistedFiles.filename)
                             ORDER BY tmp.NPP DESC
                             '''.format(Dbhelper.FILES_FOR_MOVING_TAB_NAME,temptabname,Dbhelper.DEST_FILES_TAB_NAME)
            connection.execute(insertquery,(launchid,os.path.join(self.rootfolder,subfoldernamestr) + os.sep))
            '''
                2. Files to be uploaded
            '''
            insertquery = '''INSERT INTO {} (launch_id, source_file_id,  action, sourcefilepath, subfolder, destfilename)
                             SELECT ?,NewFiles.source_file_id, tmp.action, NewFiles.sourcefilepath, tmp.subfolder,tmp.filename
                             FROM {} AS tmp INNER JOIN {} as NewFiles ON tmp.source_file_id = NewFiles.source_file_id
                             WHERE tmp.action = 'Upload'
                             ORDER BY tmp.NPP DESC
                             '''.format(Dbhelper.FILES_FOR_MOVING_TAB_NAME,temptabname,sourcetabname)
            connection.execute(insertquery,(launchid,))
            connection.commit()


    def getqueue(self,launchid):        
        '''
        Gets all data for processing
        '''
        cursor = self.connection.cursor()
        query = "SELECT row_id,file_id,source_file_id, action, subfolder, destfilename FROM {} WHERE launch_id=? ORDER BY row_id".format(Dbhelper.FILES_FOR_MOVING_TAB_NAME)
        cursor.execute(query,(launchid,))
        return  cursor.fetchall() 

    '''
    returns list of subfolders in warehouse
    '''
    def getfolderslist(self):
        cursor = self.connection.cursor()
        query = "SELECT DISTINCT subfolder FROM {} ORDER BY subfolder".format(Dbhelper.DEST_FILES_TAB_NAME)
        cursor.execute(query)
        return [item[0] for item in cursor.fetchall()]

    def getfilelistbysubfolder(self,subfoldername):
        cursor = self.connection.cursor()
        query = query = 'SELECT  file_id, filename, extension, size, filehash, creationdate, creationdatesource FROM {} WHERE subfolder= ?'.format(Dbhelper.DEST_FILES_TAB_NAME)
        cursor.execute(query,(subfoldername,))
        #return [item[1] for item in cursor.fetchall()]
        return cursor.fetchall()

 
'''
class StoredFile():
    def __init__(self,fileid,fileinfo):
        self.id = fileid
        self.filename = fileinfo[0]
        self.subfolder = fileinfo[1]
        self.extension = fileinfo[2]
        self.size = fileinfo[3]
        self.filehash = fileinfo[4]
        self.creationdate = fileinfo[5]
        self.creationdatesource = fileinfo[6]
'''  
        


class Uploader():
    '''
    Basic class that proivides all methods for uploading and maintainance data in photo storage
    '''
    AVAILABLE_EXTENSIONS_FOR_DATE_EXTRACTING = (".JPG",".JPEG")

    def __init__(self,destfolder,deletesource = False):
        '''
        destfolder is the root folder of the storage
        '''
        self.rootfolder = destfolder
        if not os.path.exists(destfolder):
                os.makedirs(destfolder)
        self.dbhelper = Dbhelper(destfolder)
        self.deletesource = deletesource
        self.launch_id = None

    def __createlaunch__(self,params):
        self.launch_id = self.dbhelper.create_launch(params)



##    def __uploadfile__(self, filepath):
##        '''
##        Uploads file with known data to the storage. returns id of added file
##        '''
##        #fileinfo = self.__getfileinfofromfilesystem__(filepath)
##        #TO_DO Use kwargs?
##        newfileid = self.dbhelper.insertnewfileinfo(fileinfo[0],fileinfo[1],fileinfo[2],fileinfo[3],fileinfo[4],fileinfo[5])
##        return newfileid


    def __getfileinfofromfilesystem__(self,filepath):
        '''
        Returns file info as a tuple: filepath, extension, size,  filehash, creationdate, creationdatesource
        '''
        extension = os.path.splitext(filepath)[1]
        size = os.path.getsize(filepath)
        filehash = md5(filepath)
        creationdatesource = 'File_creation_date' # 'Exif',
        extension = extension.upper()
        
        if extension in Uploader.AVAILABLE_EXTENSIONS_FOR_DATE_EXTRACTING:
            '''try to get date from exiftag'''
            exifcreatedate, exifcreatedatemilisecs = exiftag(filepath,("EXIF DateTimeOriginal","EXIF SubSecTimeOriginal"))
            
            if not exifcreatedate is None:
                if not exifcreatedatemilisecs is None:
                    creationdate = datetime.datetime.strptime("{}.{}".format(exifcreatedate,exifcreatedatemilisecs), "%Y:%m:%d %H:%M:%S.%f")
                else:
                    creationdate = datetime.datetime.strptime(exifcreatedate, "%Y:%m:%d %H:%M:%S") 
                creationdatesource = 'Exif'

        '''
        creationdatesource will be 'Exif', if extracted succesful. if problem occured then extracting from file creation date
        '''
        if creationdatesource == 'File_creation_date':
                '''windows only'''
                
                #creationtime = time.localtime(os.path.getctime(filepath))
                #print(creationtime)
                #creationdate = time.strftime("%Y-%m-%d %H:%M:%S", creationtime)
                creationdate = datetime.datetime.fromtimestamp(os.path.getctime(filepath)) 
                
        return (filepath, extension, size,  filehash, creationdate, creationdatesource )


##    def __getsourcefileinfo__(self,launchid, source_file_id):
##        '''
##        gets file information from table that stores info about files we want to upload
##        '''
        
        
    
    def loaddatafromsource(self,sourcepath):
        '''
        loads data from source folder
        '''
        if self.launch_id  is None:
            self.__createlaunch__(None)

        
        for dirname, dirnames, filenames in os.walk(sourcepath):
            print("Processing folder {}".format(dirname))
            # print path to all filenames.
            for filename in filenames:
                fullfilename = os.path.join(dirname, filename)
                print(fullfilename)
                fileinfo = self.__getfileinfofromfilesystem__(fullfilename)
                #print(fileinfo)
                self.dbhelper.insertsourcefileinfo(self.launch_id,fileinfo[0],fileinfo[1],fileinfo[2],fileinfo[3],fileinfo[4],fileinfo[5] )
                #filescount +=1 
        self.dbhelper.commit()
        self.dbhelper.markduplicatesinsource(self.launch_id)
        self.dbhelper.fillqueue(self.launch_id)


    def processfiles(self):
        '''
        moves and uploades all files to the datastore root
        '''
        queue =  self.dbhelper.getqueue(self.launch_id)

        totalfilesforprocessing = len(queue)
        processedindex = 0
        
        # Files for moving cannot be moved directly, beecause in some situations
        # two files names should be intercahnged. First rename this files to 
        # temporary unique name, afer uploading moving to destination 
        
        #list of tuple (tempfilename, destfilename)
        filesformovingtemporarylist = []


        for fileinfo in queue:
            #print("processing file:", fileinfo)
            file_id = fileinfo[1]
            source_file_id  = fileinfo[2]
            action = fileinfo[3]
            subfolder = fileinfo[4]
            filename = fileinfo[5]
            
            destfilepath = os.path.join(self.rootfolder,subfolder,filename)

            destfolder = os.path.dirname(destfilepath)
            if not os.path.exists(destfolder):
                os.makedirs(destfolder)
            
            if action == 'Move' and not file_id is None:
                uploadfileinfo = self.dbhelper.getfileinfo(file_id)
                filepath =  os.path.join(self.rootfolder,uploadfileinfo[1],uploadfileinfo[0])
                tempfilepath = os.path.join(self.rootfolder,uploadfileinfo[1],str(file_id)+ '_' +uploadfileinfo[0])
                
                #Move file
                if not os.path.exists(tempfilepath):
                    shutil.move(filepath,tempfilepath)
                    #Saving temp file info, we will move it to destination after uploading new files
                    tempfiletuple = (tempfilepath,destfilepath)
                    filesformovingtemporarylist.append(tempfiletuple) 

                    #TO-DO change file location only after moving to destination, or change twice:
                    #First to temporary file, second to final Destination
                    self.dbhelper.changefiledestination(file_id, subfolder, filename)
                else:
                    print('\r\n')
                    raise Exception("File already exists. file_id=" + str(file_id) + ' Dest file=' + str(tempfilepath))


            elif action == 'Upload' and not source_file_id is None:
                sourcefileinfo = self.dbhelper.getsourcefileinfo(self.launch_id, source_file_id)
                sourcefilepath = sourcefileinfo[0]
                extension = sourcefileinfo[1]
                size = sourcefileinfo[2]
                filehash = sourcefileinfo[3]
                creationdate = sourcefileinfo[4]
                creationdatesource = sourcefileinfo[5]
                '''copy file'''
                if not os.path.exists(destfilepath):
                    if self.deletesource:
                        shutil.move(sourcefilepath,destfilepath)
                    else:
                        #print("source=",sourcefilepath," dest=",destfilepath) 
                        shutil.copy2(sourcefilepath,destfilepath)
                else:
                    print('\r\n')
                    raise Exception("File already exists. source_file_id=" + str(source_file_id))
                
                '''insert to database'''
                self.dbhelper.insertnewfileinfo(self.launch_id, filename, extension, size, filehash, creationdate, creationdatesource, subfolder)
                
            else:
                #To-Do error
                raise("Error copying file...")
            processedindex += 1
            printProgressBar(processedindex,totalfilesforprocessing, length = 60,prefix = "{}/{}".format(processedindex,totalfilesforprocessing))
        #Moving temporary files to destination
        print('Processing temporary files...')
        for tempfileformove in filesformovingtemporarylist:
            tempfilepath = tempfileformove[0]
            destfilepath = tempfileformove[1]
            print('Moving file {} to {}'.format(tempfilepath,destfilepath),end=' ')
            if not os.path.exists(destfilepath):
                shutil.move(tempfilepath,destfilepath)
                print('Ok')
            else:
                raise Exception("File already exists. destfilepath=" + destfilepath)    
            
            
        print('finished')
    

    def getlistcontents(self):
        '''
        returns dictionary of folders and folder contents
        '''
        res = {}
        folderslist =  self.dbhelper.getfolderslist()
        for subfoldername in folderslist:
            filesinsubfolder = self.dbhelper.getfilelistbysubfolder(subfoldername)
            res[subfoldername] = filesinsubfolder
        return res

    '''
    Gets stored file info from database as dictionary
    '''
    def getstoredfileinfo(self,fileid):
        fileinfo = {}
        fileinfotuple = self.dbhelper.getfileinfo(fileid)
        fileinfo["filename"], fileinfo["subfolder"], fileinfo["extension"],fileinfo["size"],fileinfo["filehash"], fileinfo["creationdate"], fileinfo["creationdatesource"] = fileinfotuple
        fileinfo["absolutepath"] = os.path.join(self.rootfolder,fileinfo["subfolder"],fileinfo["filename"]) 
        return fileinfo 

        
    def checkstorageintegrity(self, mode = 'fast'):
      contents = self.getlistcontents()
      errorslist =[]
      for subfolder, fileslist in contents.items():
          print("Checking folder {}".format(os.path.join(self.rootfolder,subfolder)))
          for fileinfo in fileslist:
              fullfilepath = os.path.join(self.rootfolder,subfolder,fileinfo[1]) 
              if not os.path.exists(fullfilepath):
                 errorslist.append([fullfilepath,'Missing file'])
                 if mode == 'full':
                     pass #check hashsum here

      return errorslist
  
            




    