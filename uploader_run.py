from uploader import Uploader
import datetime


if __name__ == '__main__':
    
    startdate  = datetime.datetime.now().strftime(r"%m.%d.%Y %H:%M:%S")
    uploader = Uploader(r'<Dest folder>')
    uploader.loaddatafromsource(r"Source folder")
    uploader.processfiles()
    enddate  = datetime.datetime.now().strftime(r"%m.%d.%Y %H:%M:%S")
    print("Finished. Start date {} End date {}".format(startdate,enddate))
