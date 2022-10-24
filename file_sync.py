#!/usr/bin/env python3
import os
import requests
import re
import gzip
import shutil
from bs4 import BeautifulSoup as bs
from bs4 import SoupStrainer as ss

class fileSynchronizator:

    def __init__(self, url="https://portal.cisjr.cz", folder="archives"):

        self.url = url
        self.folder = folder

        # Make Folder if the folder do not exist
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)

            if not os.path.exists(self.folder+"/canceled"):
                os.makedirs(self.folder+"/canceled")

    def download_and_unzip(self, archiveName, remotePath, subFolder="", prefix = ""):
        # Get archive from ftp server
        req = requests.get(self.url+remotePath, stream=True)
        with open("./"+archiveName, 'wb') as file:
            for chunk in req.iter_content(chunk_size=128):
                file.write(chunk)

        os.mkdir("./tmp")
        try:
            shutil.unpack_archive(
                filename=archiveName, extract_dir='./tmp'
            )
        except shutil.ReadError:
            with gzip.open("./"+archiveName, 'rb') as f_in:
                with open("./tmp/"+archiveName.replace(".zip",""), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

        for filename in os.listdir("./tmp"):
            shutil.move("./tmp/"+filename, "./"+self.folder+subFolder+prefix+filename)


        shutil.rmtree("./tmp")
        if os.path.exists("./"+archiveName):
            os.remove("./"+archiveName)

    def get_all_xmls(self):
        # Get html page from url
        links = bs(requests.get(self.url+"/pub/draha/celostatni/szdc/").text,
                   'html.parser', parse_only=ss('a'))

        # Iterate through all years
        for year_link in links:
            # Mach only year format
            if re.match(r'[0-9][0-9][0-9][0-9]', year_link.text):
                year = bs(requests.get(
                    self.url+year_link["href"]).text, 'html.parser', parse_only=ss('a'))

                # Iterate through all months
                for month_link in year:
                    # Months
                    if re.match(r'[0-9][0-9][0-9][0-9]-[0-9][0-9]', month_link.text):
                        month = bs(requests.get(
                            self.url+month_link["href"]).text, 'html.parser', parse_only=ss('a'))
                        for item in month:
                            if re.match(r'cancel.*', item.text):
                                self.download_and_unzip(item.text, item["href"], subFolder="canceled/")
                            elif re.match(r'.*zip', item.text):
                                self.download_and_unzip(item.text, item["href"], prefix=month_link.text)
                    # Archive with scedule for whole year
                    elif re.match(r'GVD[0-9][0-9][0-9][0-9].zip', month_link.text):
                        self.download_and_unzip(month_link.text, month_link["href"])
        return


if __name__ == "__main__":
    sync = fileSynchronizator()

    sync.get_all_xmls()
