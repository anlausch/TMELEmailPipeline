'''
Created on 29.03.2016

@author: anlausch
'''
import os
import codecs
import re
from dateutil.parser import parser

class DataImporter(object):
    '''
    This class is for importing textual data from the file system
    '''

    def __init__(self, path, number_docs):
        '''
        Constructor
        @param {String} path: path to the directory
        @param {Integer} number_docs: number of docs to be imported 
        '''
        self.path = path
        self.number_docs = number_docs
    
    
    def filter_web_addresses(self, line):
        '''
        Filters out web addresses using a regex pattern and saves them into a file
        @param {String} line: input string to which the filter shall be applied 
        '''
        p_web_address = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        matches = re.findall(p_web_address, line)
        if len(matches) > 0:
            with open(".\\..\\output\\web_addresses.log", "a") as log:
                for match in matches:
                    for group in match:
                        if group != "" and group != " ":
                            log.write(group)
                            line = line.replace(group, " _ ")
                    log.write("\n")
        return line
    
    
    def filter_image_artefacts(self, line):
        '''
        Filters out image artefacts using a regex pattern and saves them into a file
        @param {String} line: input string to which the filter shall be applied 
        '''
        p_image = '\[IMAGE\]'
        matches = re.findall(p_image, line)
        if len(matches) > 0:
            with open(".\\..\\output\\images.log", "a") as log:
                for match in matches:
                    for group in match:
                        if group != "" and group != " ":
                            log.write(group)
                            line = line.replace(group, " _ ")
                    log.write("\n")
        return line
    
    
    def filter_email_addresses(self, line):
        '''
        Filters out email addresses using a regex pattern and saves them into a file
        @param {String} line: input string to which the filter shall be applied 
        '''
        p_email_address = '([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\s)|([a-zA-Z0-9_.+-\/]+@[a-zA-Z0-9-]+\.?.[a-zA-Z0-9-.]+)'
        matches = re.findall(p_email_address, line)
        if len(matches) > 0:
            with open(".\\..\\output\\email_addresses.log", "a+") as log:
                for match in matches:
                    for group in match:
                        if group != "":
                            log.write(group. replace('\n', ''))
                            line = line.replace(group, " _ ")
                    log.write("\n")
        return line
    
    
    def filter_dates(self, line):
        '''
        Filters out dates using a regex pattern and saves them into a file
        @param {String} line: input string to which the filter shall be applied 
        '''
        p_date = '\d{1,2}\/\d{1,2}\/\d{4}'
        matches = re.findall(p_date, line)
        if len(matches) > 0:
            with open(".\\..\\output\\dates.log", "a") as log:
                for match in matches:
                    for group in match:
                        if group != "":
                            log.write(group)
                            line = line.replace(group, " _ ")
                    log.write("\n")
        return line
    
    
    def filter_noise(self, line):
        '''
        Filters out noise by calling several functions that use regex patterns
        @param {String} line: input string to which the filter shall be applied 
        '''
        line = self.filter_web_addresses(line)
        line = self.filter_email_addresses(line)
        line = self.filter_dates(line)
        line = self.filter_image_artefacts(line)
        return line
    
    
    def get_thread_id(self, line):
        '''
        Returns the thread id using a regex pattern
        @param {String} line: input string to which the filter shall be applied 
        '''
        p_threadid = re.compile('ThreadID: ([0-9]+)')
        matches = re.findall(p_threadid, line)
        if len(matches) > 0:
            return matches[0]
        else:
            return ""
    
    
    def get_subject(self, line):
        '''
        Returns the subject using a regex pattern
        @param {String} line: input string to which the filter shall be applied 
        '''
        p_subject = re.compile('Subject: (.*)')
        matches = re.findall(p_subject, line)
        if len(matches) > 0:
            subject = matches[0].replace('Re: ', '').replace('RE: ','').replace('Fw: ', '').replace('FW: ','').replace('Fwd: ','').replace('FWD: ','')
            return subject
        else:
            return ""
    
    
    def get_date_sent(self, line):
        '''
        Returns the sent date using a regex pattern
        @param {String} line: input string to which the filter shall be applied 
        '''
        p_date = re.compile('Date: (.*)\\r')
        matches = re.findall(p_date, line)
        if len(matches) > 0:
            dateparser = parser()
            date = dateparser.parse(matches[0])
            return date
        else:
            return ""
    
    
    def gen_read_threads(self):
        '''
        Generator that reads threads from the file system, 
        applies several filters and yields the textual data
        '''
        data = dict();
        for root, dirs, files in os.walk(self.path):  # @UnusedVariable
            thread_content = ""
            thread_id = ""
            original = ""
            if (len(files) > 0 and len(data) < self.number_docs) or (self.number_docs==-1 and len(files) > 0):
                for f in files:
                    try:
                        if (len(data) < self.number_docs and ".original" in f) or (self.number_docs==-1 and ".original" in f):
                            original = codecs.open(os.path.join(root, f), "r", "utf-8").read()
                            thread_id = self.get_thread_id(original)
                            thread_date = self.get_date_sent(original)
                            thread_content += self.get_subject(original)
                        elif (len(data) < self.number_docs and not ".original" in f) or (self.number_docs==-1 and not ".original" in f):
                            file_data_lines = codecs.open(os.path.join(root, f), "r", "utf-8").readlines()
                            is_body = False
                            for line in file_data_lines:
                                if is_body == True:
                                    line = self.filter_noise(line)
                                    thread_content += line
                                if self.get_thread_id(line) != "":
                                    is_body = True
                        elif len(data) == self.number_docs:
                            return
                    except Exception as e:
                        print("[ERROR] %s" % e)
                        break;
                if thread_id != "":
                    yield dict([('id', thread_id), ('content', thread_content), ('path', root), ('original', original), ('timestamp', thread_date)])
                    data[thread_id] = ""
            elif len(data) >= self.number_docs and self.number_docs != -1:
                return
        return