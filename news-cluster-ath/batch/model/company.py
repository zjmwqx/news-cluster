__author__ = 'root'


class Company:
    def __init__(self, companyID):
        self.companyID = companyID
        self.newsList = []
    def addNews(self, nws):
        self.newsList.append(nws)