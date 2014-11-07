__author__ = 'Jimin.Zhou'
class News:
    def __init__(self, _oid, newsID,companyID, newsTitle, publishTime, storyID=-1):
        """

        :type self: object
        """
        self.id=_oid
        self.newsID = newsID
        self.companyID = companyID
        self.newsTitle = newsTitle
        self.publishTime = publishTime
        self.storyID = -1
        self.isMain = False
        self.storyID = storyID