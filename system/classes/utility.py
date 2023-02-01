import tldextract
from urllib.parse import urlparse
from os.path import splitext

class URLManager(object):
    def getDomain(self, url):
        url_obj = tldextract.extract(url)
        domain = url_obj.domain +'.'+ url_obj.suffix
        if url_obj.subdomain != "" and url_obj != "www":
            domain = url_obj.subdomain +'.'+ domain

        if 'www.' in domain[0:4]:
            domain = domain.replace("www.", "", 1)

        return domain

    def getDomainAndProtocol(self, url):
        url_obj = tldextract.extract(url)
        domain = url_obj.domain +'.'+ url_obj.suffix
        if url_obj.subdomain != "" and url_obj != "www":
            domain = url_obj.subdomain +'.'+ domain

        if 'www.' in domain[:4]:
            domain = domain.replace("www.", "", 1)
            
        return domain, self.getProtocol(url)

    def getProtocol(self, link):
        if 'https' in link[:5]:
            return 'https'
        else:
            return 'http'

    def getLinkWithoutProtocol(self, link):
        link = link.replace(self.getProtocol(link)+"://","", 1)
        if 'www.' in link[:4]:
            link = link.replace("www.", "", 1)

        return link

    def linkNormalize(self, link):
        if 'https' in link[:5]:
            return link
        elif 'http' in link[:4]:
            return link
        elif '//' in link[:2]:
            return 'http:'+link
        else:
            return link

    def getCategorizedLink(self, domain, domain_protocol, link):

        try:
            link = self.linkNormalize(link)
            link_no_protocol = self.getLinkWithoutProtocol(link)
            domain_no_protocol = self.getLinkWithoutProtocol(domain)

            link_protocol = self.getProtocol(link)
            """
            if 'stumbleupon.com' in link or 'www.' in link or 'http://' in link_no_protocol or '//' in link_no_protocol:
                print("CIAO")
            """

            if domain == self.getDomain(link):
                return "internal_link", link_no_protocol, domain_protocol # INTERNAL
            elif ('http://' in link[:7] or 'https://' in link[:8]) and 'javascript' not in link and 'mailto' not in link and '#' != link:
                return "external_link", link_no_protocol, link_protocol
            elif '/' == link[0] and '//' != link[:2] and 'mailto:' not in link:
                return 'internal_link', domain_no_protocol + link_no_protocol, domain_protocol
            elif '//' == link[:2] and 'mailto:' not in link:
                return 'external_link', link_no_protocol, link_protocol
            elif '/' != link[0] and '://' not in link and 'mailto:' not in link:
                return 'internal_link', domain_no_protocol +'/'+ link_no_protocol, domain_protocol
            else:
                # '#' != link:
                return 'unknown_link_tag', link_no_protocol, None
        except:
            return 'unknown_link_tag', link_no_protocol, None

    def adjustURL(self, url, fromHttpsToHttp=True):
        
        # from HTTPS to HTTP
        if fromHttpsToHttp:
            if url[:5] == "https":
                url = "http"+url[5:]
            elif url[:5] != "http:":
                url = "http:"+url

            # add // 
            if url[5:7] != "//":
                url = url[:5]+"//"+url[5:]
        
            # remove www
            url = url.replace("http://www.", "http://")

        elif url[:5] == "https":

            # add // 
            if url[6:8] != "//":
                url = url[:6]+"//"+url[6:]

            # remove www
            url = url.replace("https://www.", "https://")

        elif url[:5] == "http:":

            # add // 
            if url[5:7] != "//":
                url = url[:5]+"//"+url[5:]

            # remove www
            url = url.replace("http://www.", "http://")
        else:
            url = "http:"+url

            # add // 
            if url[5:7] != "//":
                url = url[:5]+"//"+url[5:]

            # remove www
            url = url.replace("http://www.", "http://")

        # remove url parameters
        o = urlparse(url)
        url = o.scheme + "://" + o.netloc + o.path

        # remove the final /
        if url[-1] == "/":
            url = url[:-1] 

        # remove the #internal_link part
        pos = url.find("#")
        if pos > -1:
            url = url[0: pos]
            
        return url

    """
    def listExtension(self, type_s):

        if type_s == "VIDEO":
            return ['.m1v', '.mpeg', '.mov', '.qt', '.mpa', '.mpg', '.mpe', '.avi', '.movie', '.mp4']
        elif type_s == "AUDIO":
            return ['.ra', '.aif', '.aiff', '.aifc', '.wav', '.au', '.snd', '.mp3', '.mp2']
        elif type_s == "IMAGE":
            return ['.ras', '.xwd', '.bmp', '.jpe', '.jpg', '.jpeg', '.xpm', '.ief', '.pbm', '.tif', '.gif', '.ppm', '.xbm', '.tiff', '.rgb', '.pgm', '.png', '.pnm']
        elif type_s == "COMPUTER_FILE":
            return ['.txt', '.pptx','.ppt','.ods','.xlsx','.xls','.pdf','.docx','.doc']
        else:
            return []
    """

    def getExtensionAndPageType(self, url):
        
        path = urlparse(url.lower()).path
        url_ext = splitext(path)[1]

        #### TWITTER
        if 'twitter.com' in self.getDomain(url) or 'twimg.com' in self.getDomain(url):
            if 'profile_images/' not in url and 'format=' in url: 
                url_ext = "."+url[url.find("format="):url.find("&name=")].replace("format=", "")

        VIDEO = ['.m1v', '.mpeg', '.mov', '.qt', '.mpa', '.mpg', '.mpe', '.avi', '.movie', '.mp4'] #, '.m3u8']
        AUDIO = ['.ra', '.aif', '.aiff', '.aifc', '.wav', '.au', '.snd', '.mp3', '.mp2']
        IMAGE = ['.svg', '.ras', '.xwd', '.bmp', '.jpe', '.jpg', '.jpeg', '.xpm', '.ief', '.pbm', '.tif', '.gif', '.ppm', '.xbm', '.tiff', '.rgb', '.pgm', '.png', '.pnm']
        COMPUTER_FILE = ['.txt', '.pptx','.ppt','.ods','.xlsx','.xls','.pdf','.docx','.doc', '.zip', '.rar']
        MOBILE_FILE = ['.apk', '.ipa']

        if url_ext in VIDEO:
            return url_ext.replace(".", ""), "video"
        elif url_ext in AUDIO:
            return url_ext.replace(".", ""), "audio"
        elif url_ext in IMAGE:
            return url_ext.replace(".", ""), "image"
        elif url_ext in MOBILE_FILE:
            return url_ext.replace(".", ""), "mobile_app"
        elif url_ext in COMPUTER_FILE:
            return url_ext.replace(".", ""), "computer_file"
        
        if url_ext.replace(".", "").isnumeric():
            return "", "page"
        else:
            return url_ext.replace(".", ""), "page"

    def elaboratePointer(self, url, domain, domain_protocol, referenced_as_tag):
        field_name, field_value, field_protocol = self.getCategorizedLink(domain, domain_protocol, url)
        point_to = None
        
        if field_name != "unknown_link_tag":
            # APPEND A NEW POINT TO OBJECT
            point_to = {
                "referenced_as_tag": referenced_as_tag
            }

            point_to['extension_s'], point_to['type_s'] = self.getExtensionAndPageType(field_value) # EXTENSION

            # HTTPS
            if field_protocol == "https":
                point_to['https'] = True
            else:
                point_to['https'] = False

            point_to['domain'] = self.getDomain(field_value)
            point_to['full_url_s'] = field_protocol+"://"+field_value
            point_to['url'] = field_value

        return field_name, field_value, point_to

    def imageToInclude(self, field_value):
        if 'facebook' not in field_value and \
            'stumbleupon' not in field_value and \
            'paypal' not in field_value and \
            'instagram' not in field_value and \
            'google' not in field_value and \
            'reddit' not in field_value and \
            'digg' not in field_value and \
            'linkedin' not in field_value and \
            'pinterest' not in field_value and \
            'telegram' not in field_value and \
            'twitter' not in field_value:
                return True
        
        return False

import random
class UserAgentManager(object):

	def __init__(self):
		self.list = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
                    "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9",
                    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
                    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1"]
		self.shuffleAndGet()

	def shuffleAndGet(self):
		random.shuffle(self.list)
		return self.list[0]