import smtplib, ssl

# TEMPORARY CLASS, REMOVED FROM THE CLIENT SIDE
class AlertManager(object):

	def __init__(self, receivers):
		self.lock = False
	
	def unlock(self):
		self.lock = False

	def sendAlert(self, messageText):
		self.lock = True