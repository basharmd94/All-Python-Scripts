import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def send_mail(subject, bodyText, attachment=[], recipient = ['ithmbrbd@gmail.com']):
	me = "pythonhmbr12@gmail.com"
	you = recipient
	msg = MIMEMultipart('alternative')
	msg['Subject'] = subject
	msg['From'] = me
	msg['To'] = ", ".join(you)
	text = bodyText
#### if no attachment file provide in send mail argument then attachment part will ignore
	if not attachment: 
		part1 = MIMEText(text, "plain")
		msg.attach(part1)
	else:
		part1 = MIMEText(text, "plain")
		msg.attach(part1)

		for i in range (0, len(attachment)):
			part2 = MIMEBase('application', "octet-stream")
			part2.set_payload(open(attachment[i], "rb").read())
			encoders.encode_base64(part2)
			part2.add_header(f'Content-Disposition', 'attachment; filename="{}"'.format(attachment[i]))
			msg.attach(part2)

	username = 'pythonhmbr12@gmail.com'
	password = 'vksikttussvnbqef'

	s = smtplib.SMTP('smtp.gmail.com:587')
	s.starttls()
	s.login(username, password)
	s.sendmail(me,you,msg.as_string())
	s.quit()
