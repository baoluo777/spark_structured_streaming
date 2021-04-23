from bs4 import BeautifulSoup
import requests
import time

import smtplib
from email.mime.text import MIMEText
from email.header import Header

while 1 == 1:
    url = "http://rsj.wuhai.gov.cn/"
    f = requests.get(url)  # Get该网页从而获取该html内容
    soup = BeautifulSoup(f.content, "lxml")  # 用lxml解析器解析该网页的内容, 好像f.text也是返回的html
    result = f.content.decode()
    if result.__contains__("特殊工种"):
        # 第三方 SMTP 服务
        mail_host = "smtp.qq.com"  # 设置服务器
        mail_user = "5724589@qq.com"  # 用户名
        mail_pass = "rpzcqdzliyugbheh"  # 获取授权码
        sender = '5724589@qq.com'  # 发件人账号
        receivers = ['5724589@qq.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
        send_content = 'http://rsj.wuhai.gov.cn/'
        message = MIMEText(send_content, 'plain', 'utf-8')  # 第一个参数为邮件内容,第二个设置文本格式，第三个设置编码
        message['From'] = Header("特殊工种公告出来了！！！", 'utf-8')  # 发件人
        message['To'] = Header("我是收件人", 'utf-8')  # 收件人

        subject = '特殊工种公告出来了！！！'
        message['Subject'] = Header(subject, 'utf-8')
        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
            smtpObj.login(mail_user, mail_pass)
            smtpObj.sendmail(sender, receivers, message.as_string())
            print("邮件发送成功")
        except smtplib.SMTPException:
            print("Error: 无法发送邮件")
    else:
        print("false")
    time.sleep(300)
