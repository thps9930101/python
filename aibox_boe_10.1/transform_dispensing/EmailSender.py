import smtplib
import ssl
from email.message import EmailMessage
from typing import List, Optional
import mimetypes


class EmailSender:
    # =======================
    #  信件基本設定
    # =======================
    def __init__(self, smtp_host: str, smtp_port: int,
                 username: str, password: str, use_tls: bool = True):
        """
        初始化郵件寄送器
        - smtp_host: SMTP 主機，例如 smtp.gmail.com
        - smtp_port: 587(TLS) 或 465(SSL)
        - username/password: 帳號密碼（Gmail 用 App Password）
        - use_tls: True=STARTTLS(port 587), False=SSL(port 465)
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_tls = use_tls

    # =======================
    #  信件傳送設定
    # =======================
    def send(self, subject: str, to_addrs: List[str],
             body_text: Optional[str] = None,
             body_html: Optional[str] = None,
             attachments: Optional[List[str]] = None) -> bool:
        """
        寄送郵件，成功回傳 True。
        """

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.username
        msg["To"] = ", ".join(to_addrs)

        # ---- 郵件內容 ----
        if body_html and body_text:
            msg.set_content(body_text)
            msg.add_alternative(body_html, subtype="html")
        elif body_html:
            msg.set_content("HTML email. Please view in HTML client.")
            msg.add_alternative(body_html, subtype="html")
        elif body_text:
            msg.set_content(body_text)
        else:
            msg.set_content("")

        # ---- 附件 ----
        if attachments:
            for path in attachments:
                try:
                    with open(path, "rb") as f:
                        data = f.read()

                    ctype, encoding = mimetypes.guess_type(path)
                    if ctype is None:
                        maintype, subtype = "application", "octet-stream"
                    else:
                        maintype, subtype = ctype.split("/", 1)

                    filename = path.split("/")[-1]
                    msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)
                except Exception as e:
                    print(f"[warn] attach {path} failed: {e}")

        # ---- 寄送 ----
        try:
            context = ssl.create_default_context()

            if self.use_tls:
                with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                    server.ehlo()
                    server.starttls(context=context)
                    server.ehlo()
                    server.login(self.username, self.password)
                    server.send_message(msg)
            else:
                with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, context=context, timeout=30) as server:
                    server.login(self.username, self.password)
                    server.send_message(msg)

            print("Email sent successfully.")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to send email: {e}")
            return False
        

if __name__ == "__main__":

    sender = EmailSender(
        smtp_host="smtp.office365.com",
        smtp_port=587,
        username="no-reply1@lightmatrix3d.com",
        password="MeowMeow3d",
        use_tls=True,
    )

    ok = sender.send(
        subject="Test Email from EmailSender Class",
        to_addrs=[
            "faye@lightmatrix3d.com"
        ],
        body_text="這是一封測試郵件",
        body_html="<h3>這是一封 HTML 測試郵件</h3><p>Hello!</p>",
        attachments=None,
    )

    print("Sent:", ok)