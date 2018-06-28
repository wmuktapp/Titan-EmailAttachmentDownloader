"""Download attachments from an email account that match given criteria and upload directly to Titan's blob storage.

For more help, please execute the following at the command prompt:
emailattachmentdownloader --help

"""


import datetime
import email
import imaplib
import re
import sys

import click


class EmailDownloaderError(Exception):
    """An error was encountered during the matching of the criteria or attachment download/upload process."""


class _DateType(click.ParamType):
    """Initialise a custom click type to be used to validate a date provided at command line input."""

    name = "Date (YYYY-MM-DD)"

    def convert(self, value, param, ctx):
        """Check that the input is a valid FTP connection string on a standard regex pattern.

        This method is called implicitly by click and shouldn't be invoked directly.

        Positional Arguments:
        1. value (string): the value that is to be validated / converted
        2. param (unknown): (unknown as not documented by click). This value should be passed to the second parameter of
        the fail() method
        3. ctx (unknown): (unknown as not documented by click). This value should be passed to the third parameter of
        the fail() method

        """
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            self.fail("Incorrect date format, should be YYYY-MM-DD")


class TitanFlowManager(object):
    def __init__(self, imap_ssl_host, username, password, fetch_one, match_date_received, email_subject, email_sender,
                 filename_pattern, archive_folder, load_date):
        """Initialise an object that controls the flow of the application.

        Positional Arguments:
        1. imap_ssl_host (string): The hostname/port to connect to using IMAP over SSL. Note that the default port,
        993, is assumed
        2. username (string): The username to connect to the IMAP server with. Typically, this is the full email address
        3. password (string): The password to connect to the IMAP server with
        4. fetch_one (bool): Whether to return after uploading one email with attachments that match the criteria or
        all emails
        5. match_date_received (bool): If True, the search will be restricted to emails that were received the day
        after the load_date value
        6. email_subject (string): The regular expression pattern to use to match email subjects
        7. email_sender (string): The regular expression pattern to use to match email senders
        8. filename_pattern (string): The regular expression pattern to use to match attachment filenames
        9. archive_folder (string): If provided, all 'matched' emails will be moved to this folder. If the archive
        folder is a sub folder, use parent/child syntax. Note that the existence of this folder is checked immediately
        to avoid a situation where attachments are succesfully downloaded & uploaded but the emails unmoved
        10. load_date (datetime.date): the load_date which is inused unless match_date_received is True

        """
        self.imap_ssl_host = imap_ssl_host
        self.username = username
        self.password = password
        self.fetch_one = fetch_one
        self.match_date_received = match_date_received
        self.email_subject = re.compile(email_subject)
        self.email_sender = re.compile(email_sender)
        self.filename_pattern = re.compile(filename_pattern)
        self.archive_folder = archive_folder
        self.load_date = load_date

        from datalake import utilities
        self.acquire_program = utilities.AcquireProgram()
        self.logger = self.acquire_program.logger

    @staticmethod
    def raise_if_not_ok(response, message):
        """Helper function raises an EmailDownloadError is the response is not "OK".

        Positional Arguments:
        1. response (string): the first argument returned by imap.uid calls
        2. message (list): the second argument returned by imap.uid calls

        """
        if response != "OK":
            raise EmailDownloaderError(message[0].decode())

    def archive_uids(self, imap, uids):
        """Move the emails identified by the uids out of the inbox and into the archive folder.

        Positional Arguments:
        1. imap (imaplib.IMAP4_SSL): the logged in imap object to use to interact with the email server
        2. uids (iterable): the iterable containing the email unique IDs to archive

        """
        imap.select()  # get out of readonly mode for the moving
        for uid in uids:
            self.raise_if_not_ok(*imap.uid("COPY", uid, self.archive_folder))
            self.raise_if_not_ok(*imap.uid("STORE", uid, "+FLAGS", "(\Deleted)"))
        imap.expunge()

    def get_attachments(self, imap):
        """Yield tuples of (uid, attachment) for each email+attachment that matches the given criteria.

        If the archive folder is provided, check this exists first, if not raise an EmailDownloaderError. Then, iterate
        through emails (most recent to old) - either all or restricted to the day after the load_date if
        match_date_received was True. For each email, check that the subject and sender matches the given patterns and
        if so, iterate through the attachments, only yielding values if the attachment filename also matches the
        provided pattern. If fetch_one is True, yield the first matching email's matching attachments, otherwise yield
        all matches. Finally, if 0 matches are found, raise an EmailDownloaderError.

        Positional Arguments:

        1. imap (imaplib.IMAP4_SSL): the logged in imap object to use to interact with the email server

        """
        if self.archive_folder is not None:
            self.raise_if_not_ok(*imap.select(self.archive_folder, readonly=True))
        imap.select(readonly=True)
        if self.match_date_received:
            on_value = (self.load_date - datetime.timedelta(days=1)).strftime("%d-%b-%Y")
            uids = imap.uid("search", "ON", on_value)
        else:
            uids = imap.uid("search", None, "ALL")[1][0].split()
        uids.reverse()
        attachments_found = False
        for uid in uids:
            raw_mail = imap.uid("fetch", uid, "(RFC822)")[1][0][1]
            mail = email.message_from_bytes(raw_mail, _class=email.message.EmailMessage)
            if self.email_subject.match(mail["Subject"]) and self.email_sender.match(mail["From"]):
                for attachment in mail.iter_attachments():
                    if self.filename_pattern.match(attachment["Content-Description"]):
                        attachments_found = True
                        yield uid, attachment
                if self.fetch_one and attachments_found:
                    return
        if not attachments_found:
            raise EmailDownloaderError("0 attachments were found matching the criteria.")

    def run(self):
        """Run the end to end download and upload process."""
        self.logger.info("EXECUTION STARTED")
        self.logger.info("Connecting to, and authenticating with, the IMAP server over SSL...")
        with imaplib.IMAP4_SSL(self.imap_ssl_host) as imap:
            imap.login(self.username, self.password)
            self.logger.info("Getting attachments that match the provided criteria...")
            attachments = self.get_attachments(imap)
            uids_to_move = set()
            self.logger.info("Recording all email unique IDs that need to be archived...")
            self.logger.info("Uploading attachments...")
            for uid, attachment in attachments:
                self.upload(attachment)
                uids_to_move.add(uid)
            self.logger.info("Archiving matched emails...")
            if self.archive_folder is not None:
                self.archive_uids(imap, uids_to_move)
        self.logger.info("EXECUTION FINISHED")

    def upload(self, attachment):
        """Upload the attachment's byte payload to Titan's blob storage.

        Positional Arguments:
        1. attachment (email.message.EmailMessage): the attachment whose byte payload should be uploaded

        """
        blob_name = self.acquire_program.get_blob_name("{TITAN_DATA_SET_NAME}_{TITAN_LOAD_DATE}_{file_name}",
                                                       file_name=attachment["Content-Description"])
        self.acquire_program.create_blob_from_bytes(attachment.get_payload(decode=True), blob_name=blob_name)


@click.command()
@click.option("-h", "--imap-ssl-hostname", required=True, help="The hostname/port to connect to using IMAP over SSL. "
              "Note that the default port, 993, is assumed.")
@click.option("-u", "--username", required=True, help="The username to connect to the IMAP server with. Typically, "
              "this is the full email address.")
@click.option("-p", "--password", required=True, help="The password to connect to the IMAP server with.")
@click.option("-f", "--fetch-one", type=bool, required=True, help="Whether to return after uploading one email with "
              "attachments that match the criteria or all emails.")
@click.option("-m", "--match-date-received", type=bool, default=False, help="If True, the search will be restricted to "
              "emails that were received the day after the --load-date value. Defaults to False.")
@click.option("-e", "--email-subject", default=".*", help="The regular expression pattern to use to match email "
              "subjects. Defaults to r\".*\"")
@click.option("-s", "--email-sender", default=".*", help="The regular expression pattern to use to match email "
              "senders. Defaults to r\".*\"")
@click.option("-n", "--filename-pattern", default=".*", help="The regular expression pattern to use to match "
              "attachment filenames. Before a regex match is sought, any instances of YYYY, MM and DD "
              "(case-sensitive) will be replaced by the year, month and date respectively of the --load-date. Defaults "
              "to r\".*\"")
@click.option("-a", "--archive-folder", help="If provided, all 'matched' emails will be moved to this folder. If the "
              "archive folder is a sub folder, use parent/child syntax. Note that the existence of this folder is "
              "checked immediately to avoid a situation where attachments are succesfully downloaded & uploaded but "
              "the emails unmoved. Defaults to None.")
@click.option("-l", "--load-date", type=_DateType(), help="If provided, must be in the format of YYYY-MM-DD. Defaults "
              "to yesterday.")
def main(imap_ssl_host, username, password, fetch_one, match_date_received, email_subject, email_sender,
         filename_pattern, archive_folder, load_date):
    """Download attachments from an email account that match given criteria and upload directly to Titan's blob storage.

    Look for emails containing attachments that match the provided pattern and download either the most recent one
    email's attachments or all email's attachments, failing if there are no files. For more information, execute the
    following at the command prompt: ftpfiledownloader --help

    """
    if load_date is None:
        load_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    yyyy, mm, dd = str(load_date).split("-")
    filename_pattern = filename_pattern.replace("YYYY", yyyy).replace("MM", mm).replace("DD", dd)
    flow_manager = TitanFlowManager(imap_ssl_host, username, password, fetch_one, match_date_received, email_subject,
                                    email_sender, filename_pattern, archive_folder, load_date)
    try:
        flow_manager.run()
    except Exception as error:
        flow_manager.logger.exception(error)
        sys.exit("ERROR ENCOUNTERED - CHECK LOGS")
