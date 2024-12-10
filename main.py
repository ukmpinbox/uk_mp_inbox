#pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

print('  __  __ _____         ______ __  __          _____ _             _    _ _____  _____       _______ ______ _____    ')
print(' |  \/  |  __ \       |  ____|  \/  |   /\   |_   _| |           | |  | |  __ \|  __ \   /\|__   __|  ____|  __ \   ')
print(' | \  / | |__) |      | |__  | \  / |  /  \    | | | |           | |  | | |__) | |  | | /  \  | |  | |__  | |__) |  ')
print(' | |\/| |  ___/       |  __| | |\/| | / /\ \   | | | |           | |  | |  ___/| |  | |/ /\ \ | |  |  __| |  _  /   ')
print(' | |  | | |           | |____| |  | |/ ____ \ _| |_| |____       | |__| | |    | |__| / ____ \| |  | |____| | \ \   ')
print(' |_|  |_|_|           |______|_|  |_/_/    \_\_____|______|       \____/|_|    |_____/_/    \_\_|  |______|_|  \_\  ')
print('                                                                                                                    ')
print('                                                                                                                    ')

print('Running...')
import os
os.chdir('C:/Users/Harry.Woolford/Documents/MP_email/')


import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]


def main():
    """Shows basic usage of the Drive v3 API.
  Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("drive", "v3", credentials=creds)

        # Call the Drive v3 API
        results = (
            service.files()
            .list(pageSize = 10, fields = "nextPageToken, files(id, name)")
            .execute()
        )
        items = results.get("files", [])
    
        if not items:
            return
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")
      

if __name__ == "__main__":
    main()

print('Authorisation complete...')
from simplegmail import Gmail

gmail = Gmail()

# Starred messages
messages = gmail.get_starred_messages()
#messages = gmail.get_sent_messages()
# ...and many more easy to use functions can be found in gmail.py!
print('Emails imported...')

# 1.
import pandas as pd
from simplegmail.query import construct_query

# 2.
gmail = Gmail()

# 3.
labels = gmail.list_labels()
mp_label = list(filter(lambda x: x.name == '1_all_mps', labels))[0]

# 4.
messages = gmail.get_messages(labels=[mp_label])

# 5.
def get_label_names(label_ids):
    label_names = []
    for label_id in label_ids:
        label = next(filter(lambda x: x.id == label_id, labels), None)
        if label and label.name != 'UNREAD':
            label_names.append(label.name)
    return sorted(label_names, key=lambda x: (x != '1_all_mps', x))

# 6. 
# Create an empty list to store data
all_emails = []

# 7.
for message in messages:
    all_emails.append({
        #"To": message.recipient, #uk_mp_inbox is the recipient for all
        #"Label": message.label_ids, # This works, but leaves labels out of order
        "Msg_id":       message.id,
        "From":         message.sender,
        "Labels":       get_label_names(message.label_ids),
        "CC":           message.cc,
        "Subject":      message.subject,
        "Date":         message.date,
        "Preview":      message.snippet,
        "Message_body": message.plain,  # or message.html
        "Message_html": message.html
    })

# 8.
all_emails_df = pd.DataFrame(all_emails)
print('Emails reformated...')
# Function to convert HTML to plain text and remove newline characters
from bs4 import BeautifulSoup

def extract_text(html):
    if not pd.isna(html):
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        
        # Break into lines and remove leading and trailing spaces on each
        lines = (line.strip() for line in text.splitlines())
        
        # Break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        
        # Drop blank lines and join the text
        text = '\n'.join(chunk for chunk in chunks if chunk)
        
        return text
    else:
        return None

# Assume 'all_emails_df' is your DataFrame and 'Message_html' is the column name
all_emails_df['body_extracted'] = all_emails_df['Message_html'].apply(extract_text)


# Extract Newsletter_title and Email using regular expressions
all_emails_df[['newsletter_title', 'email']] = all_emails_df['From'].str.extract(r'^(.*?)\s*<([^>]+)>')


all_emails_df['Labels'] = all_emails_df['Labels'].astype(str)
all_emails_df['unique_label'] = all_emails_df['Labels'].str.split(',').str[1].str.strip().str.replace("'", '').str.rstrip(']')


import pytz

# Convert the 'Date' column to datetime and make sure it's timezone-aware
all_emails_df['Date'] = pd.to_datetime(all_emails_df['Date'], utc=True)

# Define the cutoff date and make it timezone-aware with the same timezone as 'Date'
cutoff_date = pd.to_datetime('2024-07-04').tz_localize('UTC')

# Create the 'gen_elect' column based on the condition
all_emails_df['gen_elect'] = all_emails_df['Date'].apply(lambda x: 2019 if x <= cutoff_date else 2024)

# Remove the timezone information to convert 'Date' back to a naive datetime
all_emails_df['Date'] = all_emails_df['Date'].dt.tz_convert(None)


all_emails_df['Date'] = all_emails_df['Date'].astype(str)

# Now write to JSON

mp_roster_2019 = pd.read_csv("list_of_mps_2024_05.csv", encoding='utf-8-sig')


mp_roster_2024 = pd.read_csv("list_of_mps_2024_09.csv", encoding='utf-8-sig')


mp_roster = pd.concat([mp_roster_2019, mp_roster_2024], ignore_index=True)


filtered_df = mp_roster[mp_roster['mnis_id'] == "5283"]


full_mp_inbox_df = pd.merge(all_emails_df, mp_roster, how='inner', on=['unique_label', 'gen_elect'])

print('Saving full inbox...')
full_mp_inbox_df.columns = full_mp_inbox_df.columns.str.lower()


columns_to_include = ['msg_id',
    'subject',
    #'message_html',
    'body_extracted',
    'date',
    'gen_elect',
    'mnis_id',
    'first_name',
    'last_name',
    'party',
    'constituency',
    'gender',
    'in_office_since']  # Replace with the actual column names to be included

truncated_df = full_mp_inbox_df.loc[:, columns_to_include]

#rename body_extracted
truncated_df = truncated_df.rename(columns={'body_extracted': 'body'})
print('Saving truncated inbox...')


truncated_df['date'] = truncated_df['date'].astype(str)

# Now write to JSON
truncated_df.to_json('output/trunc_mp_inbox_df.json', orient='records', indent=4)

print('Pushing to GitHub...')

os.chdir('C:/Users/Harry.Woolford/Documents/MP_email/output/')

import subprocess


# Initialize a Git repository
subprocess.run(["git", "init"])

# Add the JSON file to the repository
subprocess.run(["git", "add", "C:/Users/Harry.Woolford/Documents/MP_email/output/trunc_mp_inbox_df.json"])

# Commit the changes
subprocess.run(["git", "commit", "-m", "Automatic update"])

# Push to GitHub (replace with your repository URL)
subprocess.run(["git", "remote", "add", "origin", "https://github.com/ukmpinbox/uk_mp_inbox.git"])
subprocess.run(["git", "push", "-f", "origin", "master"])

print('COMPLETED')