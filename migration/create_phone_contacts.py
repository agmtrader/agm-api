import pandas as pd

# Read the no_changed_email.csv file
df = pd.read_csv('outputs/no_changed_email.csv')

# Create a new dataframe with phone contact format
phone_contacts = pd.DataFrame()

# Use Title as Name, clean up the name field
phone_contacts['Name'] = df['Title'].str.strip()

# Use Phone Number, ensure it's clean
phone_contacts['Phone'] = df['Phone Number'].astype(str).str.replace('.0', '', regex=False)
phone_contacts['Phone'] = phone_contacts['Phone'].replace('nan', '')
phone_contacts['Phone'] = phone_contacts['Phone'].replace('', '')

# Use Email Address as primary email
phone_contacts['Email'] = df['Email Address'].fillna('')

# Remove rows where both phone and email are empty
phone_contacts = phone_contacts[
    (phone_contacts['Phone'] != '') | (phone_contacts['Email'] != '')
]

# Sort by name
phone_contacts = phone_contacts.sort_values('Name')

# Remove duplicates based on phone number only (keep first occurrence)
phone_contacts_dedup = phone_contacts.drop_duplicates(subset=['Phone'], keep='first')

# For contacts without phone numbers, also remove email duplicates
no_phone = phone_contacts[phone_contacts['Phone'] == '']
no_phone_dedup = no_phone.drop_duplicates(subset=['Email'], keep='first')

# Combine phone-based and email-based deduplication
final_contacts = pd.concat([
    phone_contacts_dedup[phone_contacts_dedup['Phone'] != ''],
    no_phone_dedup
])

# Sort by name again
final_contacts = final_contacts.sort_values('Name')

# Save to CSV
final_contacts.to_csv('outputs/phone_contacts.csv', index=False)

print(f"Original contacts: {len(phone_contacts)}")
print(f"After deduplication: {len(final_contacts)}")
print(f"Duplicates removed: {len(phone_contacts) - len(final_contacts)}")
print(f"Contacts with phone numbers: {len(final_contacts[final_contacts['Phone'] != ''])}")
print(f"Contacts with email addresses: {len(final_contacts[final_contacts['Email'] != ''])}") 