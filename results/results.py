import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="whitegrid")

internet_by_country = pd.read_csv('internet_by_country.csv')
internet_by_gridid = pd.read_csv('internet_by_gridid.csv')
sms_call_by_country = pd.read_csv('sms_call_by_country.csv')
sms_call_by_gridid = pd.read_csv('sms_call_by_gridid.csv')

plt.figure(figsize=(12, 6))
sns.barplot(
    x='countrycode',
    y='total_internet_transfer',
    data=internet_by_country,
    palette='Blues_d'
)
plt.yscale('log')
plt.xlabel('Country Code', fontsize=12)
plt.ylabel('Total Internet Transfer (Log Scale)', fontsize=12)
plt.title('Total Internet Transfer by Country (Log Scale)', fontsize=16)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 6))
sns.barplot(
    x='GridID',
    y='total_internet_transfer',
    data=internet_by_gridid,
    palette='Oranges_d'
)
plt.yscale('log')
plt.xlabel('Grid ID', fontsize=12)
plt.ylabel('Total Internet Transfer (Log Scale)', fontsize=12)
plt.title('Total Internet Transfer by GridID (Log Scale)', fontsize=16)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 6))
sns.barplot(
    x='countrycode',
    y='total_sms_count',
    data=sms_call_by_country,
    color='green',
    label='Total SMS Count'
)
sns.barplot(
    x='countrycode',
    y='total_call_time',
    data=sms_call_by_country,
    color='blue',
    alpha=0.7,
    label='Total Call Time'
)
plt.yscale('log')
plt.xlabel('Country Code', fontsize=12)
plt.ylabel('Count / Time (Log Scale)', fontsize=12)
plt.title('SMS and Call Data by Country (Log Scale)', fontsize=16)
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 6))
sns.barplot(
    x='GridID',
    y='total_sms_count',
    data=sms_call_by_gridid,
    color='purple',
    label='Total SMS Count'
)
sns.barplot(
    x='GridID',
    y='total_call_time',
    data=sms_call_by_gridid,
    color='red',
    alpha=0.7,
    label='Total Call Time'
)
plt.yscale('log')
plt.xlabel('Grid ID', fontsize=12)
plt.ylabel('Count / Time (Log Scale)', fontsize=12)
plt.title('SMS and Call Data by GridID (Log Scale)', fontsize=16)
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()