from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
import sklearn.model_selection

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import confusion_matrix
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
from sklearn import metrics

spark = SparkSession.builder.appName("Prediction").getOrCreate()

accounts_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb").option("dbtable", "accountstable").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").load()
customers_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb").option("dbtable", "customerstable").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").load()

transactions_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb").option("dbtable", "transactionstable").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").load()

accounts_df = accounts_df.toPandas()
customers_df = customers_df.toPandas()
transactions_df = transactions_df.toPandas()


# Join DataFrames
new_accounts_df = accounts_df.join(customers_df, "Customer_ID")
final_merged_df = new_accounts_df.join(transactions_df, "Account_ID")

# Show the resulting DataFrame
final_merged_df.show()

final_merged_df['Churn'].replace({"Yes":1, "No":0}, inplace=True)

final_merged_df.head()
# Get DataFrame column data types
print(final_merged_df.dtypes)

# Create features (X) and target (y) variables
X = final_merged_df.drop(['Churn','Account_ID','Name','Age','Address','Postcode','Transaction_ID','Open_Date'], axis=1)
y = final_merged_df['Churn']

# Replace categorical values with numerical values
final_merged_df['Credit_Card'].replace({"Yes":1, "No":0}, inplace=True)
final_merged_df['Gender'].replace({"Female":1, "Male":0}, inplace=True)
final_merged_df['Account_Type'].replace({"savings":1, "current":0}, inplace=True)

# Drop unnecessary columns from X
X = X.drop(['Churn','Account_ID','Name','Age','Address','Postcode','Transaction_ID','Open_Date'], axis=1)
X = X.drop(['Last_Activity_Date','Phone_Number','Email'], axis=1)

# Convert 'Transaction_Date' to datetime
X['Transaction_Date'] = pd.to_datetime(X['Transaction_Date'], format="%d/%m/%Y")

def modify_amount(row):
    if row['Transaction_Type'] == 'Deposit':
        return row['Amount']
    elif row['Transaction_Type'] == 'Withdrawal' or row['Transaction_Type'] == 'onlinepayment':
        return -row['Amount']
    else:
        return None

# Apply the function to create a new 'Modified_Amount' column
X['Modified_Amount'] = X.apply(modify_amount, axis=1)

# Drop the original 'Amount' column if needed
X = X.drop(columns=['Amount'])

# Drop the 'Transaction_Type' column
X = X.drop(columns=['Transaction_Type'])

# Rename the 'Modified_Amount' column to 'Transaction_Amount'
X = X.rename(columns={'Modified_Amount': 'Transaction_Amount'})

# Print unique values in the 'Country' column
print(X['Country'].unique())

# Check unique values in 'Employment_Status' before one-hot encoding
print(X['Employment_Status'].unique())

#Data Pre-Processing
cat_features = ['Employment_Status', 'Country']
X1 = pd.get_dummies(X, columns=cat_features)

# Replace True/False with 1/0 in the newly created dummy columns
X1['Employment_Status_Employed'] = X1['Employment_Status_Employed'].replace({True: 1, False: 0})
X1['Employment_Status_Retired'] = X1['Employment_Status_Retired'].replace({True: 1, False: 0})
X1['Employment_Status_Selfemployed'] = X1['Employment_Status_Selfemployed'].replace({True: 1, False: 0})
X1['Employment_Status_Unemployed'] = X1['Employment_Status_Unemployed'].replace({True: 1, False: 0})
X1['Employment_Status_Unknown'] = X1['Employment_Status_Unknown'].replace({True: 1, False: 0})
X1['Country_France'] = X1['Country_France'].replace({True: 1, False: 0})
X1['Country_Germany'] = X1['Country_Germany'].replace({True: 1, False: 0})
X1['Country_Spain'] = X1['Country_Spain'].replace({True: 1, False: 0})

# Fill NaN values with -1
X1 = X1.fillna(-1)

# Convert 'Transaction_Date' to int (you may need to convert it to appropriate integer representation)
X1['Transaction_Date'] = pd.to_datetime(X1['Transaction_Date'], format='%Y-%m-%d')
X1['Transaction_Date'] = X1['Transaction_Date'].astype(int)

# Drop 'Transaction_Date' from X1 and store it in X2
X2 = X1['Transaction_Date']
X1 = X1.drop(['Transaction_Date'], axis=1)

# Show X1 DataFrame
print(X1.head())