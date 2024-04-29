from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import confusion_matrix
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
from sklearn import metrics
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Prediction").getOrCreate()

accounts_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb").option("dbtable", "accountstable").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").load()
customers_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb").option("dbtable", "customerstable").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").load()

transactions_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb").option("dbtable", "transactionstable").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").load()

# Join DataFrames
new_accounts_df = accounts_df.join(customers_df, "Customer_ID")
final_merged_df = new_accounts_df.join(transactions_df, "Account_ID")

# Show the resulting DataFrame
final_merged_df.show()

final_merged_df.Churn.replace({"Yes":1, "No":0}, inplace=True)

final_merged_df.head()
final_merged_df.dtypes

X = final_merged_df.drop(['Churn','Account_ID','Name','Age','Address','Postcode','Transaction_ID','Open_Date'], axis=1) #features (independent variables)
y = final_merged_df['Churn'] #target (dependent variable)

final_merged_df.Credit_Card.replace({"Yes":1, "No":0}, inplace = True)
final_merged_df.Gender.replace({"Female":1, "Male":0}, inplace = True)
final_merged_df.Account_Type.replace({"savings":1, "current":0}, inplace = True)
X = final_merged_df.drop(['Churn','Account_ID','Name','Age','Address','Postcode','Transaction_ID','Open_Date'], axis=1) #features (independent variables)
X = X.drop(['Last_Activity_Date','Phone_Number','Email',], axis=1) #features (independent variables)
X['Transaction_Date'] = pd.to_datetime(X['Transaction_Date'], format = "%d/%m/%Y")

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

X = X.drop(columns=['Transaction_Type'])
X = X.rename(columns={'Modified_Amount': 'Transaction_Amount'})

print(X['Country'].unique())

# Check unique values in 'Employment_Status' before one-hot encoding
print(X['Employment_Status'].unique())

#Data Pre-Processing
cat_features = ['Employment_Status']
X1= pd.get_dummies(X, columns=cat_features)
cat_features = ['Country']
X1= pd.get_dummies(X1, columns=cat_features)
X1.Employment_Status_Employed.replace({"True":1, "False":0}, inplace = True)
X1.Employment_Status_Retired.replace({"True":1, "False":0}, inplace = True)
X1.Employment_Status_Selfemployed.replace({"True":1, "False":0}, inplace = True)
X1.Employment_Status_Unemployed.replace({"True":1, "False":0}, inplace = True)
X1.Employment_Status_Unknown.replace({"True":1, "False":0}, inplace = True)
X1.Country_France.replace({"True":1, "False":0}, inplace = True)
X1.Country_Germany.replace({"True":1, "False":0}, inplace = True)
X1.Country_Spain.replace({"True":1, "False":0}, inplace = True)

X1 = X1.fillna(-1)
X2 = X1['Transaction_Date']
X1 = X1.drop(['Transaction_Date'])
X1 = X1.astype(int)
X1.show()
#
# X_train, X_test, y_train, y_test = train_test_split(X1, y, test_size = 0.2, random_state=42)
#
# # Decision tree with giniIndex
# clf_gini = DecisionTreeClassifier(criterion = "gini",random_state = 100,max_depth=3, min_samples_leaf=5)
# # Performing training
# clf_gini.fit(X_train, y_train)
# # Predicton with giniIndex
# y_pred = clf_gini.predict(X_test)
# print("Decision Tree with giniIndex Results")
# print("Predicted values:",y_pred)
# print("Confusion Matrix: ",confusion_matrix(y_test, y_pred))
# print ("Accuracy : ",accuracy_score(y_test,y_pred)*100)
# print("Report : ",classification_report(y_test, y_pred))
#
# #AUC & ROC CURVE
# y_pred_proba = clf_gini.predict_proba(X_test)[::,1]
# fpr_DTGTEST, tpr_DTGTEST, _ = metrics.roc_curve(y_test,  y_pred_proba)
# auc_DTGTEST = metrics.roc_auc_score(y_test, y_pred_proba)
#
# #create ROC curve
# plt.plot(fpr_DTGTEST,tpr_DTGTEST,label="AUC DT gini Test="+str(auc_DTGTEST))
# plt.ylabel('True Positive Rate')
# plt.xlabel('False Positive Rate')
# plt.legend(loc=4)
# plt.title("ROC curve using Decission Tree with giniIndex")
# plt.show()
#
# X_test['y_pred'] = y_pred
#
#
# prediction_DF1 = final_merged_df[(final_merged_df['Customer_ID'].isin(X_test['Customer_ID'])) & (X_test['y_pred'] == 1)]
# print(prediction_DF1.head())
#
# churn_Prediction = prediction_DF1[["Customer_ID","Account_ID","Name","Phone_Number","Age","Address","Postcode","Country"]]
# print(churn_Prediction.head())
