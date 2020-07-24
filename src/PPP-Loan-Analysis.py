import pyspark as ps
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
import pandas as pd
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
import seaborn as sns
plt.style.use('fivethirtyeight')

spark = ps.sql.SparkSession.builder
        .master("local[4]")
        .appName("PPP")
        .getOrCreate()
sc = spark.sparkContext

df = spark.read.csv('The-United-States-of-PPP/All-Data/150k-minus/PPP-Data-150k-minus.csv',
                   header=True,
                   sep=",",
                   inferSchema=True)
#Clean the NonProfit column by filling N, which corresponds to For-Profit companies
df_cleanNP = df.na.fill('N','NonProfit')
# Drop remaining NA values from dataset
df_drop_na = df_cleanNP.na.drop()

#Slice existing NAICSCode to create new column containing NAICS Sector Code
def NAICS_Sector_Code_Column(number):
    return (int(str(number)[:2]))

udf_NAICS_Sector_Code_Column = udf(lambda x: NAICS_Sector_Code_Column(x))
df_NAICS_2_Digit = df_drop_na.withColumn('NAICS_Sector_Code',udf_NAICS_Sector_Code_Column(df_drop_na.NAICSCode))

#Import NAICS Sector Code Data
df_NAICS_Sector_Codes = spark.read.csv('The-United-States-of-PPP/NAICS_Data/NAICS_Sector_Code.csv',
                   header=True,
                   sep=",",
                   inferSchema=True)
# Join imported data to existing dataframe
df_SC = df_NAICS_2_Digit.join(df_NAICS_Sector_Codes,on='NAICS_Sector_Code')

#Slice existing NAICSCode to create new column containing NAICS Industry Group Code
def NAICS_Industry_Group_Column(number):
    return (int(str(number)[:4]))
    
udf_NAICS_Industry_Group_Column = udf(lambda x: NAICS_Industry_Group_Column(x))
df_IG = df_SC.withColumn('NAICS_Industry_Group_Code',udf_NAICS_Industry_Group_Column(df_SC.NAICSCode))

#Import NAICS Industry Group Code Data
df_NAICS_Industry_Group_Codes = spark.read.csv('The-United-States-of-PPP/NAICS_Data/NAICS_Industry_Group_Code.csv',
                   header=True,
                   sep=",",
                   inferSchema=True)
# Join imported data to existing dataframe
df_IG_join = df_IG.join(df_NAICS_Industry_Group_Codes,on='NAICS_Industry_Group_Code')

# Import NAICS Industry Code Data
df_NAICS_Industry_Codes = spark.read.csv('The-United-States-of-PPP/NAICS_Data/NAICS_Industry_Code.csv',
                   header=True,
                   sep=",",
                   inferSchema=True)
# Join imported data to existing dataframe
df_Ind_join = df_IG_join.join(df_NAICS_Industry_Codes,on='NAICSCode')

# Clean any remaining NA values
df_cleaned2 = df_Ind_join.na.drop()
# Final Cleaned Dataframe Count
df_cleaned2.count()

#STATE SUBSET
df_cleaned2.createOrReplaceTempView("State_DF")
state_df = spark.sql("SELECT State, COUNT(LoanAmount) as Loan_Count, SUM(LoanAmount)/1000000 as Loan_Amount, SUM(JobsRetained)/1000 as Jobs_Retained FROM State_DF GROUP BY State ORDER BY State")
# state_df.show(51) - For Visualization Purposes in Spark
df_state_data_temp = state_df.toPandas()

# Pull in State NAICS Data
df_NAICS_State_Counts = spark.read.csv('The-United-States-of-PPP/NAICS_Data/NAICS_State_Business_Counts.csv',
                   header=True,
                   sep=",",
                   inferSchema=True)
df_State_Bus_Counts = df_NAICS_State_Counts.toPandas()
df_state_data = df_state_data_temp.merge(df_State_Bus_Counts,on='State')

#Create columns
df_state_data['JR_LA'] = df_state_data['Jobs_Retained'] / df_state_data['Loan_Amount']
df_state_data['Percent_Funded'] = df_state_data['Loan_Count'] / df_state_data['NAICS_Business_count']
df_state_data['Natl_Avg_Loan_Size'] = df_state_data['Loan_Amount'].mean()*1000000 / df_state_data['Loan_Count'].mean()
df_state_data['State_Avg'] = df_state_data['Loan_Amount']*1000000 / df_state_data['Loan_Count']
df_state_data['State_vs_Natl'] = df_state_data['State_Avg'] - df_state_data['Natl_Avg_Loan_Size']
df_state_data['P_State_vs_Natl'] = df_state_data['State_Avg'] / df_state_data['Natl_Avg_Loan_Size']

#N_Loans_vs_N_Businesses_State
fig = plt.figure(figsize=(10,6))
plt.scatter(df_state_data['Loan_Count']/1000,df_state_data['NAICS_Business_count']/1000000, color='#008c8c',s=150)
plt.xlabel('Number of Loans (k)', fontweight='bold', color = '#173158')
plt.ylabel('Number of NAICS Businesses (M)', fontweight='bold', color = '#173158')
plt.gca().xaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
plt.title('Number of Loans vs. Number of Businesses by State', fontweight='bold', color = '#173158')
plt.ylim(0,2.25)
# plt.savefig('N_Loans_vs_N_Businesses_State.png')

#Loan_Amount_vs_N_Businesses_State
fig = plt.figure(figsize=(10,6))
plt.scatter(df_state_data['Loan_Amount'],df_state_data['NAICS_Business_count']/1000000, color='#008c8c',s=150)
plt.xlabel('Amount Loaned ($M)', fontweight='bold', color = '#173158')
plt.ylabel('Number of NAICS Businesses (M)', fontweight='bold', color = '#173158')
plt.gca().xaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
plt.title('Amount Loaned vs. Number of Businesses by State', fontweight='bold', color = '#173158')
plt.ylim(0,2.25)
# plt.savefig('Loan_Amount_vs_N_Businesses_State.png')

#Jobs Retained vs. Amount Loaned by State
JR_LA_Mean = df_state_data['JR_LA'].mean()
fig = plt.figure(figsize=(22,9))
plt.bar(df_state_data['State'],df_state_data['JR_LA'], color='#008c8c', width=0.7)
plt.xlabel('State',color='#173158', fontweight='bold')
plt.ylabel('Jobs Retained (k) per Amount Loaned (M)', color='#173158', fontweight='bold')
plt.axhline(y = JR_LA_Mean, color='#173158', alpha = 0.8)
plt.text(x=4,y=0.22,s='National Average',fontweight='bold', fontsize=16,color = '#173158',text= f'National Average {National_JR_LA_Avg}')
plt.title('Number of Jobs Retained vs. Amount Loaned by State', fontweight='bold', color = '#173158' )
# plt.savefig('JR_LA_State.png')

#Percentage of Businesses Funded by State
funded_mean = round(df_state_data['Percent_Funded'].mean(),4)
fig = plt.figure(figsize=(22,9))
plt.bar(df_state_data['State'],df_state_data['Percent_Funded'], color='#008c8c',width=0.7)
# plt.xaxis.label.set_color('#173158')
plt.xlabel('State',color='#173158', fontweight='bold')
plt.ylabel('Percentage of Businesses Funded', color='#173158', fontweight='bold')
plt.axhline(y = funded_mean, color='#173158', alpha = 0.8)
plt.text(x=2,y=0.32,s='National Average',fontweight='bold',fontsize = 16,color = '#173158',text= f'National Average {funded_mean:.2%}')
plt.gca().yaxis.set_major_formatter(StrMethodFormatter('{x:,.2%}'))
plt.title('Percentage of Businesses Funded by State', fontweight='bold', color = '#173158' )
plt.tight_layout()
# plt.savefig('P_Funded_State.png')

#Distribution of % of States Businesses Funded
funded_mean = df_state_data['Percent_Funded'].mean()
funded_std = df_state_data['Percent_Funded'].std()
dist_model = stats.norm(loc=funded_mean,scale=funded_std)
low_bound = dist_model.ppf(0.001)
high_bound = dist_model.ppf(0.999)
low_val_sig = dist_model.ppf(0.025).round(2)
high_val_sig = dist_model.ppf(0.975).round(2)
samples = 1000
x = np.linspace(low_bound,high_bound,samples)
x2 = np.linspace(5,40,num=250)
y = dist_model.pdf(x)
x2 = dist_model.pdf(df_state_data['Percent_Funded'])
fig, ax = plt.subplots(1, 1, figsize=(12,9))
ax.plot(x, y, c='#173158', label = 'Distribution')
ax.set_xlabel('% of Businesses Funded', color = '#173158', fontweight='bold')
ax.set_ylabel('PDF', color = '#173158', fontweight='bold')
ax.set_title('Distribution of % of States Businesses Funded', color = '#173158', fontweight='bold')
ax.axvline(x=low_val_sig,ymin=0.0,ymax=1,linestyle='--',linewidth=4,c='#4192c3')
ax.axvline(x=high_val_sig,ymin=0.0,ymax=1,linestyle='--',linewidth=4,c='#4192c3')
plt.gca().xaxis.set_major_formatter(StrMethodFormatter('{x:,.2%}'))
mask_hp = x > high_val_sig
mask_lp = x < low_val_sig
ax.fill_between(x, y, 0, 
                where=mask_hp | mask_lp, color="#008c8c", alpha=0.3, label=' 95% Confidence Interval')
plt.text(x=0.065,y=2.5,s='Lower Bound',fontweight='bold', fontsize=14,color = '#4192c3',text= f'Lower Bound {low_val_sig:.2%}')
plt.text(x=0.325,y=2.5,s='Upper Bound',fontweight='bold', fontsize=14,color = '#4192c3',text= f'Upper Bound {high_val_sig:.2%}')
ax.legend(loc='center')
plt.show()
# print(low_val_sig)
# print(high_val_sig)
# print((1 - dist_model.cdf(.3385))*100)
# plt.savefig('Dist_P_Funded.png')

#Distribution of Jobs Retained vs. Loan Amounts
funded_mean = df_state_data['JR_LA'].mean()
funded_std = df_state_data['JR_LA'].std()
dist_model = stats.norm(loc=funded_mean,scale=funded_std)
low_bound = dist_model.ppf(0.001)
high_bound = dist_model.ppf(0.999)
low_val_sig = dist_model.ppf(0.025).round(2)
high_val_sig = dist_model.ppf(0.975).round(2)
samples = 1000
x = np.linspace(low_bound,high_bound,samples)
x2 = np.linspace(5,40,num=250)
y = dist_model.pdf(x)
x2 = dist_model.pdf(df_state_data['JR_LA'])
fig, ax = plt.subplots(1, 1, figsize=(12,9))
ax.plot(x, y, c='#173158', label = 'Distribution')
ax.set_xlabel('Jobs Retained vs. Loan Amount', color = '#173158', fontweight='bold')
ax.set_ylabel('PDF', color = '#173158', fontweight='bold')
ax.set_title('Distribution of Jobs Retained vs. Loan Amounts', color = '#173158', fontweight='bold')
ax.axvline(x=low_val_sig,ymin=0.0,ymax=1,linestyle='--',linewidth=4,c='#4192c3')
ax.axvline(x=high_val_sig,ymin=0.0,ymax=1,linestyle='--',linewidth=4,c='#4192c3')
plt.gca().xaxis.set_major_formatter(StrMethodFormatter('{x:,.2%}'))
mask_hp = x > high_val_sig
mask_lp = x < low_val_sig
ax.fill_between(x, y, 0, 
                where=mask_hp | mask_lp, color="#008c8c", alpha=0.3, label=' 95% Confidence Interval')
plt.text(x=0.0775,y=3.25,s='Lower Bound',fontweight='bold', fontsize=14,color = '#4192c3',text= f'Lower Bound {low_val_sig:.2%}')
plt.text(x=0.1925,y=3.25,s='Upper Bound',fontweight='bold', fontsize=14,color = '#4192c3',text= f'Upper Bound {high_val_sig:.2%}')
ax.legend(loc='center')
plt.show()
# print(low_val_sig)
# print(high_val_sig)
# print((1 - dist_model.cdf(.238933))*100)
# plt.savefig('Dist_JR_LA.png')

#SECTOR SUBSET
df_cleaned2.createOrReplaceTempView("NAICS_SC")
naics_sc = spark.sql("SELECT NAICS_Sector, COUNT(LoanAmount) as Loan_Count, SUM(LoanAmount)/1000000 as Loan_Amount, SUM(JobsRetained)/1000 as Jobs_Retained FROM NAICS_SC GROUP BY NAICS_Sector ORDER BY NAICS_Sector")
# naics_sc.show(24) - For Visualization Purposes in Spark
df_sector_data_temp = naics_sc.toPandas()
#Pull in NAICS sector data
df_NAICS_Sector_Bus_Counts = spark.read.csv('The-United-States-of-PPP/NAICS_Data/NAICS_Sector_Business_Counts.csv',
                   header=True,
                   sep=",",
                   inferSchema=True)
df_sector_bus_counts = df_NAICS_Sector_Bus_Counts.toPandas()
df_sector_data = df_sector_data_temp.merge(df_sector_bus_counts,on='NAICS_Sector')
#Create columns
df_sector_data['JR_LA'] = (df_sector_data['Jobs_Retained'] / df_sector_data['Loan_Amount']).round(2)
df_sector_data['Percent_Funded'] = df_sector_data['Loan_Count'] / df_sector_data['NAICS_Sector_Count']
df_sector_data['Avg_Loan_Size'] = df_sector_data['Loan_Amount'].mean()*1000000 / df_sector_data['Loan_Count'].mean()
df_sector_data['Sector_Avg'] = df_sector_data['Loan_Amount']*1000000 / df_sector_data['Loan_Count']
df_sector_data['Sector_vs_Avg'] = df_sector_data['Sector_Avg'] - df_sector_data['Avg_Loan_Size']
df_sector_data['P_Sector_vs_Avg'] = df_sector_data['Sector_Avg'] / df_sector_data['Avg_Loan_Size']

#Sector funding as a % of average
threshold = 1
values = df_sector_data['P_Sector_vs_Avg']
x = df_sector_data['NAICS_Sector']
#Custom sector names used to shorten length for graphing/axis issues
x2 = ['Accom. & Food Serv.',
       'Admin./Support & Waste Mgmt. & ...',
       'Ag., Forestry, Fishing and Hunting',
       'Arts, Entertainment, & Rec', 'Construction',
       'Educational Serv.', 'Finance & Insurance',
       'Health Care & Social Assist.', 'Information',
       'Mgmt. of Companies & Enterprises', 'Manufacturing',
       'Mining, Quarrying, & Oil & Gas Extraction',
       'Other Serv. (except Public Admin.)',
       'Professional, Scientific, & Technical Serv.',
       'Public Admin.', 'Real Estate & Rental & Leasing',
       'Retail Trade', 'Transport. and Warehousing', 'Utilities',
       'Wholesale Trade']
above_threshold = np.maximum(values - threshold, 0)
below_threshold = np.minimum(values, threshold)
fig, ax = plt.subplots(figsize=(22,9))
ax.bar(x2, below_threshold, 0.7, color="#4192c3")
ax.bar(x2, above_threshold, 0.7, color="#173158",
        bottom=below_threshold)
ax.set_xticklabels(labels=x2,rotation=45,ha='right')
plt.gca().yaxis.set_major_formatter(StrMethodFormatter('{x:,.2%}'))
plt.axhline(y = 1, color='#008c8c', alpha = 0.8)
plt.ylabel('Percentage of Businesses Funded vs. Average', color='#173158', fontweight='bold')
plt.title('Sector Funding as a % of Average',color='#173158', fontweight='bold')
plt.ylim(0,1.50)
# plt.savefig('Sector_vs_Avg.png')

#INDUSTRY GROUP SUBSET
df_cleaned2.createOrReplaceTempView("NAICS_IG")
naics_ig = spark.sql("SELECT NAICS_Industry_Group, COUNT(LoanAmount) as Loan_Count, SUM(LoanAmount)/1000000 as Loan_Amount, SUM(JobsRetained)/1000 as Jobs_Retained FROM NAICS_IG GROUP BY NAICS_Industry_Group ORDER BY NAICS_Industry_Group")
# naics_ig.show() - For Visualization Purposes in Spark
df_ig_data_temp = naics_ig.toPandas()

df_NAICS_Ind_Bus_Counts = spark.read.csv('The-United-States-of-PPP/NAICS_Data/NAICS_Industry_Group_Business_Counts.csv',
                   header=True,
                   sep=",",
                   inferSchema=True)
df_ig_bus_counts = df_NAICS_Ind_Bus_Counts.toPandas()
df_ig_data = df_ig_data_temp.merge(df_ig_bus_counts,on="NAICS_Industry_Group")

#Create columns
df_ig_data['JR_LA'] = df_ig_data['Jobs_Retained'] / df_ig_data['Loan_Amount']
df_ig_data['Percent_Funded'] = df_ig_data['Loan_Count'] / df_ig_data['NAICS_Industry_Group_Count']
df_ig_data['Avg_Loan_Size'] = df_ig_data['Loan_Amount'].mean()*1000000 / df_ig_data['Loan_Count'].mean()
df_ig_data['Industry_Group_Avg'] = df_ig_data['Loan_Amount']*1000000 / df_ig_data['Loan_Count']
df_ig_data['Industry_Group_vs_Avg'] = df_ig_data['Industry_Group_Avg'] - df_ig_data['Avg_Loan_Size']
df_ig_data['P_Industry_Group_vs_Avg'] = df_ig_data['Industry_Group_Avg'] / df_ig_data['Avg_Loan_Size']

#INDUSTRY SUBSET
df_cleaned2.createOrReplaceTempView("NAICS_Code")
naics_code = spark.sql("SELECT NAICS_Industry, COUNT(LoanAmount) as Loan_Count, SUM(LoanAmount)/1000000 as Loan_Amount, SUM(JobsRetained)/1000 as Jobs_Retained FROM NAICS_Code GROUP BY NAICS_Industry ORDER BY NAICS_Industry")
# naics_code.show() - For Visualization Purposes in Spark
df_industry_data_temp = naics_code.toPandas()

df_NAICS_Ind_Counts = spark.read.csv('The-United-States-of-PPP/NAICS_Data/NAICS_Industry_Business_Counts.csv',
                   header=True,
                   sep=",",
                   inferSchema=True)
df_ind_bus_counts = df_NAICS_Ind_Counts.toPandas()
df_industry_data = df_industry_data_temp.merge(df_ind_bus_counts,on="NAICS_Industry")

#Create columns
df_industry_data['JR_LA'] = df_industry_data['Jobs_Retained'] / df_industry_data['Loan_Amount']
df_industry_data['Percent_Funded'] = df_industry_data['Loan_Count'] / df_industry_data['NAICS_Industry_Count']
df_industry_data['Avg_Loan_Size'] = df_industry_data['Loan_Amount'].mean()*1000000 / df_industry_data['Loan_Count'].mean()
df_industry_data['Industry_Avg'] = df_industry_data['Loan_Amount']*1000000 / df_industry_data['Loan_Count']
df_industry_data['Industry_vs_Avg'] = df_industry_data['Industry_Avg'] - df_industry_data['Avg_Loan_Size']
df_industry_data['P_Industry_vs_Avg'] = df_industry_data['Industry_Avg'] / df_industry_data['Avg_Loan_Size']


#BUSINESS TYPE SUBSET
df_cleaned2.createOrReplaceTempView("Loans_by_BT")
loans_by_bt = spark.sql("SELECT BusinessType, COUNT(LoanAmount) as Loan_Count, SUM(LoanAmount)/1000000 as Loan_Amount, SUM(JobsRetained)/1000 as Jobs_Retained FROM Loans_by_BT GROUP BY BusinessType ORDER BY BusinessType")
# loans_by_bt.show() - For Visualization Purposes in Spark
df_loans_by_bt = loans_by_bt.toPandas()
#Create columns
df_loans_by_bt['Percent_of_Loans'] = df_loans_by_bt['Loan_Count'] / df_loans_by_bt['Loan_Count'].sum()
df_loans_by_bt['Number of Loans'] = df_loans_by_bt['Loan_Count'].map('{:,.0f}'.format) 
df_loans_by_bt['Amount Loaned ($M)'] = df_loans_by_bt['Loan_Amount'].map('${:,.2f}'.format)
df_loans_by_bt['Jobs Retained'] = df_loans_by_bt['Jobs_Retained'].map('{:,.2f}k'.format)
df_loans_by_bt['% of Loans'] = (df_loans_by_bt['Percent_of_Loans']*100).map('{:,.2f}%'.format)
df_loans_by_bt['Business Type'] = df_loans_by_bt['BusinessType']
#Drop indexes not needed,sort, and copy
df_loan_graph = df_loans_by_bt.drop([0,2,4,7,11,15,16])
df_bt_t = df_loan_graph.sort_values('Percent_of_Loans',ascending=True)
df_bt_ts = df_bt_t.loc[:,:]

#Business types
fig = plt.figure(figsize=(12,6))
plt.barh(df_bt_ts['BusinessType'],df_bt_ts['Percent_of_Loans'],color='#008c8c')
plt.xlabel('Percentage of Loans', fontweight='bold', color = '#173158')
plt.xlim=(0,.3)
plt.xticks=(np.arange(0, .3, step=0.05))
plt.gca().xaxis.set_major_formatter(StrMethodFormatter('{x:,.2%}'))
plt.title('Percentage of Loans by Business Type', fontweight='bold', color = '#173158')
# plt.savefig('BusinessTypes.png')

df_cleaned2.createOrReplaceTempView("Loans_by_Race")
loans_by_race = spark.sql("SELECT RaceEthnicity, COUNT(LoanAmount) as Loan_Count, SUM(LoanAmount) as Loan_Amount, SUM(JobsRetained) as Jobs_Retained FROM NAICS_CB_Test GROUP BY RaceEthnicity ORDER BY RaceEthnicity")
# loans_by_race.show() - For Visualization Purposes in Spark

df_cleaned2.createOrReplaceTempView("Loans_by_Gender")
loans_by_gender = spark.sql("SELECT Gender, COUNT(LoanAmount) as Loan_Count, SUM(LoanAmount) as Loan_Amount, SUM(JobsRetained) as Jobs_Retained FROM NAICS_CB_Test GROUP BY Gender ORDER BY Gender")
# loans_by_gender.show() - For Visualization Purposes in Spark

df_cleaned2.createOrReplaceTempView("Loans_by_Veteran")
loans_by_veteran = spark.sql("SELECT Veteran, COUNT(LoanAmount) as Loan_Count, SUM(LoanAmount) as Loan_Amount, SUM(JobsRetained) as Jobs_Retained FROM NAICS_CB_Test GROUP BY Veteran ORDER BY Veteran")
# loans_by_veteran.show() - For Visualization Purposes in Spark

df_cleaned2.createOrReplaceTempView("Loans_by_NonProfit")
loans_by_nonprofit = spark.sql("SELECT NonProfit, COUNT(LoanAmount) as Loan_Count, SUM(LoanAmount)/1000000 as Loan_Amount, SUM(JobsRetained)/1000 as Jobs_Retained FROM Loans_by_NonProfit GROUP BY NonProfit ORDER BY NonProfit")
# loans_by_nonprofit.show() - For Visualization Purposes in Spark
df_np_loans = loans_by_nonprofit.toPandas()
df_np_loans['Percent_of_Loans'] = df_np_loans['Loan_Count'] / df_np_loans['Loan_Count'].sum()
df_np_loans['Percent_of_Loans_D'] = df_np_loans['Loan_Amount'] / df_np_loans['Loan_Amount'].sum()
df_np_loans





