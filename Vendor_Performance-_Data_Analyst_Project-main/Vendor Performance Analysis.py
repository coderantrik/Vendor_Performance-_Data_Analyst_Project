import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import sqlite3
from scipy.stats import ttest_ind
import scipy.stats as stats
warnings.filterwarnings('ignore')
from IPython.display import display



conn=sqlite3.connect('invertory.db')
df=pd.read_sql_query("select * from vendor_sales_summary",conn)
print(df.head())
print(df.describe().T)


df=pd.read_sql_query(""" SELECT *
FROM vendor_sales_summary
WHERE GrossProfit > 0
AND ProfitMargin > 0 
AND TotalSalesQuantity > 0""",conn)
print(df)


numerical_cols=df.select_dtypes(include=np.number).columns
plt.figure(figsize=(15,10))
for i, col in enumerate(numerical_cols):
    plt.subplot(4,4,i+1)
    sns.histplot(df[col],kde=True,bins=30)
    plt.title(col)
plt.tight_layout()
plt.show()



plt.figure(figsize=(15,10))
for i, col in enumerate(numerical_cols):
    plt.subplot(4,4,i+1)
    sns.boxplot(y=df[col])
    plt.title(col)
plt.tight_layout()
print(plt.show())



categorical_cols=["VendorName","Description"]
plt.figure(figsize=(12,5))
for i, col in enumerate(categorical_cols):
    plt.subplot(1,2,i+1)
    sns.countplot(y=df[col],order=df[col].value_counts().index[:10])
    plt.title(f"Count Plot of{col}")
plt.tight_layout()
print(plt.show())


plt.figure(figsize=(12,8))
correlation_matrix=df[numerical_cols].corr()
sns.heatmap(correlation_matrix,annot=True,fmt=".2f",cmap="coolwarm",linewidths=0.5)
plt.title("Correlation Heatmap")
print(plt.show())



brand_performance=df.groupby('Description').agg({
    'TotalSalesDollars':'sum',
    'ProfitMargin':'mean'}).reset_index()
low_sales_threshold=brand_performance['TotalSalesDollars'].quantile(0.15)
high_margin_sales_threshold=brand_performance['ProfitMargin'].quantile(0.85)
print(low_sales_threshold,high_margin_sales_threshold)

target_brands=brand_performance[
    (brand_performance['TotalSalesDollars']<=low_sales_threshold)&
    (brand_performance['ProfitMargin']>=high_margin_sales_threshold)
]
print("Brand with Low Sales but High Profit Margins:")
display(target_brands.sort_values('TotalSalesDollars'))



brand_performance=brand_performance[brand_performance['TotalSalesDollars']<10000]



plt.figure(figsize=(10,6))
sns.scatterplot(data=brand_performance,x='TotalSalesDollars',y='ProfitMargin',color="blue",label="All Brands",alpha=0.2)
sns.scatterplot(data=target_brands,x="TotalSalesDollars",y="ProfitMargin",color="red",label="Target Brands")
plt.axhline(high_margin_sales_threshold,linestyle="--",color="black",label="High Margin Threshold")
plt.axvline(low_sales_threshold,linestyle="--",color="black",label="low Margin Threshold")
plt.xlabel("Total Sale ($)")
plt.ylabel("Profit Margin (%)")
plt.title("Brands for Promotional or Pricing Adjustment")
plt.legend()
plt.grid(True)
print(plt.show())



def format_dollars(value):
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.2f}K"
    else:
        return f"{value:.2f}"

    

top_vendors=df.groupby('VendorName')["TotalSalesDollars"].sum().nlargest(10)
top_brands=df.groupby("Description")["TotalSalesDollars"].sum().nlargest(10)
print(top_vendors,top_brands)

print(top_brands.apply(lambda x:format_dollars(x)))

plt.figure(figsize=(15,5))
plt.subplot(1,2,1)
ax1=sns.barplot(y=top_vendors.index,x=top_vendors.values,palette="Blues_r")
plt.title("Top 10 Vendors by sales")
for bar in ax1.patches:
    ax1.text(bar.get_width()+(bar.get_width()*0.02),
             bar.get_y()+bar.get_height()/2,
             format_dollars(bar.get_width()),
             ha='left',va='center',fontsize=10,color='black')
plt.subplot(1,2,2)
ax2=sns.barplot(y=top_brands.index.astype(str),x=top_brands.values,palette="Reds_r")
plt.title("Top 10 Brands by sales")
for bar in ax2.patches:
    ax2.text(bar.get_width()+(bar.get_width()*0.02),
             bar.get_y()+bar.get_height()/2,
             format_dollars(bar.get_width()),
             ha='left',va='center',fontsize=10,color='black')
plt.tight_layout()
plt.show()


vendor_performance=df.groupby('VendorName').agg({
    'TotalPurchaseDollars':'sum',
    'Grossprofit':'sum',
    'TotalSalesDollars':'sum'
}).reset_index()
vendor_performance.shape


vendor_performance['PurchaseContribution%']=vendor_performance['TotalPurchaseDollars']/vendor_performance['TotalPurchaseDollars'].sum()*100
vendor_performance=round(vendor_performance.sort_values('PurchaseContribution%',ascending=False),2)


top_vendors=vendor_performance.head(10)
top_vendors['TotalSalesDollars']=top_vendors['TotalSalesDollars'].apply(format_dollars)
top_vendors['TotalPurchaseDollars']=top_vendors['TotalPurchaseDollars'].apply(format_dollars)
top_vendors['Grossprofit']=top_vendors['Grossprofit'].apply(format_dollars)
print(top_vendors)

top_vendors['PurchaseContribution%'].sum()
print(top_vendors)

top_vendors['Cumulative_Contribution%']=top_vendors['PurchaseContribution%'].cumsum()
print(top_vendors)




fig , ax1 = plt.subplots(figsize=(10,6))
sns.barplot(x=top_vendors['VendorName'],y=top_vendors['PurchaseContribution%'],palette="mako",ax=ax1)
for i,value in enumerate(top_vendors['PurchaseContribution%']):
    ax1.text(i,value-1,str(value)+'%',ha='center',fontsize=10,color="white")
ax2=ax1.twinx()
ax2.plot(top_vendors['VendorName'],top_vendors['Cumulative_Contribution%'],color="red",marker='o',linestyle='dashed',label='Cumulative_Contribution%')
ax1.set_xticklabels(top_vendors['VendorName'],rotation=90)
ax1.set_ylabel('Purchase Contribution %',color='blue')
ax2.set_ylabel('Cumulative Contribution %',color='red')
ax1.set_xlabel('Vendors')
ax1.set_title('Pareto Char: Vendor Contribution to Total Purchase')
ax2.axhline(y=100,color='gray',linestyle="dashed",alpha=0.7)
ax2.legend(loc='upper right')
plt.show()             


print(f"Total Purchase Contribution of top 10 vendors is {round(top_vendors['PurchaseContribution%'].sum(),2)}%")


vendors=list(top_vendors['VendorName'].values)
purchase_contribution=list(top_vendors['PurchaseContribution%'].values)
total_contribution=sum(purchase_contribution)
remaining_contribution=100-total_contribution
vendors.append("Other Venodr")
purchase_contribution.append(remaining_contribution)
fig,ax=plt.subplots(figsize=(8,8))
wedges,texts,autotexts=ax.pie(purchase_contribution,labels=vendors,autopct='%1.1f%%',
                              startangle=140,pctdistance=0.85,colors=plt.cm.Paired.colors)
centre_circle=plt.Circle((0,0),0.70,fc="white")
fig.gca().add_artist(centre_circle)
plt.text(0,0,f"Top 10 Total:\n{total_contribution:.2f}%",fontsize=14,fontweight="bold",ha="center",va="center")
plt.title("Top 10 Vendor's Purchase Contribution(%)" )
plt.show()


df['UnitPurchasePrice']=df['TotalPurchaseDollars']/df['TotalPurchaseQuantity']
df["OrderSize"]=pd.qcut(df["TotalPurchaseQuantity"],q=3,labels=["Small","Medium","Large"])
df[['OrderSize','TotalPurchaseQuantity']]
df.groupby('OrderSize')[['UnitPurchasePrice']].mean()

plt.figure(figsize=(10,6))
sns.boxplot(data=df,x="OrderSize",y="UnitPurchasePrice",palette="Set2")
plt.title("Impact of bulk purchasing on Unit Price")
plt.xlabel("Order Size")
plt.ylabel("Avergae Unit Purcahse Price")
plt.show()

df[df['StockTurnover']<1].groupby('VendorName')[['StockTurnover']].mean().sort_values('StockTurnover',ascending=True).head(10)

df["UnsoldInventoryValue"]=(df['TotalPurchaseQuantity']-df['TotalSalesQuantity'])*df['PurchasePrice']
print('Total Unsold Capital:',format_dollars(df['UnsoldInventoryValue'].sum()))



invertory_value_per_vendor=df.groupby("VendorName")['UnsoldInventoryValue'].sum().reset_index()
invertory_value_per_vendor=invertory_value_per_vendor.sort_values(by='UnsoldInventoryValue',ascending=  False)
invertory_value_per_vendor['UnsoldInventoryValue']=invertory_value_per_vendor['UnsoldInventoryValue'].apply(format_dollars)
print(invertory_value_per_vendor.head(10))


top_threshold=df["TotalSalesDollars"].quantile(0.75)
low_threshold=df["TotalSalesDollars"].quantile(0.25)

top_vendors=df[df["TotalSalesDollars"]>=top_threshold]["ProfitMargin"].dropna()
low_vendors=df[df["TotalSalesDollars"]<=low_threshold]["ProfitMargin"].dropna()
print(top_vendors,low_vendors)


def confidence_interval(data,confidence=0.95):
    mean_val=np.mean(data)
    std_err=np.std(data,ddof=1)/np.sqrt(len(data))
    t_critical=stats.t.ppf((1+confidence)/2,df=len(data)-1)
    margin_of_error=t_critical*std_err
    return mean_val,mean_val-margin_of_error,mean_val+margin_of_error



top_mean,top_lower,top_upper=confidence_interval(top_vendors)
low_mean,low_lower,low_upper=confidence_interval(low_vendors)

print(f"Top Vendors 95% CI: ({top_lower:.2f}),{top_upper:.2f}),Mean:{top_mean:.2f}")
print(f"Low Vendors 95% CI: ({low_lower:.2f}),{low_upper:.2f}),Mean:{low_mean:.2f}")
plt.figure(figsize=(12,6))

sns.histplot(top_vendors,kde=True,color="blue",bins=30,alpha=0.5,label="Top Vendors")
plt.axvline(top_lower,color="blue",linestyle="--",label=f"Top Lower:{top_lower : .2f}")
plt.axvline(top_upper,color="blue",linestyle="--",label=f"Top Upper:{top_upper : .2f}")
plt.axvline(top_mean,color="blue",linestyle="--",label=f"Top Mean:{top_mean : .2f}")

sns.histplot(low_vendors,kde=True,color="red",bins=30,alpha=0.5,label="Low Vendors")
plt.axvline(low_lower,color="red",linestyle="--",label=f"Low Lower:{low_lower : .2f}")
plt.axvline(low_upper,color="red",linestyle="--",label=f"Low Upper:{low_upper : .2f}")
plt.axvline(low_mean,color="red",linestyle="--",label=f"Low Mean:{low_mean : .2f}")

plt.title("Confidence Interval Comparison: Top vs. Low Vendors (Profit Margin)")
plt.xlabel("Profit Margin (%)")
plt.ylabel("Frequency")
plt.legend()
plt.grid(True)
plt.show()



top_threshold=df["TotalSalesDollars"].quantile(0.75)
low_threshold=df["TotalSalesDollars"].quantile(0.25)
top_vendors=df[df["TotalSalesDollars"] >= top_threshold]['ProfitMargin'].dropna()
low_vendors=df[df["TotalSalesDollars"] >= low_threshold]['ProfitMargin'].dropna()
t_stat,p_value=ttest_ind(top_vendors,low_vendors,equal_var=False)
print(f"T-Statistics:{t_stat:.4f},P-Value:{p_value:.4f}")
if p_value<0.05:
    print("Reject H0 : There is a significant differnece in profit mrgins between top and low-performing vendors. ")
else:
    print("Fail to Reject HO : No significant difference in profit margins.")
