from sre_parse import State
import streamlit as st
#import folium
import pandas as pd
#from streamlit_folium import folium_static
import requests
import json
import plotly.express as px
import mysql.connector
import PIL
from PIL import Image
from streamlit_option_menu import option_menu


# Streamlit Dashboard
icon=Image.open(r'C:\Users\josea\Downloads\phonepe_image.png')
st.set_page_config(page_title= "Phonepe Pulse Visualization",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This dashboard app is created by *Jose*!"""})

st.sidebar.header(":wave: :red[**Hello! Welcome to the Phonepe Dashboard!**]")


# Creating option Menu on the sidebar
with st.sidebar:
    selected = option_menu("Menu", ["Home","Explore Data","Insights"], 
                icons=["house","graph-up-arrow","bar-chart-line", "exclamation-circle"],
                menu_icon= "menu-button-wide",
                default_index=0,
                styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "#ff0582"},
                        "nav-link-selected": {"background-color": "#ff0582"}})


#----------------------------------------- Home ----------------------------------------------------  
if selected == "Home":
    st.markdown("# :green[Phonepe Data Visualization and Exploration]")
    st.markdown("## :red[A User-Friendly Tool Using Streamlit and Plotly]")
    col1,col2 = st.columns([3,2],gap="medium")
    with col1:
        st.write(" ")
        st.write(" ")
        st.markdown("### :green[Technologies used :] We have used following technologies in our project Github Cloning, Python, Pandas, MySQL, mysql-connector-python, Streamlit, and Plotly.")
        st.markdown("### :green[Overview :] In this streamlit web app you can visualize the phonepe pulse data and gain insights on transactions, number of users, top 10 state, district, pincode and which brand has most number of users and so on. Bar charts, Pie charts and Geo map visualization are used to get some insights.")

    with col2:
        st.write(" ")
        st.write(" ")
        st.write(" ")
        st.video(r"D:\Guvi\Project 2 Phonepe\pulse-video.mp4")
        st.write(" ")
        st.write(" ")
        st.video(r"C:\Users\josea\Downloads\upi.mp4")


#--------------------------------------- Explore Data --------------------------------------------------
if selected == "Explore Data":
    #SQL connection
    mydb=mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        port='3306',
        database='phonepe_pulse'
    )
    cursor=mydb.cursor()
    cursor.execute('use phonepe_pulse')

    #Aggregate_transsaction
    cursor.execute("select * from aggregate_transaction;")
    Aggre_trans = pd.DataFrame(cursor.fetchall(),columns = ("State", "Year", "Quarter", "Transaction_Type", "Transaction_Count", "Transaction_Amount"))

    #Aggreagted_user
    cursor.execute("select * from aggregate_user")
    Aggre_user = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year', 'Quarter', 'Brands', 'User_Count', 'User_percentage'])

    #Map transaction
    cursor.execute("select * from map_transaction;")
    Map_trans = pd.DataFrame(cursor.fetchall(), columns = ("State", "Year", "Quarter", "Districts", "Transaction_Count", "Transaction_Amount"))

    #Map_user
    cursor.execute("select * from map_user;")
    Map_user = pd.DataFrame(cursor.fetchall(), columns = ("State", "Year", "Quarter", "Districts", "Registered_Users"))

    #Top_transaction
    cursor.execute("select * from top_transaction;")
    Top_trans = pd.DataFrame(cursor.fetchall(), columns = ("State", "Year", "Quarter", "District_Name",  "District_Pincode", "Transaction_Count", "Transaction_Amount"))

    #Top_user
    cursor.execute("select * from top_user;")
    Top_user = pd.DataFrame(cursor.fetchall(), columns = ("State", "Year", "Quarter", "District_Name", "District_Pincode", "Registered_User"))


    # Choropleth Mapping India
    choropleth = st.radio(
    ":blue[CHOROPLETH MAPPING INDIA]",
    [":rainbow[State wise Transaction Amount]", ":rainbow[State wise Transaction Count]"])


    # State wise Transaction Amount
    if choropleth == ":rainbow[State wise Transaction Amount]":
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        state_names_tra = [feature["properties"]["ST_NM"] for feature in data1["features"]]
        state_names_tra.sort()
        df_state_names_tra = pd.DataFrame({"State":state_names_tra})
        frames = []
        for year in Map_user["Year"].unique():
            for quarter in Aggre_trans["Quarter"].unique():
                at1 = Aggre_trans[(Aggre_trans["Year"]==year)&(Aggre_trans["Quarter"]==quarter)]
                atf1 = at1[["State","Transaction_Amount"]]
                atf1 = atf1.sort_values(by="State")
                atf1["Year"]=year
                atf1["Quarter"]=quarter
                frames.append(atf1)
        merged_df = pd.concat(frames)
        fig_tra = px.choropleth(merged_df, geojson= data1, locations= "State", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "State", title = "TRANSACTION AMOUNT",
                                animation_frame="Year", animation_group="Quarter")
        fig_tra.update_geos(fitbounds= "locations", visible =False)
        fig_tra.update_layout(width =600, height= 700)
        fig_tra.update_layout(title_font= {"size":25})
        st.plotly_chart(fig_tra)
    
        # Bar Chart
        attype= Aggre_trans[["Transaction_Type","Transaction_Amount"]]
        att1= attype.groupby("Transaction_Type")["Transaction_Amount"].sum()
        df_att1= pd.DataFrame(att1).reset_index()
        fig_tra_pa= px.bar(df_att1, x= "Transaction_Type", y= "Transaction_Amount", title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                        color_discrete_sequence= px.colors.sequential.Blues_r)
        fig_tra_pa.update_layout(width= 600, height= 500)
        st.plotly_chart(fig_tra_pa)


    # State wise Transation Count
    else:
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response= requests.get(url)
        data2= json.loads(response.content)
        state_names_tra= [feature["properties"]["ST_NM"]for feature in data2["features"]]
        state_names_tra.sort()
        df_state_names_tra= pd.DataFrame({"States":state_names_tra})
        frames= []
        for year in Aggre_trans["Year"].unique():
            for quarter in Aggre_trans["Quarter"].unique():
                at1= Aggre_trans[(Aggre_trans["Year"]==year)&(Aggre_trans["Quarter"]==quarter)]
                atf1= at1[["State", "Transaction_Count"]]
                atf1=atf1.sort_values(by="State")
                atf1["Year"]=year
                atf1["Quarter"]=quarter
                frames.append(atf1)
        merged_df = pd.concat(frames)
        fig_tra= px.choropleth(merged_df, geojson= data2, locations= "State",featureidkey= "properties.ST_NM",
                            color= "Transaction_Count", color_continuous_scale="Sunsetdark", range_color= (0,3000000),
                            title="TRANSACTION COUNT", hover_name= "State", animation_frame= "Year", animation_group= "Quarter")
        fig_tra.update_geos(fitbounds= "locations", visible= False)
        fig_tra.update_layout(width= 600, height= 700)
        fig_tra.update_layout(title_font={"size":25})
        st.plotly_chart(fig_tra)

        # Bar chart
        attype= Aggre_trans[["Transaction_Type", "Transaction_Count"]]
        att1= attype.groupby("Transaction_Type")["Transaction_Count"].sum()
        df_att1= pd.DataFrame(att1).reset_index()
        fig_pc= px.bar(df_att1,x= "Transaction_Type",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                    color_discrete_sequence=px.colors.sequential.Redor_r)
        fig_pc.update_layout(width=600, height= 500)
        st.plotly_chart(fig_pc)


#--------------------------------- Insights -----------------------------------------------
if selected == "Insights":
    # SQL connection
    mydb=mysql.connector.connect(
        host='localhost',
        user='root',
        password="",
        port='3306',
        database='phonepe_pulse'
    )
    cursor=mydb.cursor()
    mydb.commit()
    cursor.execute('use phonepe_pulse')
    
    st.markdown("## :violet[Insights]")
    st.write("---")
    st.subheader(":blue[Let's learn some basic information about the data:]")
    options = ["--select",
               "1. Top 10 states based on year and amount of transaction",
               "2. Bottom 10 states based on year and amount of transaction",
               "3. Most used transaction type based on state and amount of transaction",
               "4. Top 10 Registered users based on state",
               "5. Bottom 10 Registered users based on state",
               "6. Top 10 Districts based on states and Count of transaction",
               "7. Bottom 10 Districts based on states and Count of transaction",
               "8. Top 10 mobile brands with highest user count and their user percentage"]
    
    select=st.selectbox("Select the options",options)
    if select=="1. Top 10 states based on year and amount of transaction":
        cursor.execute("SELECT DISTINCT State, Year, SUM(Transaction_Amount) AS Total_Transaction_Amount FROM top_transaction GROUP BY State,year ORDER BY Total_Transaction_Amount DESC LIMIT 10")
        df1 = pd.DataFrame(cursor.fetchall(), columns=['States','Transaction_Year', 'Transaction_Amount'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df1)
        with col2:
            st.title("Top 10 states and amount of transaction")
            st.bar_chart(data=df1,x="Transaction_Amount",y="States")

    elif select=="2. Bottom 10 states based on year and amount of transaction":
        cursor.execute("SELECT DISTINCT State, Year, SUM(Transaction_Amount) AS Total_Transaction_Amount FROM top_transaction GROUP BY State,year ORDER BY Total_Transaction_Amount LIMIT 10")
        df2 = pd.DataFrame(cursor.fetchall(), columns=['States','Transaction_Year', 'Transaction_Amount'])
        col1,col2 = st.columns([0.4,0.4])
        with col1:
            st.write(df2)
        with col2:
            st.title("Bottom 10 states and amount of transaction")
            st.bar_chart(data=df2,x="Transaction_Amount",y="States")

    elif select=="3. Most used transaction type based on state and amount of transaction":
        cursor.execute("SELECT DISTINCT transaction_type, sum(transaction_amount) as amount from aggregate_transaction GROUP BY transaction_type ORDER BY amount DESC")
        df3=pd.DataFrame(cursor.fetchall(), columns=['Transaction_Type', 'Amount'])
        col1,col2=st.columns(2)
        with col1:
            st.write(df3)
        with col2:
            st.title("Most used Transaction Type Based on Amount")
            st.bar_chart(data=df3,x="Transaction_Type", y="Amount")

    elif select=="4. Top 10 Registered users based on state":
        cursor.execute("SELECT State,Registered_User from top_user GROUP BY State order by Registered_User desc limit 10")
        df4=pd.DataFrame(cursor.fetchall(), columns=['State','Registered_Users'])
        col1,col2=st.columns(2)
        with col1:
            st.write(df4)
        with col2:
            st.title("Top 10 Registered Users based on State")
            st.bar_chart(data=df4, x='State', y='Registered_Users')

    elif select=="5. Bottom 10 Registered users based on state":
        cursor.execute("SELECT State,Registered_User from top_user GROUP BY State order by Registered_User limit 10")
        df5=pd.DataFrame(cursor.fetchall(), columns=['State','Registered_Users'])
        col1,col2=st.columns(2)
        with col1:
            st.write(df5)
        with col2:
            st.title("Bottom 10 Registered Users based on State")
            st.bar_chart(data=df5, x='State', y='Registered_Users')

    elif select=="6. Top 10 Districts based on states and Count of transaction":
        cursor.execute("SELECT DISTINCT District, State, sum(Transaction_Count) as Tra_Counts FROM map_transaction group by District, state order by Tra_Counts desc limit 10")
        df6=pd.DataFrame(cursor.fetchall(), columns=['District','State', 'Tra_Counts'])
        col1,col2=st.columns([0.4,0.3])
        with col1:
            st.write(df6)
        with col2:
            st.title("Top 10 District based on State and Transaction_Count")
            st.bar_chart(data=df6, x='District', y='Tra_Counts') 

    elif select=="7. Bottom 10 Districts based on states and Count of transaction":
        cursor.execute("SELECT DISTINCT District, State, sum(Transaction_Count) as Tra_Counts FROM map_transaction group by District, state order by Tra_Counts limit 10")
        df7=pd.DataFrame(cursor.fetchall(), columns=['District','State', 'Tra_Counts'])
        col1,col2=st.columns([0.4,0.3])
        with col1:
            st.write(df7)
        with col2:
            st.title("Bottom 10 District based on State and Transaction_Count")
            st.bar_chart(data=df7, x='District', y='Tra_Counts') 

    elif select=="8. Top 10 mobile brands with highest user count and their user percentage":
        cursor.execute("SELECT brands, User_Count, User_Percentage FROM aggregate_user GROUP BY brands order by User_Count desc limit 10")
        df8=pd.DataFrame(cursor.fetchall(), columns=['Brands','User_Count', 'User_Percentage'])
        col1,col2=st.columns([0.4,0.3])
        with col1:
            st.write(df8)
        with col2:
            st.title("Top 10 mobile brands with highest user_count")
            st.bar_chart(data=df8, x='Brands', y='User_Count') 
      

