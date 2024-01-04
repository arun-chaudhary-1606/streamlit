import streamlit as st
import pandas as pd
import snowflake.connector as sf

#This App is a simple calculator for analysing FT Platform connector
#And to analyse the incremental data by connecting to Snowlflake

def connect_to_snowflake(account,username,password,role,wh,db,schema):
    try:
        connection_string=sf.connect(user=username,account=account,password=password,role=role,warehouse=wh,database=db,schema=schema)
    except Exception:
        st.error('Please verify credentials', icon="🚨")
        return None
    try:
        conn=connection_string.cursor()
        st.session_state['snow_conn']= conn
        st.session_state['is_ready']=True
        st.session_state['DATABASE']=db
        st.session_state['SCHEMA']=schema
        return conn
    
    except Exception:
        st.error('Some error occured. Please refresh page', icon="🚨")
        return None

#Cached the function as per https://docs.streamlit.io/library/advanced-features/caching#basic-usage
#So that frequency of querying is reduced.
@st.cache_data
def get_data(range):

    query='''WITH parse_json AS (
  SELECT
    CONNECTOR_ID,
    SUBSTRING(DATE_TRUNC('DAY', time_stamp)::varchar, 0, 10) AS "DATE",
    time_stamp as TIME_STAMP,
    PARSE_JSON(message_data) AS message_data
  FROM
    {}.log
  WHERE
    DATEDIFF(DAY, time_stamp, CURRENT_DATE) <= {}
    AND message_event = 'records_modified'
)

SELECT 
    C.CONNECTOR_ID,
    C.CONNECTOR_TYPE_ID,
    CT.OFFICIAL_CONNECTOR_NAME,
    C.CONNECTOR_NAME,
    PJ."DATE",
    PJ.TIME_STAMP,
    PJ.message_data:schema AS "SCHEMA",
    PJ.message_data:table AS "TABLE_NAME",
    SUM(PJ.message_data:count::integer) AS ROW_VOLUME
FROM 
    {}.{}.CONNECTOR C
JOIN 
    {}.{}.CONNECTOR_TYPE CT
ON 
    C.CONNECTOR_TYPE_ID = CT.ID
JOIN 
    parse_json PJ
ON 
    C.CONNECTOR_ID = PJ.CONNECTOR_ID
WHERE 
    PJ.message_data:schema != '{}'
GROUP BY 
    C.CONNECTOR_ID,
    C.CONNECTOR_TYPE_ID,
    CT.OFFICIAL_CONNECTOR_NAME,
    C.CONNECTOR_NAME,
    PJ.TIME_STAMP,
    PJ."DATE",
    PJ.message_data:schema,
    PJ.message_data:table
ORDER BY 
    PJ."DATE" DESC;
'''.format(st.session_state['SCHEMA'],range,st.session_state['DATABASE'],st.session_state['SCHEMA'],st.session_state['DATABASE'],st.session_state['SCHEMA'],st.session_state['SCHEMA'])
    
    #'query=''select * from mock_data where date>=dateadd(day, -{}, current_date());'''.format(range)
    results=st.session_state['snow_conn'].execute(query)
    results=st.session_state['snow_conn'].fetch_pandas_all()
    return pd.DataFrame(results)



#Cached the function as per https://docs.streamlit.io/library/advanced-features/caching#basic-usage
#So that frequency of querying is reduced.
@st.cache_data
def get_latency(connector_id):
    #Parses the sync stat metrics which holds the sync metadata of each job
    newquery='''select sync_id,connector_id, substr(time_stamp::varchar, 0,16) as time_stamp, parse_json(message_data):total_time_s as LATENCY_SECONDS
from LOG where message_event='sync_stats' and connector_id='{}' '''.format(str.lower(connector_id))
    newquery_results=st.session_state['snow_conn'].execute(newquery)
    newquery_results=st.session_state['snow_conn'].fetch_pandas_all()
    return pd.DataFrame(newquery_results)



st.set_page_config(
    page_title="Snowflake - Check daily modified records",
    page_icon=":snowflake:",
    layout="wide",
    initial_sidebar_state='expanded',
    menu_items={
        'Get Help': 'https://fivetran.com/docs/logs/fivetran-platform/sample-queries#snowflakedailyrecords',
        'Report a bug': "https://www.linkedin.com/in/melwinvinod/",
        'About': "# The sample queries used in this app return the volume of data that has been inserted, updated, or deleted each day into the snowflake destination. Query results are at the table level."
    }
)

#Sidebar UI
sidebar = st.sidebar
with sidebar:
    account=st.text_input("Snowflake Account",placeholder="<orgname>-<account_name>")
    username=st.text_input("User Name")
    password=st.text_input("Password",type="password")
    db=st.text_input("Database")
    role=st.text_input("Role", placeholder="Optional")
    wh=st.text_input("Warehouse",placeholder="Optional")
    schema=st.text_input("Schema", placeholder="FIVETRAN_LOG")
    connect=st.button("Connect To Snowflake", on_click=connect_to_snowflake, args=[account,username,password,role,wh,db,schema])
    

#Display toast message based on connection status
if 'is_ready' not in st.session_state:
    st.session_state['is_ready']=False
    st.header(":green[Disconnected]")

elif 'is_ready' in st.session_state and st.session_state['is_ready']==True:
    #st.toast('Connected to snowflake!', icon='😍')
    st.header(":green[Connected]")




#Main Function
if st.session_state['is_ready']==True:
    range = st.slider('calculate incremental volume for the last n days'.capitalize(), 30, 180,step=30)
    #Calls the function which returns the results in a Pandas Dataframe
    df=get_data(range)
    df = df.map(lambda x: x.strip('"') if isinstance(x, str) else x)
    
    #Following coderemoves the 0th Unnamed index
    #df.style.hide(axis="index")

    df['DATE'] = pd.to_datetime(df['DATE'])
    df.style.hide(axis="index")
    #Derive Month Name from date
    df['MONTH'] = df['DATE'].dt.month_name()
    #Sum up the incremental data based on Month name
    result_df = df.groupby('MONTH')['ROW_VOLUME'].sum().reset_index()
    st.dataframe(result_df,hide_index=True,use_container_width=True)
    result_df['ROW_VOLUME'] = pd.to_numeric(result_df['ROW_VOLUME'], errors='coerce')
    st.bar_chart(result_df,x='MONTH',y='ROW_VOLUME', color='ROW_VOLUME')
    
    
    #Print the source names:
    newdf=df.drop_duplicates(subset=['CONNECTOR_ID'])
    st.write('List of connectors:')
    st.dataframe(newdf[['CONNECTOR_ID','CONNECTOR_TYPE_ID','OFFICIAL_CONNECTOR_NAME','CONNECTOR_NAME']],hide_index=True,use_container_width=True)

    

    with st.expander("Connector Level Incremental Volume",expanded=False):
        #FILTER ON CONNECTOR_ID. Use Set to filter the discinct values
        connector_id = st.selectbox(':green[Connector]',set(df['CONNECTOR_ID']),placeholder="ConnectorID")
        connector_df=df[df['CONNECTOR_ID']==connector_id]
        

        #Check if the selected options are present in the Connector_Id, get their row_volume and sum it up
        #Make connector color green
        st.metric(label="Incremental Volume for :red[{}]".format(connector_id), value=int(df[df['CONNECTOR_ID']==(connector_id)]['ROW_VOLUME'].reset_index(drop=True).sum()))
        
        #selected_df=df[df['CONNECTOR_ID'].isin(options)].reset_index(drop=True)
        #st.dataframe(selected_df,use_container_width=True)
        
        #newdf = df.groupby('DATE')['ROW_VOLUME'].sum().reset_index()
        #newdf['DATE'] = newdf['DATE'].dt.strftime('%d-%m-%y')
        
        
        #Show schema level volume graph
        schema_graph = connector_df.groupby(['MONTH','SCHEMA'])['ROW_VOLUME'].sum().reset_index()
        st.dataframe(schema_graph,hide_index=True,use_container_width=True)
        schema_graph['ROW_VOLUME'] = pd.to_numeric(schema_graph['ROW_VOLUME'], errors='coerce')
        #st.bar_chart(schema_graph,x='MONTH',y='SCHEMA'.upper(),color='ROW_VOLUME',use_container_width=True)

        schema_totals = connector_df.groupby('SCHEMA')['ROW_VOLUME'].sum()

        #option to choose schema based on connector_id
        schema = st.selectbox(':green[Schema]',set(connector_df['SCHEMA']),placeholder="Select Schema")
        
        #Show table level volume graph on selected schema
        table_graph = connector_df[connector_df['SCHEMA']==schema].groupby(['MONTH','SCHEMA','TABLE_NAME'])['ROW_VOLUME'].sum().reset_index()
        st.dataframe(table_graph,hide_index=True,use_container_width=True)
        table_graph['ROW_VOLUME'] = pd.to_numeric(table_graph['ROW_VOLUME'], errors='coerce')
        #st.bar_chart(table_graph,x='MONTH',y='TABLE_NAME'.upper(),color='ROW_VOLUME',use_container_width=True)

        

        
        #option to choose Table based on connector_id & schema
        table = st.selectbox(':green[Table]',df[(df['CONNECTOR_ID']==connector_id) & (df['SCHEMA']==schema)]['TABLE_NAME'].unique(),placeholder="Select Table")
        
        
        #Show table volume graph on selected table
        single_table = connector_df[connector_df['TABLE_NAME']==table].groupby(['MONTH'])['ROW_VOLUME'].sum().reset_index()
        st.dataframe(single_table,hide_index=True,use_container_width=True)
        single_table['ROW_VOLUME'] = pd.to_numeric(single_table['ROW_VOLUME'], errors='coerce')
        st.bar_chart(single_table,x='MONTH',y='ROW_VOLUME'.upper(),color='ROW_VOLUME',use_container_width=True)

        #Show Pipeline level latencies
        #Check SQL query above
        latency_metrics_df=get_latency(connector_id)
        #st.dataframe(latency_metrics_df)
        latency_metrics_df['LATENCY_SECONDS'] = pd.to_numeric(latency_metrics_df['LATENCY_SECONDS'], errors='coerce')
        #st.dataframe(latency_metrics_df)

        #p99, 95, 75, 50 percentile latency
        sorted_df = latency_metrics_df.sort_values(by='LATENCY_SECONDS')
        p99_value = sorted_df['LATENCY_SECONDS'].quantile(0.99).__round__(2)
        p95_value = sorted_df['LATENCY_SECONDS'].quantile(0.95).__round__(2)
        p75_value = sorted_df['LATENCY_SECONDS'].quantile(0.75).__round__(2)
        p50_value = sorted_df['LATENCY_SECONDS'].quantile(0.50).__round__(2)
        
        # To display all of the above percentile 4 metrics in a single row
        st.markdown(":red[Pipeline Latency Metrics(seconds)]")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric(label=":green[p99 latency]", value=p99_value)
        col2.metric(label=":green[p95 latency]", value=p95_value)
        col3.metric(label=":green[p75 latency]", value=p75_value)
        col4.metric(label=":green[p50 latency]", value=p50_value)
        st.line_chart(latency_metrics_df,x='TIME_STAMP',y='LATENCY_SECONDS',use_container_width=True)
        


        
        
    
    
    