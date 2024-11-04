import pandas as pd
import requests
import asyncio
import aioodbc
from concurrent.futures import ThreadPoolExecutor
import pyodbc
from dotenv import dotenv_values
import datetime
config = dotenv_values(".env")

#cnxn = pyodbc.connect(
#        f"""
#        DSN=mariadbignef;
#        database=ignition;
#        UID=evxadmin;
#        PWD={config['password']}""")
#cursor = cnxn.cursor()



ocdExcelFile = "https://wwwapps.emnrd.nm.gov/OCD/OCDPermitting/Data/WellSearchExpandedResultsExcel.aspx?OperatorSearchClause=BeginsWith&WellSearchClause=BeginsWith&WellNumberSearchClause=BeginsWith&PoolSearchClause=BeginsWith&section=00&Status=A&CancelledAPDs=Exclude&PluggedWells=Exclude&SearchLocation=Surface"
ocdWellBaseUrl = "https://wwwapps.emnrd.nm.gov/OCD/OCDPermitting/Data/WellDetails.aspx?api="
localFileName = "ocdWells.html"

ocdColNames = ['API', 'Well Name', 'Well Number', 'Type', 'Mineral Owner', 'Surface Owner', 'Status',
               'Initial APD Approval Date', 'Unit Letter', 'Section', 'Township', 'Range', 'OCD Unit Letter',
               'Footages', 'Latitude', 'Longitude', 'Projection', 'Last Production', 'Spud Date', 'Measured Depth',
               'True Vertical Depth', 'Elevation', 'Kelly Bushing', 'Drilling Floor', 'Last Inspection', 'Last MIT',
               'Plugged On', 'Current Operator', 'District']

sqlColNames = ['API_Nbr', 'Lease_Name', 'Well_Name_Nbr', 'Well_Type', 'Operator_Name', 'Record_Type',
               'Reg_District_Nbr', 'Lease_Nbr', 'Drill_Permit_Nbr', 'Operator_Nbr', 'State_Code', 'County_Code',
               'County_Name', 'Field_Nbr', 'Field_Name', 'Last_Update_By', 'Last_Update_Date']

ocdCountyCodes = {'001': 'BERNALILLO', '003': 'CATRON', '005': 'CHAVES', '006': 'CIBOLA', '007': 'COLFAX',
                  '009': 'CURRY', '011': 'DE BACA', '013': 'DONA ANA', '015': 'EDDY', '017': 'GRANT',
                  '019': 'GUADALUPE', '021': 'HARDING', '023': 'HIDALGO', '025': 'LEA', '027': 'LINCOLN',
                  '028': 'LOS ALAMOS', '029': 'LUNA', '031': 'MCKINLEY', '033': 'MORA', '035': 'OTERO',
                  '037': 'QUAY', '039': 'RIO ARRIBA', '041': 'ROOSEVELT', '043': 'SANDOVAL', '045': 'SAN JUAN',
                  '047': 'SAN MIGUEL', '049': 'SANTA FE', '051': 'SIERRA', '053': 'SOCORRO', '055': 'TAOS',
                  '057': 'TORRANCE', '059': 'UNION', '061': 'VALENCIA'}


def fetchOcdData():
    url = ocdExcelFile
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(localFileName, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    htmlList = pd.read_html(localFileName)
    ocdWells = htmlList[0]
    ocdWells = ocdWells[ocdWells['District'].isin(['Hobbs', 'Artesia'])]
    ocdWells = ocdWells[ocdWells['Status'] == 'Active']
    ocdWells.drop(['Mineral Owner', 'Surface Owner', 'Status',
                   'Initial APD Approval Date', 'Unit Letter', 'Section', 'Township', 'Range', 'OCD Unit Letter',
                   'Footages', 'Latitude', 'Longitude', 'Projection', 'Last Production', 'Spud Date', 'Measured Depth',
                   'True Vertical Depth', 'Elevation', 'Kelly Bushing', 'Drilling Floor', 'Last Inspection', 'Last MIT',
                   'Plugged On', 'District'], axis=1, inplace=True)
    return ocdWells


def formatOcdData(dataframe):
    dataframe['Record_Type'] = 'OCD'
    dataframe['Reg_District_Nbr'] = 'N/A'
    dataframe['Lease_Nbr'] = 'N/A'
    dataframe['Drill_Permit_Nbr'] = 'N/A'
    dataframe['Operator_Nbr'] = dataframe['Current Operator'].str.split(']', expand=True)[0].str.strip('[')
    dataframe['Operator_Name'] = dataframe['Current Operator'].str.split(']', expand=True)[1]
    dataframe['State_Code'] = dataframe['API'].str.split('-', expand=True)[0]
    dataframe['County_Code'] = dataframe['API'].str.split('-', expand=True)[1]
    dataframe['County_Name'] = dataframe['County_Code'].map(ocdCountyCodes)
    dataframe['Field_Nbr'] = 'N/A'
    dataframe['Field_Name'] = 'N/A'
    dataframe['Last_Update_By'] = 'OCD_Script'
    dataframe['Last_Update_Date'] = pd.to_datetime('today')
    dataframe['Type'] = dataframe['Type'].str[:1]
    #dataframe = dataframe[dataframe['Type'].eq('O') | dataframe['County_Code'].eq('G')]
    dataframe.drop('Current Operator', axis=1, inplace=True)
    dataframe = dataframe[['API', 'Well Name', 'Well Number', 'Type', 'Operator_Name', 'Record_Type',
                           'Reg_District_Nbr', 'Lease_Nbr', 'Drill_Permit_Nbr', 'Operator_Nbr', 'State_Code',
                           'County_Code',
                           'County_Name', 'Field_Nbr', 'Field_Name', 'Last_Update_By', 'Last_Update_Date']]
    for col in dataframe.columns:
        if dataframe[col].dtype == 'object':
            dataframe[col] = dataframe[col].str.strip()
    dataframe.columns = sqlColNames
    print(len(dataframe))
    return dataframe


async def updateSQL(conn, row):
    async with conn.cursor() as cur:
        qry = f"""IF EXISTS(select * from [OCD_Wells2] where API_Nbr = ?)
               update [OCD_Wells2] set Lease_Name = ?,Well_Name_Nbr = ?,Well_Type = ?,Operator_Name = ?,
               Record_Type = ?, Reg_District_Nbr = ?, Lease_Nbr = ?, Drill_Permit_Nbr = ?, Operator_Nbr = ?, 
               State_Code = ?, County_Code = ?, County_Name = ?, Field_Nbr = ?, Field_Name = ?, Last_Update_By = ?,
               Last_Update_Date = ? where API_Nbr = ?
            ELSE
                insert into [OCD_Wells2]([Record_Type],[Well_Type] ,[Reg_District_Nbr],[Lease_Name] ,[Well_Name_Nbr],
                [Lease_Nbr] ,[API_Nbr] ,[Drill_Permit_Nbr],[Operator_Nbr],[Operator_Name] ,[State_Code],[County_Code],
                [County_Name],[Field_Nbr],[Field_Name],[Last_Update_By],[Last_Update_Date]) 
                values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
        await cur.execute(qry, row['API_Nbr'], row['Lease_Name'], row['Well_Name_Nbr'], row['Well_Type'],
                                   row['Operator_Name'], row['Record_Type'], row['Reg_District_Nbr'], row['Lease_Nbr'],
                                   row['Drill_Permit_Nbr'], row['Operator_Nbr'], row['State_Code'], row['County_Code'],
                                   row['County_Name'], row['Field_Nbr'], row['Field_Name'], row['Last_Update_By'],
                                   row['Last_Update_Date'], row['API_Nbr'], row['Record_Type'], row['Well_Type'],
                                   row['Reg_District_Nbr'], row['Lease_Name'], row['Well_Name_Nbr'], row['Lease_Nbr'],
                                   row['API_Nbr'], row['Drill_Permit_Nbr'], row['Operator_Nbr'], row['Operator_Name'],
                                   row['State_Code'], row['County_Code'], row['County_Name'], row['Field_Nbr'],
                                   row['Field_Name'], row['Last_Update_By'], row['Last_Update_Date'])

async def db_main(loop, dataframe):
    dsn = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=wbs-sql.database.windows.net;DATABASE={config['database']};UID={config['user']};PWD={config['password']}"
    async with aioodbc.create_pool(dsn=dsn, loop=loop, executor=ThreadPoolExecutor(), autocommit=True) as pool:
        tasks = [do_insert(pool, row) for index, row in dataframe.iterrows()]
        await asyncio.gather(*tasks)

async def do_insert(pool, dataframe):
    async with pool.acquire() as conn:
        await updateSQL(conn, dataframe)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(datetime.datetime.now())
    ocdWells = fetchOcdData()
    ocdWells = formatOcdData(ocdWells)
    #print(ocdWells.head())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(db_main(loop, ocdWells))
    print(datetime.datetime.now())
    #ocdWells.to_csv('ocdWells.csv', index=False)

