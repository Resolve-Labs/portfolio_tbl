import pandas as pd
import boto3
import os
import io
import argparse
def create_argument_parser():
    """
    Function to add command line arguments at run time
    """
    parser  = argparse.ArgumentParser(description = 'Script to test out pipeline')
    parser.add_argument('--run-type', nargs = '?', required = True, help = 'Command to run task or test')
    return parser


def aws_access():
    #amazon access keys from local env
    AWS_ACCESS_KEY_ID = os.getenv("aws_access")[1:-1]
    AWS_SECRET_ACCESS_KEY = os.getenv("aws_key")[1:-1]
    s3 = boto3.resource(service_name='s3',region_name='ca-central-1', aws_access_key_id=str(AWS_ACCESS_KEY_ID), aws_secret_access_key=str(AWS_SECRET_ACCESS_KEY))
    return s3


def getIndexes(dfObj, value):
    listOfPos = []
    result = dfObj.isin([value])
    seriesObj = result.any()
    columnNames = list(seriesObj[seriesObj == True].index)
    for col in columnNames:
        rows = list(result[col][result[col] == True].index)
        for row in rows:
            listOfPos.append((row, col))

    return listOfPos

def get_department_rows(df):
    dept_loc = {}
    for i,dept in enumerate(df['Portfolio'].to_list()):

        #get the row number associated with the start a department roles
        sessions = list(getIndexes(df, dept))
        row = sessions[0][0]
        # add dictionary item of row start for each session
        dept_loc[dept] = row
    return dept_loc

def get_parliament_rows(df,data):
    locations = {}
    for i,parl in enumerate(df["Ids"].to_list()):

        try:
            #get the row number associated with the start a parliament
            sessions = list(getIndexes(data, parl))
            row = sessions[0][0]
            # add dictionary item of row start for each session
            locations[elections.loc[i,"Parliament"]] = row
        except:
            locations[elections.loc[i,"Parliament"]] = 1

    return locations

def add_departments(dept_loc,d,end_row):
    #create dataframe of portfolios assocoiated with Departments
    depts = list(dept_loc.keys())

    #create blank dataframe to append data to
    portfolios = pd.DataFrame([], columns=['Portfolio','Dept','Role','Title','Start','End'])
    end_row1 = end_row
    for i,dept in enumerate(depts):
        #set start row location  with dept as the string name of the deptaremnt we using to index the dictionary
        row = dept_loc[dept]
        try:
            end_row = dept_loc[depts[i+1]]
        except:
            #end row is end of dataset
            end_row = end_row1


        #filter to just rows between department rows
        d = df.iloc[row+1:end_row]

        data = pd.DataFrame([], columns = ['Portfolio','Dept','Role','Title','Start','End'])


        data['Portfolio'] = d['Portfolio'].to_list()
        data['Dept'] = [dept]*len(d)
        data['Role'] = d['Role'].to_list()
        data['Title'] = d['Title'].to_list()
        data['Start'] = d['Start Date'].to_list()
        data['End'] = d['End Date'].to_list()

        portfolios = portfolios.append(data)

    return portfolios

def create_portfolio_tbl(Portfolios,parls):
    """
    Function to find all unique roles sittings by portfolio linked department

    """
    for i in range(len(parls)):
        #go through all parliaments by cylcing through dictionary of start row #s
        start_row = parliament_rows[parls[i]]+1
        try:
            #end of a parliament is the start row of the preceeding parliament row #
            end_row = parliament_rows[parls[i+1]]
        except:
            #for the bottom of the parliament
            end_row = -1

        #create dataset from filter so only rows in one parliament remain
        d = df.iloc[start_row:end_row]

        #get department names;all deptartment rows only have department name in 'Portfolio' Column
        depts = d[d['Name'].isna()]

        #get the start row #s for each department with portfolios in parliament
        dept_loc = get_department_rows(depts)

        #create dataframe of portfolios with column of assocoiated Departments
        portfolios = add_departments(dept_loc,d,end_row)

        #Append parlaiment datframe to master dataframe with all data
        Portfolios = Portfolios.append(portfolios)

    Portfolios.drop_duplicates()
    return Portfolios

def get_portfolio_roles(data):

    #Here we just want to grab select information from the table
    data = data[['Portfolio','Dept','Start','End']]
    data = data.drop_duplicates()

    #put oldest roles on the bottom
    data = data.sort_values(by='Start')

    #create new table of portfolio and department relationships
    Port_info = pd.DataFrame([], columns = ['Portfolio','Dept','Start','End'])
    for portfolio in tqdm(list(data['Portfolio'].unique())):

        d = data[data['Portfolio'] == portfolio]
        depts = list(d['Dept'].unique())

        if len(depts)>1:
            #if portfolio has had oversight from one 'Department'
            for i in range(len(depts)):
                d2 = d[d['Dept'] == depts[i]]
                oldest_start = d2['Start'].to_list()[0]#most recent sitting associated with dept is at the start of the list
                youngest_end = d2['End'].to_list()[-1]#oldest sitting associated with dept is at the end of the list

                port_info = pd.DataFrame([[portfolio,depts[i],oldest_start,youngest_end]], columns = ['Portfolio','Dept','Start','End'])
                Port_info = Port_info.append(port_info)

        else:
            try:
                oldest_start = d['Start'].to_list()[0]#oldest sitting associated with dept is at the end of the list
                youngest_end = d['End'].to_list()[-1]
            except:
                continue

            port_info = pd.DataFrame([[portfolio,depts[0],oldest_start,youngest_end]], columns = ['Portfolio','Dept','Start','End'])

            Port_info = Port_info.append(port_info)
    return Port_info


if __name__ == "__main__":
    parser  = create_argument_parser()
    #load command line args
    args = parser.parse_args()
    if args.run_type == 'proccess':
        s3 = aws_access()
        file_obj = s3.Bucket('polemics').Object("raw/ParlinfoFederalAreaOfResponsibilitiy.xlsx").get()
        df = pd.read_excel(io.BytesIO(file_obj['Body'].read()),'Sheet')

        file_obj = s3.Bucket('polemics').Object("references/elections.xlsx").get()
        elections = pd.read_excel(io.BytesIO(file_obj['Body'].read()),'elections')

        #Create dictionaruy pf row # for start for parliaments rows
        parliament_rows = get_parliament_rows(elections,df)
        parls = list(parliament_rows.keys())# ket keys to to index dictionary
        #create portfolio table with unique roles sittings by portfolio linked department
        data = create_portfolio_tbl(pd.DataFrame([], columns=['Portfolio','Dept','Role','Title','Start','End']),parls)
        #going to need this later to get % of time a role was active in a portfolio
        s3.Object('polemics', 'processed/portfolio_sittings_tbl.csv').put(Body=data.to_csv())
        print("Successfully uploaded to S3")
        #PUT get duration for each role here

        #
        # portfolio_roles = get_portfolio_roles(data)
        #
        #         Portfolios.to_csv('test.csv')
