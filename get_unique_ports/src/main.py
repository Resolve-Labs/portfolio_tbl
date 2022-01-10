import pandas as pd
import boto3
import os
import io
import argparse
import numpy as np
from datetime import datetime, date, timedelta

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

def get_dept_links(data):
    """
    Function to return rows of start and end dates for portfolios under each department oversight
    Accepts pandas dataframe of all portfolio role sittings
    """
    data = data[['Portfolio','Dept','Start','End']]
    data = data.drop_duplicates()


    #create new dataframe to fill with the youngest and oldest
    Port_info = pd.DataFrame([], columns = ['uid','portfolio','dept','start','end'])

    for j,portfolio in enumerate(list(data['Portfolio'].unique())):
        #look at the assocaited roles with each unique portfolio name
        d = data[data['Portfolio'] == portfolio]
        #look at all departments with oversight over portfolio at a given time
        depts = list(d['Dept'].unique())

        if len(depts)>1:
            for i in range(len(depts)):
                d2 = d[d['Dept'] == depts[i]]
                d2 = d2.sort_values(by='Start')
                oldest_start = d2['Start'].to_list()[0]#oldest sitting associated with dept is at the end of the list
                d2 = d2.sort_values(by='End')
                youngest_end = d2['End'].to_list()[-1]
                uid = str(j)+"."+str(i)
                #create dataframe for one row to append..
                port_info = pd.DataFrame([[uid,portfolio,depts[i],oldest_start,youngest_end]], columns = ['uid','portfolio','dept','start','end'])
                Port_info = Port_info.append(port_info)

        else:
            #if there is only one department associated with the portfolio
            try:
                d = d.sort_values(by='Start')
                oldest_start = d['Start'].to_list()[0]#oldest sitting associated with dept is at the end of the list
                d = d.sort_values(by='End')
                youngest_end = d['End'].to_list()[-1]#youngest sitting associated with dept is at the start of the list
            except:
                print("failure?",portfolio)
                continue
            uid = str(j)+"."+'0'
            #create dataframe for one row to append..
            port_info = pd.DataFrame([[uid,portfolio,depts[0],oldest_start,youngest_end]], columns = ['uid','portfolio','dept','start','end'])
            Port_info = Port_info.append(port_info)
    return Port_info

def get_role_stats(data,dept_data):
    """
    Function to return dataframe of portfolio role stats
    Accepts
        dept_data: a table of unique portofolio department links with start and end date
        data: a table of all role sittings with associated portfolios and department
    """
    active_pct, open_pct = [], []
    dept_data = dept_data.dropna(subset=['start']) #remove all rows with nan start dates

    #look at all sittings associated to each unique department-portoflio link (each row in dept_data)
    for row in range(len(dept_data)-1):

        #sample of all roles with given portfolio and department combo
        sample = data[ ( data['Portfolio'] == dept_data.iloc[row,1] ) & ( data['Dept'] == dept_data.iloc[row,2]) ]

        #get the portfolio start and end date and days between
        p_start = dept_data.iloc[row,3].date()
        p_end = dept_data.iloc[row,4].date()

        #get number of days between start and end date of portoflio recorded roles
        try:
            delta = p_start - p_end
            p_duration = -int(delta.days)
        except:
            #if the protfolio is active consider end date as today
            today = date.today()

            delta = p_start - today
            p_duration = -int(delta.days)

        #check first day portfolio was active
        day = p_start #re-define as it will change
        days, active = [],[]

        #create list of active days between portofolio start and end
        try:
            #look at each day between start and end date
            for ii in range(p_duration):
                days.append(day.strftime('%m/%d/%Y'))
                active_flag = 0
                #Look at each role to see if active on given day
                #look at each role with given portfolio and dept combo
                for s in range(len(sample)):
                    #get a role's start and end date
                    r_start = sample.iloc[s,5]#s = the row number of the subset
                    r_end = sample.iloc[s,6]

                    #the considered day is active if betwen the role start and end date
                    if r_start <= day <= r_end:
                        active.append(1)
                        active_flag = 1
                        break

                    else:
                        #check next if another role sittings if active day considered
                        continue

                if active_flag == 0:
                    #no roles were active on day
                    active.append(0)

                day += timedelta(days=1)#increment day by one on each loop

            try:
                active_pct.append(100*sum(active)/len(active))
                open_pct.append(len(active))# number of days between start and end date
            except:
                #If no items were appended to the lists
                active_pct.append(0)
                open_pct.append(0)

        except:
                #if incrementing the time delta failed (both nan?)
                days.append(day)
                active_pct.append(0)
                open_pct.append(0)


    dept_data['active_pct'] = active_pct
    dept_data['open_pct'] = open_pct
    print('success')

    return dept_data

def create_argument_parser():
    """
    Function to add command line arguments at run time
    """
    parser  = argparse.ArgumentParser(description = 'Script to test out pipeline')
    parser.add_argument('--run-type', nargs = '?', required = True, help = 'Command to run task or test')
    return parser

def aws_access():
    """
    Function to return connection resource with S3 bucket

    Accepts: aws_access, aws_key env variables with secret codes to connects with API
    """
    #amazon access keys from local env
    AWS_ACCESS_KEY_ID = os.getenv("aws_access")[1:-1]
    AWS_SECRET_ACCESS_KEY = os.getenv("aws_key")[1:-1]
    s3 = boto3.resource(service_name='s3',region_name='ca-central-1', aws_access_key_id=str(AWS_ACCESS_KEY_ID), aws_secret_access_key=str(AWS_SECRET_ACCESS_KEY))
    return s3

if __name__ == "__main__":
    parser  = create_argument_parser()
    #load command line args
    args = parser.parse_args()
    if args.run_type == 'proccess':
        s3 = aws_access()

        file_obj = s3.Bucket('polemics').Object("raw/ParlinfoFederalAreaOfResponsibilitiy.xlsx").get()
        df = pd.read_excel(io.BytesIO(file_obj['Body'].read()),'Sheet')

        #local for now
        # df = pd.read_excel('ParlinfoFederalAreaOfResponsibilitiy.xlsx')
        file_obj = s3.Bucket('polemics').Object("references/elections.xlsx").get()
        elections = pd.read_excel(io.BytesIO(file_obj['Body'].read()),'elections')

        # elections = pd.read_excel('elections.xlsx','elections')

        #Create dictionaruy pf row # for start for parliaments rows
        parliament_rows = get_parliament_rows(elections,df)

        #create portfolio table with unique roles sittings by portfolio linked department
        data = create_portfolio_tbl(pd.DataFrame([], columns=['Portfolio','Dept','Role','Title','Start','End']),list(parliament_rows.keys()))
        data = data[ data['Dept'] != 'Parliament: 09 (1901-02-06 - 1904-09-29)' ] #remove errenoues
         # data.to_csv('processed/portfolio_sittings_tbl2.csv')
        s3.Object('polemics', 'processed/portfolio_dept_tbl2.csv').put(Body=data.to_csv())

        file_obj = s3.Bucket('polemics').Object("processed/portfolio_dept_tbl2.csv").get()
        data = pd.read_csv(io.BytesIO(file_obj['Body'].read()),parse_dates=['start','end'])
        dept_data = get_dept_links(data)
        active_pct, open_pct = [], []
        #look at all sittings associated to each unique department-portoflio link (each row in dept_data)
        for row in range(len(dept_data)):

            sample = data[ ( data['Portfolio'] == dept_data.iloc[row,1] ) & ( data['Dept'] == dept_data.iloc[row,2])]
            #get the portfolio start and end date and days between
            p_start = dept_data.iloc[row,3].date()
            p_end = dept_data.iloc[row,4].date()
        #     print("--Portfolio: ",dept_data.iloc[row,1],'start: ',p_start.strftime('%m/%d/%Y'))
            today = date.today()
            #get number of days between start and end date of portoflio recorded roles
            try:
                delta = p_start - p_end
                delta = delta.days
                delta = -int(delta)
            except:
                #if the protfolio is active consider end date as today
                delta = p_start - today
                delta = delta.days
                delta = -int(delta)

            p_duration = delta

            #create a list of all dates between start and end date
            day = p_start
            p_days = []
            for i in range(p_duration):
                day += timedelta(days=1)
                p_days.append(day.strftime('%m/%d/%Y'))

            #create list of active days between portofolio start and end
            sample = sample[['Start','End']]
            sample.drop_duplicates(inplace=True)
            R_days = []
            for s in range(len(sample)):
                #get a role's start and end date
                r_start = sample.iloc[s,0]
                r_end = sample.iloc[s,1]
                #get number of days between start and end date of portoflio recorded roles
                try:
                    delta = r_start - r_end
                    delta = delta.days
                    delta = -int(delta)

                except:
                    #if the portfolio is active consider end date as today
                    delta = r_start.date() - today
                    delta = delta.days
                    delta = -int(delta)

                day = r_start
                r_days = []
                for i in range(delta):
                    day += timedelta(days=1)
                    r_days.append(day.strftime('%m/%d/%Y'))

                R_days = R_days + r_days

            R_days = list(set(R_days))

            try:
                active_pct.append(100*len(R_days)/len(p_days))
                open_pct.append(len(p_days))
            except:
                #If no items were appended to the lists
                active_pct.append(0)
                open_pct.append(0)


        dept_data['active_pct'] = active_pct
        dept_data['time_since_start'] = open_pct
        # print("successfully saved table")
        s3.Object('polemics', 'processed/portfolio_final_tbl.csv').put(Body=dept_data.to_csv())
        # data.to_csv('processed/portfolio_sittings_tbl2.csv')
        #s3.Object('polemics', 'processed/portfolio_sittings_tbl.csv').put(Body=data.to_csv())
        print("Successfully uploaded to S3")
