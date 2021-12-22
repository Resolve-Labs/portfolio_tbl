import pandas as pd
import boto3
import os
import io
import argparse
from datetime import timedelta
from datetime import datetime, date



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
    for row in range(len(dept_data)-1):
        #look at all sittings associated to each unique department link
        sample = data[ ( data['Portfolio'] == dept_data.iloc[row,2] ) & ( data['Dept'] == dept_data.iloc[row,3] )]

        #get the portfolio start and end date and days between
        Start = dept_data.iloc[row,3].date()
        End = dept_data.iloc[row,4].date()
        try:
            delta = Start - End
            delta = delta.days
        except:
            today = date.today()
            delta = Start - today
            delta = delta.days

        #create list of active days between portofolio start and end
        days, active = [],[]
        try:
            x = int(delta)
            for ii in range(x):
                #look at each day between start and end date
                Start += timedelta(days=1)
                days.append(Start.strftime('%m/%d/%Y'))
                #See if the each day has an active role
                for s in range(len(sample)-1):

                    #get role start and end date
                    start = sample.iloc[s,5]
                    end = sample.iloc[s,6]

                    #if the date is betwen one of the roles day is active
                    if start <= Start <= end:
                        active.append(1)
                        print(active)
                        break

                    else:
                        #check next role if not active during session
                        continue

                if len(active)<1:
                    active.append(0)

            try:
                print('Success',Start)
                active_pct.append(100*sum(active)/len(active))
                open_pct.append(len(active))
            except:
                #If no items were appended to the lists
                print('Failure',Start)
                active_pct.append(0)
                open_pct.append(0)

        except:
              #if incrementing the time delta failed
              pass


    dept_data['active_pct'] = active_pct
    dept_data['open_pct'] = open_pct
    print("returned")

    return dept_data

if __name__ == "__main__":
    parser  = create_argument_parser()
    #load command line args
    args = parser.parse_args()
    if args.run_type == 'proccess':
        s3 = aws_access()
        file_obj = s3.Bucket('polemics').Object('processed/portfolio_sittings_tbl.csv').get()
        data = pd.read_csv(io.BytesIO(file_obj['Body'].read()),parse_dates=['Start','End'])
        #get unique portfolio department links
        dept_data = get_dept_links(data)
        dept_data = get_role_stats(data,dept_data)

        print("successfully saved table")

        s3.Object('polemics', 'processed/portfolio_dept_tbl2.csv').put(Body=dept_data.to_csv())
