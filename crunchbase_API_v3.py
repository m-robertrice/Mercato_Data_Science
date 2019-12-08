import pandas as pd
import requests
from pandas.io.json import json_normalize
import time
import datetime
import winsound

sleep_time = .3

pd.set_option('display.max_columns', None)

params = {'user_key': '670809c794ef30792198fe25678dc361'}
url = 'https://api.crunchbase.com/v3.1/organizations'

querystring = {'user_key':'670809c794ef30792198fe25678dc361',
                'locations':'United States',
                'organization_types':'company',
                'category':'Embedded Software, Mobile Advertising, Education, Flash Storage, Network Security, '
                          'Quality Assurance, Pharmaceutical, Facial Recognition, Desktop Apps, Data Integration, '
                          'Subscription Service, Search Engine, Vacation Rental, Energy, Market Research, '
                          'Employee Benefits, Intelligent Systems, Enterprise, Energy Storage, Embedded Systems,'
                          'Fitness, Therapeutics, Men\'s, Collaboration, Consumer, EdTech, Identity Management,'
                          'Speech Recognition, E-Commerce, Internet, Private Cloud, Web Development, File Sharing,'
                          'Virtual World, Sales Automation, B2C, Generation Y, Business Intelligence, Virtual Assistant,'
                          'Task Management, Mobile Payments, Peer to Peer, Developer APIs, Cosmetics, Point of Sale,'
                          'IT Infrastructure, Data Center, Ad Targeting, Consumer Reviews, Google, CAD, Network Hardware,'
                          'Legal Tech, Penetration Testing, Cloud Infrastructure, Enterprise Applications, Cyber Security,'
                          'Genetics, Presentation Software, Fleet Management, Recruiting, CMS, Consumer Applications,'
                          'Quantified Self, SEO, Security, Electronic Design Automation (EDA), Web Hosting, SNS,'
                          'Consumer Software, SaaS, Document Management, Application Performance Management, Machine Learning,'
                          'Developer Tools, RFID, WebOS, Quantum Computing, Email, Photo Sharing, Lead Management,'
                          'Product Management, Marketplace, Computer, Cloud Computing, UX Design,'
                          'Natural Language Processing, Modbile Apps, eSports, Consumer Research, Mining Technology,'
                          'Supply Chain Management, Ad Retargeting, Assistive Technology, Meeting Software,'
                          'Credit Cards, Autonomous Vehicles, Augmented Reality, Consumer Electronics, '
                          'Prediction Markets, Health Diagnostics, Data Visualization, Messaging, Clinical Trials,'
                          'Business Information Systems, Event Management, Retail Technology, E-Signature,'
                          'Social Media Management, Personal Health, Analytics, Electronic Health Record (EHR),'
                          'Data Mining, Enterprise Resource Planning (ERP), Wireless, Simulation,'
                          'App Discovery, mHealth, PaaS, Software Engineering, A/B Testing, Computer Vision, '
                          'Logistics, Semantic Web, Bioinformatics, Darknet, Visual Search, Cloud Management,'
                          'Apps, Image Recognition, Video Streaming, Data Storage, Web Apps, Cloud Storage,'
                          'Vertical Search, Virtual Workforce, IT Management, IaaS, Semantic Search, Marketing Automation,'
                          'Digital Signage, Hardware, Software, Optical Communication, Developer Platform,'
                          'Knowledge Management, E-Commerce Platforms, Big Data, Wellness, Artificial Intelligence,'
                          'Database, Property Management, Predictive Analytics, Cloud Data Services, Scheduling,'
                          'Enterprise Software, B2B, Linux, Online Portals, Compliance, Project Management, Cloud Security,'
                          'Information Technology, Web Design, Usability Testing, Social CRM, FinTech, Privacy,'
                          'Information and Communications Technology (ICT), InsurTech, Text Analytics, E-Learning, '
                          'Virtualization, Open Source, Internet of Things, Product Research, Motion Capture, '
                          'Virtual Reality, Payments, Freemium, Human Computer Interaction, Lead Generation'}

#source: https://www.inc.com/business-insider/these-25-universities-produce-the-most-startup-founders.html
master_list_universities = ['Dartmouth College',	'New York University',	'Brigham Young University',	'University of Washington',	'University of Waterloo',	'Duke University',	'Carnegie Mellon University',	'University of Southern California',	'Brown University',	'Columbia University',	'University of Wisconsin',	'Technion - Isreal Institute of Technology',	'University of California, Los Angeles',	'Princeton University',	'Yale University',	'University of Illinois',	'University of Texas',	'Tel Aviv University',	'University of Michigan',	'Cornell University',	'University of Pennsylvania',	'Harvard University',	'Massachusetts Institute of Technology',	'University of California, Berkeley',	'Stanford University',	'Dartmouth',	'NYU',	'BYU',	'Duke',	'Carnegie Mellon',	'USC',	'Brown',	'Columbia',	'Technion',	'UCLA',	'Princeton',	'Yale',	'UT',	'Tel Aviv',	'Cornell',	'U Penn',	'Harvard',	'MIT',	'Berkeley',	'Stanford']
master_list_job_titles = [
    'Founder',
    'Chief Operating Officer',
    'Chief Revenue Officer',
    'Chief Marketing Officer',
    'Chief Executive Officer',
    'Chief Financial Officer',
    'Chief Technical Officer',
    'COO',
    'CRO',
    'CEO',
    'CMO',
    'CFO',
    'CTO',
    'Vice President',
    'VP',
    'Director',
    'Head'
]
master_list_degrees = [
    'PhD',
    'Masters',
    'MBA'
    'Bachelors',
    'Associates'
]
masters_list = [
    'M.A.', 'MA', 'MS', 'M.S.'
]
MBA_list = [
    'Masters of Business Administration', 'EMBA'
]
bachelors_list = [
    'B.S.','B.A.','B.E.','BS','BA','BME','BEE','BCE','B.ME.','B.EE.','B.CE.'
]
degree_conversion = {
    'PhD': ['Ph.D', 'PhD'],
    'Masters': ['Masters', 'M.A.', 'MA', 'MS', 'M.S.', 'Master'],
    'MBA': ['MBA', 'Masters of Business Administration', 'EMBA', 'M.B.A.'],
    'Bachelors': ['Bachelors', 'Bachelor', 'B.S.','B.A.','B.E.','BS','BA','BME','BEE','BCE','B.ME.','B.EE.','B.CE.', 'B.S', 'BFA', 'B.FA', 'B.F.A.', 'B.F.A',
                  'B.E', 'B.EE', 'B.ME', 'B.CE', 'B.Arch', 'B.B.A.', 'BBA', 'B.BA', 'B.B.A', 'BAAS'],
    'Associates': ['Associates', 'Associate', 'A.A.', 'A.S.', 'AA', 'AS', 'A.A', 'A.S']
}

#next time dont make a list of dictionaries, just list the key value pairs
job_title_conversion = [
    {'Chief Marketing Officer':'CMO'},
    {'Chief Executive Officer':'CEO'},
    {'Chief Operating Officer':'COO'},
    {'Chief Revenue Officer':'CRO'},
    {'Chief Technical Officer':'CTO'},
    {'Chief Financial Officer':'CFO'},
    {'Vice President': 'VP'}
]

raw_df = []

#eventually create a for loop that will tab through every page that crunchbase has to offer. Remmber to rate limit
for i in range(1):
    try:
        page = str(i+1)
        querystring = {'user_key': '670809c794ef30792198fe25678dc361',
                       'page':page,
                       'locations': 'United States',
                       'organization_types': 'company',
                       'category': 'Embedded Software, Mobile Advertising, Education, Flash Storage, Network Security, '
                                   'Quality Assurance, Pharmaceutical, Facial Recognition, Desktop Apps, Data Integration, '
                                   'Subscription Service, Search Engine, Vacation Rental, Energy, Market Research, '
                                   'Employee Benefits, Intelligent Systems, Enterprise, Energy Storage, Embedded Systems,'
                                   'Fitness, Therapeutics, Men\'s, Collaboration, Consumer, EdTech, Identity Management,'
                                   'Speech Recognition, E-Commerce, Internet, Private Cloud, Web Development, File Sharing,'
                                   'Virtual World, Sales Automation, B2C, Generation Y, Business Intelligence, Virtual Assistant,'
                                   'Task Management, Mobile Payments, Peer to Peer, Developer APIs, Cosmetics, Point of Sale,'
                                   'IT Infrastructure, Data Center, Ad Targeting, Consumer Reviews, Google, CAD, Network Hardware,'
                                   'Legal Tech, Penetration Testing, Cloud Infrastructure, Enterprise Applications, Cyber Security,'
                                   'Genetics, Presentation Software, Fleet Management, Recruiting, CMS, Consumer Applications,'
                                   'Quantified Self, SEO, Security, Electronic Design Automation (EDA), Web Hosting, SNS,'
                                   'Consumer Software, SaaS, Document Management, Application Performance Management, Machine Learning,'
                                   'Developer Tools, RFID, WebOS, Quantum Computing, Email, Photo Sharing, Lead Management,'
                                   'Product Management, Marketplace, Computer, Cloud Computing, UX Design,'
                                   'Natural Language Processing, Modbile Apps, eSports, Consumer Research, Mining Technology,'
                                   'Supply Chain Management, Ad Retargeting, Assistive Technology, Meeting Software,'
                                   'Credit Cards, Autonomous Vehicles, Augmented Reality, Consumer Electronics, '
                                   'Prediction Markets, Health Diagnostics, Data Visualization, Messaging, Clinical Trials,'
                                   'Business Information Systems, Event Management, Retail Technology, E-Signature,'
                                   'Social Media Management, Personal Health, Analytics, Electronic Health Record (EHR),'
                                   'Data Mining, Enterprise Resource Planning (ERP), Wireless, Simulation,'
                                   'App Discovery, mHealth, PaaS, Software Engineering, A/B Testing, Computer Vision, '
                                   'Logistics, Semantic Web, Bioinformatics, Darknet, Visual Search, Cloud Management,'
                                   'Apps, Image Recognition, Video Streaming, Data Storage, Web Apps, Cloud Storage,'
                                   'Vertical Search, Virtual Workforce, IT Management, IaaS, Semantic Search, Marketing Automation,'
                                   'Digital Signage, Hardware, Software, Optical Communication, Developer Platform,'
                                   'Knowledge Management, E-Commerce Platforms, Big Data, Wellness, Artificial Intelligence,'
                                   'Database, Property Management, Predictive Analytics, Cloud Data Services, Scheduling,'
                                   'Enterprise Software, B2B, Linux, Online Portals, Compliance, Project Management, Cloud Security,'
                                   'Information Technology, Web Design, Usability Testing, Social CRM, FinTech, Privacy,'
                                   'Information and Communications Technology (ICT), InsurTech, Text Analytics, E-Learning, '
                                   'Virtualization, Open Source, Internet of Things, Product Research, Motion Capture, '
                                   'Virtual Reality, Payments, Freemium, Human Computer Interaction, Lead Generation'}

        raw = requests.request("GET", url, params=querystring).json()['data']['items']
        #print(type(raw))
        #print('___________________________________________________________________________________________________________')
        raw_df.extend(raw)

        time.sleep(sleep_time)

    except:
        print('error')

    #print(i)
raw_df = json_normalize(raw_df)
#print(raw_df)


#take uuid and name of the company to create the greater data frame. following queries will be run off of this dataframe
master_df = pd.DataFrame(raw_df.pop('uuid'))

raw_name = raw_df.pop('properties.name')
raw_name.rename('name')

raw_primary_role = raw_df.pop('properties.primary_role')
raw_primary_role.rename('primary_role')

raw_description = raw_df.pop('properties.short_description')
raw_description.rename('description')

raw_city = raw_df.pop('properties.city_name')
raw_city.rename('city')

raw_region = raw_df.pop('properties.region_name')
raw_region.rename('region')

master_df = master_df.join(raw_name)
master_df = master_df.join(raw_primary_role)
master_df = master_df.join(raw_description)
master_df = master_df.join(raw_city)
master_df = master_df.join(raw_region)

#print(master_df)

count = 0
#property columns
description = []
closed_on = []
founded_on = []
is_closed = []

#relationship columns relationships.
acquired_by = []
categories = []
founders = []
funding_rounds = []
headquarters_city = []
headquarters_state = []
investors = []
ipo = []
news = []

counter = 0
for uuid in master_df['uuid']:
    params = {'user_key': '670809c794ef30792198fe25678dc361'}
    isclosed_url = 'https://api.crunchbase.com/v3.1/organizations' + '/' + str(uuid)

    subraw = requests.request('GET', isclosed_url, params=params).json()['data']

    subraw = json_normalize(subraw)
    #print(subraw)
    #print(subraw)
    #description.append(subraw['properties.short_description'].values)
    closed_on.append(subraw['properties.closed_on'].values)
    founded_on.append(subraw['properties.founded_on'].values)
    is_closed.append(subraw['properties.is_closed'].values)
    acquired_by.append(subraw['relationships.acquired_by.paging.first_page_url'].values)
    categories.append(subraw['relationships.categories.items'].values)
    founders.append(subraw['relationships.founders.items'].values)
    funding_rounds.append(subraw['relationships.funding_rounds.items'].values)
    investors.append(subraw['relationships.investors.items'].values)
    ipo.append(subraw['relationships.ipo.paging.first_page_url'].values)
    news.append(subraw['relationships.news.items'].values)

    #print(counter)
    counter += 1

    #pprint.pprint(subraw['properties.short_description'])
    time.sleep(sleep_time)

print('break 1')
'''
for companies in founded_on:
    print(companies)
'''
#print(is_closed)
#master_df['description'] = pd.Series(description)
master_df['closed_on'] = pd.Series(closed_on)
master_df['founded_on'] = pd.Series(founded_on)
master_df['is_closed'] = pd.Series(is_closed)
master_df['acquired_by'] = pd.Series(acquired_by)
master_df['categories'] = pd.Series(categories)
master_df['founders'] = pd.Series(founders)
master_df['funding_rounds'] = pd.Series(funding_rounds)
#master_df['headquarters_city'] = pd.Series(headquarters_city)
#master_df['headquarters_state'] = pd.Series(headquarters_state)
#master_df['investors'] = pd.Series(investors) #remove investors b/c no data is returned that is additive to what we get from funding rounds
master_df['ipo'] = pd.Series(ipo)
master_df['news'] = pd.Series(news)

print('break 2')

#convert founded on date to date time object
count_temp1 = 0
temp_founded_on_list = []
for companies in master_df['founded_on']:
    #print(companies)
    for dates in companies:
        if dates is not None and dates is not 'None':
            temp_founded_on_list.append(datetime.datetime.strptime(dates, '%Y-%m-%d'))
        else:
            temp_founded_on_list.append(None)
master_df['founded_on'] = pd.Series(temp_founded_on_list)


########################################################################################################################
#Need to add try and except logic to each of the sections below
########################################################################################################################

#Failed counters
categories_failed_count = 0
founders_failed_count = 0
rounds_failed_count = 0
news_failed_count = 0
degrees_failed_count = 0
experience_failed_count = 0

########################################################################################################################


print('break 3')

#clean categories so that only the actual category groups are left
categories_new = []
count  = 0
for companies in master_df['categories']:
    try:
        temp_list_companies = []
        #print(companies)
        if companies:#pd.isna(companies):
            for items in companies[0]:
                if 'properties' in items.keys():
                    #print(count, 'properties')
                    if items['properties']['category_groups']:
                        #print(count)
                        temp_list = items['properties']['category_groups']
                        temp_list_companies.append(temp_list[0])
                        #temp_list_companies.append(items['properties']['category_groups'][0])

            new_temp_list = []
            for words in temp_list_companies:
                if words not in new_temp_list:
                    new_temp_list.append(words)
            if new_temp_list:
                categories_new.append(new_temp_list)
            else:
                categories_new.append(None)
        else:
            categories_new.append(None)
            #print(companies)
    except:
        categories_new.append(None)
        categories_failed_count += 1
    count += 1
#print(categories_new)
master_df['categories2'] = pd.Series(categories_new)

########################################################################################################################

print('break 4')

#clean founders column
founders_uuid = []
for companies in master_df['founders']:
    try:
        temp_list_founders = []
        if not None:#pd.isna(companies):
                if companies:
                    for items in companies[0]:
                        if items['uuid']:
                            temp_list_founders.append(items['uuid'])
                    founders_uuid.append(temp_list_founders)
                else:
                    founders_uuid.append(None)
                    #print(companies)
        else:
            #print(companies)
            founders_uuid.append(None)
        #print(temp_list_founders)
    except:
        founders_uuid.append(None)
        founders_failed_count += 1


master_df['founders'].update(pd.Series(founders_uuid, name='founders_uuid'))

########################################################################################################################


print('break 5')

#clean funding rounds column
rounds_information = []
for companies in master_df['funding_rounds']:
    try:
        temp_list_rounds = []
        if not None:#pd.isna(companies):
                if companies:
                    for items in companies[0]:
                        temp_list_sub = {}
                        if items:
                            if 'properties' in items.keys():
                                #print(items)
                                temp_list_sub['date'] = items['properties']['announced_on']
                                temp_list_sub['amount_raised'] = items['properties']['money_raised']

                                if 'relationships' in items.keys():
                                    for investments in items['relationships']['investments']:
                                        temp_list_investors = []
                                        if 'relationships' in investments.keys():
                                            if 'investors' in investments['relationships'].keys():
                                                if 'properties' in investments['relationships']['investors'].keys():
                                                    if 'name' in investments['relationships']['investors']['properties'].keys():
                                                        temp_list_investors = investments['relationships']['investors']['properties']['name']

                                temp_list_sub['investors'] = temp_list_investors
                                #print(temp_list_sub)
                        temp_list_rounds.append(temp_list_sub)

                    rounds_information.append(temp_list_rounds)
                else:
                    rounds_information.append([None])
                    #print(companies)
        else:
            #print(companies)
            rounds_information.append([None])
        #print(temp_list_rounds)
    except:
        rounds_information.append([None])
        rounds_failed_count += 1

master_df['funding_rounds2'] = pd.Series(rounds_information)

########################################################################################################################

print('break 6')

#clean news column
news_count = []
for companies in master_df['news']:
    #print(companies)
    try:
        temp_news = []
        if companies:
            for news in companies[0]:
                temp_news.append(datetime.datetime.strptime(news['properties']['posted_on'], '%Y-%m-%d'))
            news_count.append(temp_news)
        else:
            news_count.append(float(0))
    except:
        news_count.append(float(0))

    news_failed_count += 0
master_df['news'].update(pd.Series(news_count, name='news'))

########################################################################################################################
'''
count_temp1 = 0
for companies in master_df['founders']:
    print('Company #' + str(count_temp1))
    pprint.pprint(companies)
    count_temp1+=1
'''

#degree information parsed
founders_degree_info = []
founders_experience = []

temp_counter = 0
for founder in master_df['founders']:
    try:
        #founder degree information
        if founder is not None:
            degree_info_list = []
            for uuid in founder:

                url = 'https://api.crunchbase.com/v3.1/people' + '/' + str(uuid) + '/' + 'degrees'

                subraw = requests.request('GET', url, params=params).json()['data']
                subraw = json_normalize(subraw)

                if subraw['items'][0]:
                    for degree in subraw['items'][0]:
                        temp_degree_info = {}
                        degree = json_normalize(degree)
                        temp_degree_info['completed_date'] = degree['properties.completed_on'][0]
                        temp_degree_info['degree_type'] = degree['properties.degree_type_name'][0]
                        temp_degree_info['degree_subject'] = degree['properties.degree_subject'][0]
                        temp_degree_info['school'] = degree['relationships.school.properties.name'][0]
                        degree_info_list.append(temp_degree_info)

                else:
                    degree_info_list.append(None)
                #print(degree_info_list)


            founders_degree_info.append(degree_info_list)

        else:
            founders_degree_info.append(founder)
    except:
        founders_degree_info.append(founder)
        degrees_failed_count += 1
    time.sleep(sleep_time)


    try:
        #founder job experience information
        if founder is not 'None':
            founders_job_list = []
            for uuid in founder:
                url = 'https://api.crunchbase.com/v3.1/people' + '/' + str(uuid) + '/' + 'jobs'

                subraw = requests.request('GET', url, params=params).json()['data']
                #print(subraw['items'])

                if subraw['items']:
                    if subraw['items'][0]:
                        for job in subraw['items']:
                            temp_job_info = {}
                            job = json_normalize(job)

                            temp_job_info['started_on'] = job['properties.started_on'][0]
                            temp_job_info['job_category'] = job['properties.job_type'][0]
                            temp_job_info['job_title'] = job['properties.title'][0]

                            temp_job_info['job_company_description'] = job['relationships.organization.properties.short_description'][0]

                            founders_job_list.append(temp_job_info)
                else:
                    founders_job_list.append(None)

            founders_experience.append(founders_job_list)

        else:
            founders_experience.append(None)

    except:
        founders_experience.append(None)
        experience_failed_count += 1

    time.sleep(sleep_time)
    temp_counter+=1
master_df['founders_degree_info'] = pd.Series(founders_degree_info)
master_df['founders_experience'] = pd.Series(founders_experience)


print('break 7')
'''
for index, companies in master_df.iterrows():
    print(index, companies['founded_on'], companies['acquired_by'], companies['ipo'])
'''
########################################################################################################################
#remove all instances of none in the job experience and education
#master_df.to_csv('master_df.csv')
#print(master_df.head())
for degrees in master_df['founders_degree_info']:
    if degrees is not None:
        for degree in list(degrees):
            if degree is 'None':
                degrees.remove(degree)

for jobs in master_df['founders_experience']:
    if jobs is not None:
        for job in list(jobs):
            if job is 'None':
                jobs.remove(job)

########################################################################################################################
#convert funding round date to datetime format

for companies in master_df['funding_rounds2']:
    if companies is not 'None' and companies is not None:
        for round in companies:
            if round is not None and round is not 'None':
                #print(round)
                if round['date'] is not 'None' and round['date'] is not None:
                    round['date'] = datetime.datetime.strptime(round['date'], '%Y-%m-%d')

########################################################################################################################
acquired_date = []
ipo_date = []
walkingdead_date = [] #the assumption on the walking dead date is that the company is a walking dead after 4 years

for index, row in master_df.iterrows():

    acquired_by_url = row['acquired_by'][0]
    acquired_by_raw = requests.request('GET', acquired_by_url, params=params).json()['data']['items']
    if not acquired_by_raw:
        acquired_date.append(None)
    else:
        date = datetime.datetime.strptime(acquired_by_raw[0]['properties']['announced_on'],'%Y-%m-%d')
        acquired_date.append(date)

    time.sleep(sleep_time)

    ipo_url = row['ipo'][0]
    ipo_raw = requests.request('GET', ipo_url, params=params).json()['data']['items']
    if not ipo_raw:
        ipo_date.append(None)
    else:
        date = datetime.datetime.strptime(ipo_raw[0]['properties']['went_public_on'], '%Y-%m-%d')
        ipo_date.append(date)

    time.sleep(sleep_time)

    temp_funding_dates = []
    if row['funding_rounds2'] is not 'None' and row['funding_rounds2'] is not None:
        for rounds in row['funding_rounds2']:
            if rounds is not None:
                if rounds['date'] is not None and rounds['date'] is not 'None':
                    temp_funding_dates.append(rounds['date'])

    if temp_funding_dates:
        walkingdead_date_temp = max(temp_funding_dates) + datetime.timedelta(days=(365*4))

    if temp_funding_dates:
        if walkingdead_date_temp < datetime.datetime.today():
            walkingdead_date.append(True)
        else:
            walkingdead_date.append(False)
    else:
        walkingdead_date.append(False)

master_df['acquired_date'] = pd.Series(acquired_date)
master_df['ipo_date'] = pd.Series(ipo_date)
master_df['walkingdead_date'] = pd.Series(walkingdead_date)

#TODO: Create master column for exit before stratifying the data
    #TODO: Verify that the above 'TODO' is working
#TODO: there are companies that have acquried or IPO dates but no founding date, need to find some averages to back into founded date
#TODO: stratify by year
#TODO: clean categories
    #TODO: determine which categories will be put in an 'other' bucket
    #TODO: determine which categories can be bundled ie advertising technology and adtech
#TODO: clean degrees types
#TODO: clean degree subjects
#TODO: handle degree exceptions where the the completed date is none, need to specify that these degrees were likely never finished and provision them accordingly
#TODO: Review schools list, I used a list of schools that have seen the most founders, but that will create a bias, I should simply include top schools regardless of who comes from them
#TODO: Convert schools - University of California Los Angeles should be converted to UCLA etc.

#TODO: Create an investors columns, add investors conversion and section to function

print('break 8')
outcome = []
for index, companies in master_df.iterrows():
    #print(index, companies['founded_on'], companies['acquired_by'], companies['ipo'], companies['is_closed'])
    #print(type(companies['walkingdead_date']))

    if companies['acquired_date'] is not None and companies['acquired_date'] is not pd.NaT:
        outcome.append(1)
    elif companies['ipo_date'] is not None and companies['ipo_date'] is not pd.NaT:
        outcome.append(1)
    elif companies['walkingdead_date'] is not None and companies['walkingdead_date'] is not pd.NaT:
        outcome.append(0)
    elif companies['closed_on'][0] is True:
        outcome.append(0)
    else:
        outcome.append(None)

master_df['outcome'] = pd.Series(outcome)



print('break 9')

#TODO: add total funding limit where after so much raised it can be assumed an exit was successful
def year_segmentation(df, year, degree_conversion, master_list_universities, master_list_job_titles, job_title_conversion):
    # create new columns for degree type, subject, and school
    degree_types = []
    degree_subjects = []
    degree_schools = []
    total_funding = []
    investors = []

    # create new columns for experience
    job_titles = []

    for index, row in df.iterrows():
        # print(index, row['founders_degree_info'])
        # clean news data
        temp_news = 0
        test = datetime.datetime(2019, 1, 1, 0, 0)
        if type(row['news']) is list:
            for news in list(row['news']):
                # print(index, news, row['founded_on'])
                if (row['founded_on'] + datetime.timedelta(
                        days=365 * year)) > news:  # try 20 years to see if the logic is at least working.
                    temp_news += 1
        row['news'] = temp_news

#TODO: Add time intelligence to funding block & create an investors column

        if row['funding_rounds2'] is not 'None':
            for funding in row['funding_rounds2']:
                if funding is not None:
                    if type(row['founded_on']) is datetime.datetime and type(funding['date']) is datetime.datetime:
                        if row['founded_on'] + datetime.timedelta(days=365 * year) > funding['date']:
                            row['funding_rounds2'].remove(funding)

        total_funding_temp = 0
        if row['funding_rounds2'] is not 'None':
            for funding in row['funding_rounds2']:
                if funding is not None:
                    if funding['amount_raised'] is not None:
                        total_funding_temp += funding['amount_raised']
        total_funding.append(total_funding_temp)

        investors_temp = []
        if row['funding_rounds2'] is not 'None':
            for funding in row['funding_rounds2']:
                if funding is not None:
                    if funding['investors'] is not None:
                        investors_temp.append(funding['investors'])
        if investors_temp:
            investors.append(investors_temp)
        else:
            investors.append(None)

        # clean founder education
        # reduce degrees down to 'Type' 'Subject' and 'University'
        temp_degree_type = []
        temp_degree_subject = []
        temp_degree_school = []
        if row['founders_degree_info']:
            if row['founders_degree_info'] is not 'None':
                for degrees in list(row['founders_degree_info']):
                    if degrees is not None:
                        if degrees['completed_date'] is not None:
                            if type(degrees['completed_date']) is not datetime.datetime:
                                degrees['completed_date'] = datetime.datetime.strptime(degrees['completed_date'],
                                                                                           '%Y-%m-%d')
                            # print(type(row['founded_on']), type(degrees['completed_date']))
                            if type(row['founded_on']) is datetime.datetime and type(
                                    degrees['completed_date']) is datetime.datetime:
                                if row['founded_on'] + datetime.timedelta(days=365 * year) > degrees[
                                    'completed_date']:
                                    row['founders_degree_info'].remove(degrees)

                        if degrees['degree_type'] is not None and degrees['degree_type'] is not 'None':
                            temp_degrees = 'Other'
                            for key in degree_conversion:
                                # print(index, 'Key --> ', key)
                                # print(index, 'Value --> ', degree_conversion[key])
                                # print(index, 'Type --> ', type(degree_conversion[key]))
                                # print(index, degrees['degree_type'])
                                if degrees['degree_type'] in degree_conversion[key]:
                                    # print('**********************success************************')
                                    temp_degrees = key

                            temp_degree_type.append(temp_degrees)

                        if degrees['degree_subject'] is not None and degrees['degree_subject'] is not 'None':
                            temp_degree_subject.append(degrees['degree_subject'])

                        if degrees['school'] is not None and degrees['school'] is not 'None':
                            if degrees['school'] in master_list_universities:
                                temp_degree_school.append(degrees['school'])
                            else:
                                temp_degree_school.append('Other')

        # clean founder experience
        temp_job_title = []
        if row['founders_experience']:
            if row['founders_experience'] is not 'None':
                for jobs in list(row['founders_experience']):
                    # print(degrees)
                    if jobs is not 'None' and jobs is not None:
                        if jobs['started_on'] is not 'None':
                            if jobs['started_on'] is not None:
                                if type(jobs['started_on']) is not datetime.datetime:
                                    jobs['started_on'] = datetime.datetime.strptime(jobs['started_on'], '%Y-%m-%d')
                                    # print(type(row['founded_on']), type(degrees['completed_date']))
                                if type(row['founded_on']) is datetime.datetime and type(
                                        jobs['started_on']) is datetime.datetime:
                                    if row['founded_on'] + datetime.timedelta(days=365 * year) > jobs['started_on']:
                                        row['founders_experience'].remove(jobs)
                        if jobs['job_title'] is not None and jobs['job_title'] is not 'None':
                            # print(index, jobs['job_title'])
                            for titles in master_list_job_titles:
                                if titles in jobs['job_title']:
                                    # print(index, titles)
                                    temp_title = titles
                                    for conversion in job_title_conversion:
                                        for key, value in conversion.items():
                                            # print(index, 'key->', key)
                                            # print(index, 'value->', value)
                                            if titles is key:
                                                # print('inside final loop')
                                                # print(index, titles)
                                                # print(index, 'conversion->', value)
                                                temp_title = value
                                    temp_job_title.append(temp_title)

        if row['acquired_date'] is not None and row['founded_on'] is not None:
            if row['acquired_date'] < (row['founded_on'] + datetime.timedelta(days=365 * year)):
                df.drop(index, axis=0)

        if row['ipo_date'] is not None and row['founded_on'] is not pd.NaT:
            if row['ipo_date'] < (row['founded_on'] + datetime.timedelta(days=365 * year)):
                df.drop(index, axis=0)

        # filter funding data
        if row['funding_rounds2'] is not 'None':
            # print(index, row['funding_rounds2'])
            for rounds in list(row['funding_rounds2']):
                if rounds is not None:
                    if row['founded_on'] is not pd.NaT:
                        if rounds['date'] > (row['founded_on'] + datetime.timedelta(days=365 * year)):
                            row['funding_rounds2'].remove(rounds)
        if temp_degree_type:
            degree_types.append(temp_degree_type)
        else:
            degree_types.append(None)

        if temp_degree_subject:
            degree_subjects.append(temp_degree_subject)
        else:
            degree_subjects.append(None)

        if temp_degree_school:
            degree_schools.append(temp_degree_school)
        else:
            degree_schools.append(temp_degree_school)

        if temp_job_title:
            job_titles.append(temp_job_title)
        else:
            job_titles.append(None)

        # print(index, row['founders_experience'])
        # print(index, row['founders_degree_info'])

    df['degree_types'] = pd.Series(degree_types)
    df['degree_subjects'] = pd.Series(degree_subjects)
    df['degree_schools'] = pd.Series(degree_schools)

    df['job_titles'] = pd.Series(job_titles)

    df['total_funding'] = pd.Series(total_funding)

    df['investors'] = pd.Series(investors)

    return df


#year_one_df = year_segmentation(master_df,1,degree_conversion,master_list_universities,master_list_job_titles,job_title_conversion)
#year_two_df = year_segmentation(master_df,2,degree_conversion,master_list_universities,master_list_job_titles,job_title_conversion)
#year_three_df = year_segmentation(master_df,3,degree_conversion,master_list_universities,master_list_job_titles,job_title_conversion)
#year_four_df = year_segmentation(master_df,4,degree_conversion,master_list_universities,master_list_job_titles,job_title_conversion)
year_five_df = year_segmentation(master_df,5,degree_conversion,master_list_universities,master_list_job_titles,job_title_conversion)

'''
for index, row in year_five_df.iterrows():
    print(index)
    print(row['categories2'])
    print(type(row['categories2']))
'''

year_five_df.to_pickle('year_five_df.pkl')

########################################################################################################################
#print(master_df)

for index, row in year_five_df.iterrows():
    print(index, row['degree_types'], row['investors'], row['categories2'])

########################################################################################################################
#track the number of failed data collection attempts
print('-----------------------------------------------------------------------------------------------------------------')
print('Categories failed count = ' + str(categories_failed_count))
print('Founders failed count = ' + str(founders_failed_count))
print('Rounds failed count = ' + str(rounds_failed_count))
print('News failed count = ' + str(news_failed_count))
print('Degrees failed count = ' + str(degrees_failed_count))
print('Experience failed count = ' + str(experience_failed_count))
print('-----------------------------------------------------------------------------------------------------------------')

duration = 1000 #milliseconds
freq = 600 #Hz
winsound.Beep(freq, duration)
time.sleep(.7)
winsound.Beep(freq, duration)

