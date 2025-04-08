import requests
import json
import logging
import sendgrid
import configparser
from datetime import datetime,timedelta
import concurrent.futures as TP
from sendgrid.helpers.mail import Mail, Email, To,Bcc,Cc, Content, Attachment,Personalization,FileContent,FileName,FileType,Disposition
import os
import sys
import configparser
from pyspark.sql import SparkSession
import psycopg2
from decimal import Decimal
from encryption.encryption import EncryptionManager


# Define path to configuration files
configPath = r'<path_to_config_files>'  # Replace with actual path to config files or use environment variables

# Config file for the pipeline
config_file = os.path.join(configPath, '<pipeline_config_file>.ini')  # Replace with actual config file name
config = configparser.ConfigParser()
config.sections()
config.read(config_file)

# Config file for credentials (e.g., PostgreSQL, ADP API)
creds_file = r'<path_to_credentials_file>/master_config_file.ini'  # Replace with actual path
creds = configparser.ConfigParser()
creds.sections()
creds.read(creds_file)

# JSON data file path
json_path = os.path.join(configPath, 'json_data.json')  # Replace with actual path

# SSL certificates (make sure these are not shared in public repositories)
clientCrt = os.path.join(configPath, "<certificate_file>.pem")  # Replace with actual certificate file name
clientKey = os.path.join(configPath, "<auth_key_file>.key")  # Replace with actual key file name

# PostgreSQL credentials from the creds file
host = creds['key']['hostname']
port = creds['key']['port']
user = creds['key']['username']
password = creds['key']['password']

# ADP API credentials from the creds file
client_id = creds['key']['client_id']
client_secret = creds['key']['client_secret']

# Database connection details (using environment variables for sensitive data)
key_schema = os.getenv('DB_SCHEMA', '<default_schema>')  # Use environment variable for schema name
key_db = os.getenv('DB_NAME', '<default_db>')  # Use environment variable for database name
key_port = os.getenv('DB_PORT', '<default_port>')  # Use environment variable for port
key_host = os.getenv('DB_HOST', '<default_host>')  # Use environment variable for host
url_dec = "jdbc:postgresql://{}:{}/{}?currentSchema={}".format(key_host, key_port, key_db, key_schema)

# Config variables for pending ADP records and PostgreSQL driver
pending_adp = config['key']['pending_adp']
driver = config['key']['driver']

encryption_manager = EncryptionManager()


spark = SparkSession.builder.master("local").appName("backbone_pipeline_v1").getOrCreate()
spark.sparkContext.setLogLevel('FATAL')
log4jLogger = spark._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.debug("initializing backbone_pipeline version 1.0 spark job..")

'''pass country as argument to this job to process onboards for country specific programs i.e. US, CANADA '''
country =  sys.argv[1]
'''pass environment as second argument to this job to process onboards for country specific programs i.e. OLTP, OLTPSANDBOX '''
env = sys.argv[2]
url = 'jdbc:postgresql://{0}:{1}/{2}?currentSchema={3}'.format(host,port,env,'mdb')
log_path = config['path']['log_path']
config_path= config['path']['config_path']

def job_logger(msg,lvl='info'):
    logging.basicConfig(filename=os.path.join(log_path,'{}_{}_{}.log'.format('backbone_pipeline',env,datetime.now().strftime('%Y%m%d'))),
                format='%(asctime)s - %(message)s',
                filemode='a')
    # create logger object
    logger =  logging.getLogger("py4j")
    # Setting threshold of logger
    logger.setLevel(logging.INFO)
    if lvl == 'info':
        logger.info(msg)
    elif lvl == 'debug':
        logger.info(msg)
    elif lvl == 'error':
        logger.info(msg)
    elif lvl == 'warning':
        logger.info(msg)
    else:
        logger.info(msg)

def fetch_data_dec(table,url=url_dec,username=user,password=password,driver=driver):
  try:
    df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", table) \
    .option("user", username) \
    .option("password", password)\
    .option("driver", driver) \
    .load()
    return df
  except BaseException as e:
    job_logger('Could not connect to DB...\n{}'.format(e),'error')
    return None

def generate_token(cert=(clientCrt,clientKey)):
    try:
        url = "https://accounts.adp.com/auth/oauth/v2/token"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'
                   }
        payload='grant_type=client_credentials&client_id=&client_secret='
        response = requests.request("POST", url, headers=headers, data=payload,cert=cert)
        my_token = response.json()
        job_logger(response.text)
        vtoken = my_token['access_token']
        job_logger('ADP token successfully generated: {}'.format(vtoken))
        return vtoken
    except BaseException as e:
        job_logger('error: {}'.format(e),'error')
        return None

def populate_sendgrid_log(message,status,data):
    '''internal pipeline messaging method upon failures or Fatal Logging level'''
    sendgrid_key='' #add your key
    send_grid_cli = sendgrid.SendGridAPIClient(api_key=sendgrid_key)
    nickname = data['nickname']
    lastname = data['lastname']
    firstname = data['firstname']
    legalname = firstname + " " + lastname
    costname = data['costname']
    personalcontact = data['personalcontact']
    oslemployeeid = data['oslemployeeid']
    locationstate = data['locationstate']
    onboardprimeemail = data['onboardprimeemail']
    from_email = Email("noreply@oslrs.com","BACKBONE ERRORS")  # Change to your verified sender
    mail_body,to_email = None,None
    subject = "ADP Error code {} - {} {} {}".format(status, legalname,costname,locationstate)
    if message == "The new hire's file number must be unique within the company.":
        to_email = [To("youremail@.com")]  # Change to your recipient
        mail_body = "{} \n\n{}\n\n{}\n{}\n{}".format(message,'Please delete this Profile on ADP:',legalname,personalcontact,oslemployeeid)
    elif 'API status code on creating a new ADP account :504' in message:
        to_email = [To("youremail@.com")] # Change to your recipient
        mail_body = message
    else:
        to_email = [To("youremail@.com")]  # Change to your recipient
        mail_body = message
    content = Content("text/plain", mail_body)
    mail = Mail(from_email, to_email, subject, content)
    #Get a JSON-ready representation of the Mail object
    mail_json = mail.get()
    # Send an HTTP POST request to /mail/send
    response = send_grid_cli.client.mail.send.post(request_body=mail_json)
    pass

def get_profiles(stmt):
    '''fetch profiles from employee profile '''
    job_logger('fetching PENDING ADP records from employee profile.')
    df = spark.read.format("jdbc") \
                .option("url", url) \
                .option("query", stmt) \
                .option("user", user) \
                .option("password", password)\
                .option("driver", driver) \
                .load()
    records = df.count()
    df_dec = fetch_data_dec('encrypt_value_table')
    df_dec.createOrReplaceTempView('encrypt_value_table')
    df.createOrReplaceTempView('emp_profile')
    sql =  "select e.*,concat(split(e.enctypted_key,' ')[0],' ',k.key_val) attr_new from emp_profile e \
    inner join encrypt_value_table k on cast(e.uniqueid as int) = cast(k.uniqueid as int)"
    df = spark.sql(sql)
    df = df.drop("current_col")
    df = df.withColumnRenamed("current_col", "new_col")
    job_logger('PENDING ADP incoming records:{}'.format(records))
    collect_ = df.distinct().collect()
    profile_,all_profiles = {},[]
    job_logger('parsing the PENDING ADP records to a Json.')
    for i in collect_:
        empty_= i.asDict()
        profile_[empty_['oslemployeeid']] = empty_
    for k,v in profile_.items():
        all_profiles.append({k:v})
    job_logger('fetching PENDING ADP records from employee profile completed.')
    return all_profiles


def create_adp_profile(token,data):
    url = "https://api.adp.com/hcm/v2/applicant.onboard"
    
    rate = ''
    salary = ''
    management = False
    if data['peoplemanager'] == 'Yes':
        management = True
    if data["regularpayrate"] and data["regularpayrate"] == '1':
        rate = data["payamount"]
        salary = '{}'.format("hourlyRateAmount")
    elif data["regularpayrate"] and data["regularpayrate"] == '8' and data['paycodegroup'] != '6A1':
        rate = "{:.2f}".format(int(data["payamount"])/ 26)
        salary = '{}'.format("annualRateAmount")
    elif data["regularpayrate"] and data["regularpayrate"] == '8':
        rate = "{:.2f}".format(int(data["payamount"])/ 52)
        salary = '{}'.format("annualRateAmount")

    payload = {"applicantOnboarding": {
            "onboardingTemplateCode": {"code": "", # add your template code
                                        "name": ""
                                        },
                                        "onboardingStatus": {
                                        "statusCode": {"code": "complete",
                                                        "name": "complete"}
                                                        },
            "workerID": {"id": ""},
            "employmentEligibilityOptionCode": {
            "code": "0" },
            "preHireIndicator": False,
            "employmentEligibilityProfile": {
            "employerOrganization": {
                "locationNameCode": {
                    "code": ""
                }
            }
        },
        "onboardingExperienceCode": {
            "code": "default"
        },
        "applicantWorkerProfile": {
        "selfEmployedWorkerTypeCode": {
                "code": "1"},
            "hireDate": data["adp_hiredate"],
                "hireReasonCode": {
                "code": "new",
                    "name": "MYHIRE - My Hire"
            },
            "workersCompensationCoverage": {
                "coverageStatus": {
                    "code": ""
                },
                "coverageTypeCode": {
                    "code": data["empstateprovince"]
                    },
                "jobClassificationTypeCode": {
                    "code": ""
                },
                "coverageClassTypeCode": {
                    "code": ""
                }
            },
            "reportsTo": {
                "positionID": data["reportstoassociateid"]
            },
            "homeWorkLocation": {
                "nameCode": {
                    "code": data["homeworklocation"],
                        "name": ""
                },
                "homeshoreIndicator": False
            },
            "job": {
                "jobCode": {
                    "code": data["jobtitlecode"],
                        "name": data["jobtitlecode"] + " - " + data["jobtitle"]
                },
                "wageLawCoverages": [
                    {
                        "wageLawNameCode": {
                            "code": "",
                            "name": "E - Exept"
                        }
                    }
                ],
                    "occupationalClassifications": [
                        {
                            "classificationCode": {
                                "code": data["empstateabv"]
                            },
                            "classificationID": {
                                "code": data["empstateabv"]
                            }
                        },
                        {
                            "classificationCode": {
                                "code": data["empstateabv"]
                            },
                            "classificationID": {
                                "code": data["empstateabv"]
                            }
                        }
                    ],
                        "industryClassifications": [
                            {
                                "classificationCode": {
                                    "code": data["managementcode"],
                                    "name": ""
                                }
                            }
                        ]
            },
            "businessCommunication": {
                "mobiles": [
                    {
                        "countryDialing": "1",
                        "areaDialing": "",
                        "dialNumber": ""
                    }
                ],
                    "emails": [
                        {
                            "emailUri": data["workcontact"],
                            "notificationIndicator": False
                        }
                    ]
            },
            "laborUnion": {
                "laborUnionCode": {
                    "code": "TL1"
                }
            },
            "bargainingUnit": {
                "bargainingUnitCode": {
                    "code": "1007",
                        "name": ""
                }
            },
            "officerTypeCode": {
                "code": "EMP",
                    "name": ""
            },
            "payGradeCode": {
                "code": "GRAD1",
                    "name": ""
            },
            "benefitsEligibilityClassCode": {
                "code": "A",
                    "name": ""
            },
            "managementPositionIndicator": management,
                "workerTypeCode": {
                "code": "f",
                    "name": ""
            },
            "acaBenefitEligibilityCode": {
                "code": "D",
                    "name": "D - Designate Full Time"
            },
            "acaBenefitEligibilityDate": "",
                "homeOrganizationalUnits": [
                    {
                        "unitTypeCode": {
                            "code": "BusinessUnit",
                            "name": ""
                        },
                        "nameCode": {
                            "code": "OSLRETAIL",
                            "name": ""
                        }
                    },
                    {
                        "unitTypeCode": {
                            "code": "HomeDepartment",
                            "name": ""
                        },
                        "nameCode": {
                            "code": data["homedepartmentdescription"],
                            "name": ""
                        }
                    },
                    {
                        "unitTypeCode": {
                            "code": "HomeCostNumber",
                            "name": ""
                        },
                        "nameCode": {
                            "code": data["homecostnbrcode"],
                            "name": ""
                        }
                    }
                ]
        },
        "applicantPayrollProfile": {
            "payrollFileNumber": data["badgenumber"],
                "overtimeEligibilityIndicator": True,
                    "payrollGroupCode": data["paycodegroup"],
                        "standardHours": {
                "hoursQuantity": "",
                    "unitCode": {
                    "code": ""
                }
            },
            "tippedWorkerIndicator": False,
                "baseRemuneration": {
                "recordingBasisCode": {
                    "code": data["regularpayrate"]
                },
                "payPeriodRateAmount": {
                    "amount": rate
                },
                salary: {
                    "amount": rate
                }
            }
        },
        "applicantPersonalProfile": {
            "birthName": {
                "givenName": data["firstname"],
                    "middleName": "",
                        "familyName": data["lastname"]
            },
            "legalName": {
                "salutations": [
                    {
                        "code": "",
                        "name": data["firstname"] + " " + data["lastname"]
                    }
                ]
            },
            "preferredName": {
                "nickName":  data["nickname"] + " " + data["lastname"]
            },
            "genderCode": {
                "code": data["insurancegender"]
            },
            "birthDate": data["dob"],
                "governmentIDs": [
                    {
                        "id": encryption_manager.decrypt("{}".format(data["attr93"])),
                        "nameCode": {
                            "code": "SSN"
                        }
                    }
                ],
                    "legalAddress": {
                "lineOne": data["empaddress"],
                    "lineTwo": "",
                        "lineThree": "",
                            "cityName": data["empcity"],
                                "subdivisionCode": {
                    "code": data["empstateabv"],
                        "name": data["empstateabv"] + " - " + data["empstateprovince"],
                            "subdivisionType": ""
                },
                "countryCode": data["empcountry"],
                    "postalCode": data["emppostalzip"],
                    "subdivisionCode2": {"code": data["country"],
                        "name": "",
                        "subdivisionType": ""
                },
                "deliveryPoint": ""
            },
            "raceCode": {
                "code": data["race"],
                "identificationMethodCode": {
                  "code": data["raceid"]
                }
              },
              "ethnicityCode": {
                "code": data["ethnicity"]
            },
            "languageCode": {
                "code": "en_US",
                    "name": "en_US - English (US)"
            },
            "communication": {
                "mobiles": [
                    {
                        "countryDialing": "1",
                        "areaDialing": data["personalphone"][0:3],
                        "dialNumber": data["personalphone"][3:]
                    }    
                        ],
                    "emails": [
                        {
                            "emailUri": data["personalcontact"],
                            "notificationIndicator": True
                        }
                    ]
            }
        },
        "applicantTaxProfile": {
            "usFederalTaxInstruction": {
                "multipleJobIndicator": True,
                    "federalIncomeTaxInstruction": {
                    "taxFilingStatusCode": {
                        "code": "D"
                    },
                    "taxWithholdingStatus": {
                        "statusCode": {
                            "code": ""
                        }
                    },
                    "federalUnemploymentTaxInstruction": {
                        "taxWithholdingStatus": {
                            "statusCode": {
                                "code": "1"
                            }
                        }
                    },
                    "taxAllowanceQuantity": 0,
                        "additionalIncomeAmount": {
                        "amount": 0
                    },
                    "additionalTaxAmount": {
                        "amount": 1
                    },
                    "taxAllowances": [
                        {
                            "allowanceTypeCode": {
                                "code": "Deductions"
                            },
                            "taxAllowanceAmount": {
                                "amount": 0
                            }
                        },
                        {
                            "allowanceTypeCode": {
                                "code": "Dependents"
                            },
                            "taxAllowanceAmount": {
                                "amount": 0
                            }
                        }
                    ]
                }
            },
            "usStateTaxInstructions": {
                "stateIncomeTaxInstructions": [
                    {
                        "workedInJurisdictionIndicator": True,
                        "stateCode": {
                            "code": data["locationstate"]
                        },
                        "taxFilingStatusCode": {
                            "code": "S"
                        },
                        "taxAllowanceQuantity": 0,
                        "additionalTaxAmount": {
                            "amount": 1
                        }
                    },
                    {
                        "livedInJurisdictionIndicator": True,
                        "stateCode": {
                            "code": data["empstateabv"]
                        }
                    }
                ],
                    "suiTaxInstruction": {
                    "stateCode": {
                        "code": data["suisditax"]
                    },
                    "healthCoverageCode": {
                        "code": ""
                    }
                }
            },
            "localTaxInstructions": [
                {
                    "livedInJurisdictionIndicator": False,
                    "localCode": {
                        "code": ""
                    },
                    "taxAllowanceQuantity": 0
                },
                {
                    "workedInJurisdictionIndicator": False,
                    "localCode": {
                        "code": ""
                    }
                },
                {
                    "taxTypeCode": {
                        "code": ""
                    },
                    "localCode": {
                        "code": ""
                    }
                },
                {
                    "taxTypeCode": {
                        "code": ""
                    },
                    "localCode": {
                        "code": ""
                    }
                },
                {
                    "taxTypeCode": {
                        "code": ""
                    },
                    "localCode": {
                        "code": ""
                    }
                },
                {
                    "taxTypeCode": {
                        "code": ""
                    },
                    "localCode": {
                        "code": ""
                    }
                }]}}}
    payload = json.dumps(payload)
    headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' +token }
    cert=(clientCrt,clientKey)
    response = requests.request("POST", url, headers=headers, data=payload,cert=cert)
    status = response.status_code
    # job_logger(payload)
    if status >= 200 and status <=299:
        job_logger('API status code on creating a new ADP account :{0} \n{1}'.format(response.status_code, str(response)))
        job_logger(str(response.json()))
        key = None
        for k in dict(response.json()).keys():
            if 'confirmmessage' in k.lower():
                key = k
                break
        adpo_id = response.json()[key]['messages'][1]['resourceID']
        return adpo_id
    elif status == 500:
        adp_message = response.json()["_confirmMessage"]["messages"][0]["messageText"]
        message = 'API status code on creating a new ADP account :{0} \n{1}\n{2}'.format(response.status_code, str(response),adp_message)
        lastname = data['lastname']
        firstname = data['firstname']
        legalname,personalcontact,oslemployeeid,onboardprime = firstname + " " + lastname,data['personalcontact'],data['oslemployeeid'],data['onboardprime']
        message = "{} \n\nHi {},\n{}\n\n{}\n{}\n{}".format(message,onboardprime,'Please delete this Profile on ADP:',legalname,personalcontact,oslemployeeid)
        populate_sendgrid_log(message,status, data)
        job_logger(payload)
    elif status > 500:
        message = '{2}\nAPI status code on creating a new ADP account :{0} \n{1}'.format(response.status_code, str(response), response.text)
        lastname = data['lastname']
        firstname = data['firstname']
        legalname,personalcontact,oslemployeeid,onboardprime = firstname + " " + lastname,data['personalcontact'],data['oslemployeeid'],data['onboardprime']
        message = "{} \n\nHi {},\n{}\n\n{}\n{}\n{}".format(message,onboardprime,'Please delete this Profile on ADP:',legalname,personalcontact,oslemployeeid)
        populate_sendgrid_log(message,status, data)
        job_logger(payload)
    else:
        key = None
        for k in dict(response.json()).keys():
            if 'confirmmessage' in k.lower():
                key = k
                break
        message = response.json()[key]["messages"][0]["messageText"]
        populate_sendgrid_log(message,status, data)
        job_logger('API status code on creating a new ADP account :{0} \n{1}'.format(response.status_code, response.text))
        return None

def get_adp_details(token,adpo_id,data):
    try:
        url = "https://api.adp.com/hr/v2/workers/{0}".format(adpo_id)
        headers = {
              'Authorization': 'Bearer {}'.format(token),
              'Cookie': 'BIGipServerp_dc1_mobile_apache_sor=337592331.5377.0000; BIGipServerp_mkplproxy-dc1=1633878283.20480.0000; BIGipServerp_mkplproxy-dc2=2958951179.20480.0000; WFNCDN=wfn'
            }
        payload={}
        cert=(clientCrt,clientKey)
        response = requests.request("GET", url, headers=headers, data=payload,cert=cert)
        status = response.status_code
        if status >= 200 and status <=299:
            worker = response.json()
            job_logger(worker)
            vtoken = (worker['workers'][0]['associateOID'],worker['workers'][0]['workerID']['idValue'],data['paycodegroup']+str(data['oslemployeeid']),data['oslemployeeid'])
            job_logger('ADP details successfully returned: {}'.format(vtoken[1]))
            return vtoken
        else:
            message = response.json()["_confirmMessage"]["messages"][0]["messageText"]
            populate_sendgrid_log(message,status,data)
            job_logger('API status code on getting details on ADP:{0} \n{1}'.format(response.status_code, response.text))
    except BaseException as e:
        job_logger('error: {}'.format(e),'error')
        vtoken = (adpo_id,None,data['paycodegroup']+str(data['oslemployeeid']),data['oslemployeeid'])
        update_employee_profile(vtoken,'ADP DETAILS')
        return None

def update_employee_profile(upd_id, status):
    #job_logger('Employeeid {} status to be updated.'.format(employeeid))
    associateadpoid,associateid,positionid,employeeid = upd_id
    job_logger('Employeeid {} status updated to  {}.'.format(employeeid,status))
    stmt_pg = """update mdb.employee_profile a set status = '{}',associateadpoid = '{}',associateid = '{}',positionid='{}' where oslemployeeid = '{}'""".format(status,associateadpoid,associateid,positionid,employeeid)
    user_ = user
    password_ = password
    conn = psycopg2.connect(host=host,database=env,user=user_,password=password_)
    cur = conn.cursor()
    cur.execute(stmt_pg)
    conn.commit()
    cur.close()
    job_logger('Employeeid {} status updated to  {}.'.format(employeeid,status))
    pass

def init_adp_profile(data):
    token = generate_token()
    employeeid =  data['oslemployeeid']
    icimsid = data['icimsid']
    if token:
        adpoid = create_adp_profile(token,data)
        if adpoid:
            update = get_adp_details(token,adpoid,data)
            if update:
                update_employee_profile(update,'DATA CHANGE')
            else:
                job_logger('Failed to retrive ADP details of created profile {0}'.format(employeeid))
        else:
            job_logger('Failed to create ADP profile for employeeid {0}'.format(employeeid))
    else:
        job_logger('Failed to generate auth Token for ADP, Profile not be created.')       
    '''Create ADP profile, set status to PENDING ADP. Upon failure, 
    logging level is set and properly logged to send email out via sendgrid.'''

def init_pipeline(data):
    '''initialize onboarding pipeline, menthod needs to be called in the thread Pool'''
    for key in data.keys():
        if data[key]['status'].lower() == 'adp details':
            token = generate_token()
            update = get_adp_details(token,data[key]['associateadpoid'],data[key])
            if update:
                update_employee_profile(update,'DATA CHANGE')
        else:
            init_adp_profile(data[key])
    pass

def main():
    emp_list = get_profiles(pending_adp.format('%'))
    print(len(emp_list))
    then_=datetime.now()
    for data in emp_list:
        init_pipeline(data)
    now_=datetime.now()
    job_logger('Took {} seconds to complete tasks.'.format((now_-then_).total_seconds()))
    os.system('tail -25 ' + os.path.join(log_path,'{}_{}_{}.log'.format('backbone_pipeline',env,datetime.now().strftime('%Y%m%d'))))

if __name__ == '__main__':
    main()