import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from pathlib import Path

def main():

    DATADIR = str(Path(__file__).parent.parent) # for relative path 
    DIALECT = 'postgresql+psycopg2://'
    user = '' # TO BE REPLACED
    password = '' # TO BE REPLACED
    host = 'dbcourse2022.cs.aalto.fi'
    port = '5432'
    database = "grp30_vaccinedist"
    db_uri = "%s:%s@%s/%s" % (user, password, host, database)
    print(DIALECT + db_uri)
    engine = create_engine(DIALECT + db_uri)
    psql_conn = engine.connect()
    print("Data directory: ", DATADIR)

    try:
        connection = psycopg2.connect(
            database=database,
            user=user,  
            password=password,
            host=host, 
            port= port
        )

        cursor = connection.cursor()
        

        # psql_conn.execute('DROP TABLE "diagnosis";')
        # psql_conn.execute('DROP TABLE "symptom";')
        # psql_conn.execute('DROP TABLE "vaccinated";')
        # psql_conn.execute('DROP TABLE "patient";')
        # psql_conn.execute('DROP TABLE "vaccination";')
        # psql_conn.execute('DROP TABLE "vaccinationshift";')
        # psql_conn.execute('DROP TABLE "employee";')
        # psql_conn.execute('DROP TABLE "transportlog";')
        # psql_conn.execute('DROP TABLE "batch";')
        # psql_conn.execute('DROP TABLE "vaccstation";')
        # psql_conn.execute('DROP TABLE "vaccine";')
        # psql_conn.execute('DROP TABLE "manufacturer";')

        psql_conn.execute('''CREATE TABLE IF NOT EXISTS manufacturer
        (ID VARCHAR(10) PRIMARY KEY,
        phone VARCHAR(20) NOT NULL,
        vaccine VARCHAR(10) NOT NULL,
        country VARCHAR(30) NOT NULL);''')

        psql_conn.execute('CREATE TABLE IF NOT EXISTS vaccine('
        'ID VARCHAR(10) PRIMARY KEY,'
        'name VARCHAR(100) NOT NULL,'
        'doses INT NOT NULL,'
        'criticalTemp VARCHAR(20) NOT NULL);')

        psql_conn.execute('CREATE TABLE IF NOT EXISTS vaccstation('
        'name VARCHAR(30) PRIMARY KEY,'
        'address VARCHAR(100) NOT NULL,'
        'phone VARCHAR(20) UNIQUE NOT NULL);')

        psql_conn.execute('CREATE TABLE IF NOT EXISTS batch('
        'batchID VARCHAR(10) PRIMARY KEY,'
        'amount INT NOT NULL,'
        'type VARCHAR(100) NOT NULL references vaccine(ID),'
        'manufacturer VARCHAR(10) references manufacturer(ID) NOT NULL,'
        'manufDate DATE NOT NULL,'
        'expiration DATE NOT NULL,'
        'station VARCHAR(30) references vaccstation(name));')

        psql_conn.execute('CREATE TABLE IF NOT EXISTS transportlog('
        'logID INT PRIMARY KEY,'
        'batchID VARCHAR(10) references batch(batchID),'
        'arrival VARCHAR(30) references vaccstation(name),'
        'departure VARCHAR(30) references vaccstation(name),'
        'dateArr DATE NOT NULL,'
        'dateDep DATE NOT NULL);')

        psql_conn.execute('CREATE TABLE IF NOT EXISTS employee('
        'ssNo VARCHAR(15) PRIMARY KEY,'
        'name VARCHAR(50) NOT NULL,'
        'DOB DATE NOT NULL,'
        'phone VARCHAR(20) NOT NULL,'
        'role VARCHAR(15) NOT NULL,'
        'status INT CHECK (status IN (0,1)),'
        'vaccStation VARCHAR(30) references vaccstation(name));')

        psql_conn.execute("CREATE TABLE IF NOT EXISTS vaccinationshift(station VARCHAR(30) references vaccstation(name), weekday VARCHAR(30) CHECK (weekday IN ('Monday','Tuesday','Wednesday','Thursday','Friday')),worker VARCHAR(15) references employee(ssNo),PRIMARY KEY(weekday,worker));")

        psql_conn.execute('CREATE TABLE IF NOT EXISTS vaccination('
        'date DATE NOT NULL,'
        'station VARCHAR(30) references vaccstation(name),'
        'batchID VARCHAR(10) references batch(batchID),'
        'PRIMARY KEY(date, station));')

        psql_conn.execute("CREATE TABLE IF NOT EXISTS patient(ssNo VARCHAR(15) PRIMARY KEY,name VARCHAR(50) NOT NULL,DOB DATE NOT NULL,gender CHAR(1) CHECK (gender IN ('M','F')));")

        psql_conn.execute('CREATE TABLE IF NOT EXISTS vaccinated('
        'date DATE NOT NULL,'
        'station VARCHAR(30) references vaccstation(name),'
        'ssNo VARCHAR(15) references patient(ssNo),'
        'PRIMARY KEY (date,ssNo));')

        psql_conn.execute('CREATE TABLE IF NOT EXISTS symptom('
        'name VARCHAR(50) PRIMARY KEY,'
        'criticality INT CHECK (criticality IN (0,1)));')

        psql_conn.execute("CREATE TABLE IF NOT EXISTS diagnosis(ssNo VARCHAR(15) references patient(ssNo),symptom VARCHAR(30) references symptom(name),date VARCHAR(50) NOT NULL CHECK(date != '2020-02-29'),PRIMARY KEY(ssNo, symptom, date));")

        
        df_man = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx', sheet_name = "Manufacturer")
        df_man.rename(columns = {'ID':'id'},inplace = True)
        df_man.to_sql('manufacturer', con = psql_conn, if_exists = 'append', index = False)
        df_vacc = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx',sheet_name = "VaccineType")
        df_vacc['criticaltemp'] = df_vacc['tempMin'].astype(str) + ':' + df_vacc['tempMax'].astype(str)
        df_vacc.drop(['tempMin','tempMax'], axis = 1, inplace = True)
        df_vacc.rename(columns = {'ID':'id'},inplace = True)
        df_vacc.to_sql('vaccine', con=psql_conn, if_exists = 'append', index = False)
        df_vacStat = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx', sheet_name = "VaccinationStations")
        df_vacStat.to_sql('vaccstation', con = psql_conn, if_exists = 'append', index = False)
        df_bat = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx', sheet_name = "VaccineBatch")
        df_bat.rename(columns = {'batchID':'batchid','manufDate':'manufdate','location':'station'}, inplace = True)
        df_bat.to_sql('batch', con = psql_conn, if_exists = 'append', index = False)
        df_tranLog = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx',sheet_name = "Transportation log")
        df_tranLog['logid'] = df_tranLog['batchID'].index
        df_tranLog.rename(columns = {'batchID':'batchid','dateArr':'datearr','dateDep':'datedep','departure ':'departure'}, inplace = True)
        df_tranLog.to_sql('transportlog', con=psql_conn, if_exists = 'append', index = False)
        df_emp = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx',sheet_name = "StaffMembers")
        df_emp.rename(columns = {'social security number':'ssno','date of birth':'dob','vaccination status':'status','hospital':'vaccstation'}, inplace = True)
        df_emp.to_sql('employee', con = psql_conn, if_exists = 'append', index = False)
        df_sht = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx',sheet_name = "Shifts")
        df_sht.to_sql('vaccinationshift', con = psql_conn, if_exists='append', index = False)
        df_vcn = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx',sheet_name = "Vaccinations")
        df_vcn.rename(columns = {'location ':'station','batchID':'batchid'}, inplace = True)
        df_vcn.to_sql('vaccination', con=psql_conn, if_exists = 'append', index = False)
        df_pt = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx', sheet_name = "Patients")
        df_pt.rename(columns = {'date of birth':'dob','ssNo':'ssno'}, inplace = True)
        df_pt.to_sql('patient', con = psql_conn, if_exists='append', index = False)
        df_vcn = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx', sheet_name = "VaccinePatients")
        df_vcn.rename(columns = {'location':'station','patientSsNo':'ssno'}, inplace = True)
        df_vcn.to_sql('vaccinated', con = psql_conn, if_exists = 'append', index = False)
        df_smp = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx', sheet_name = "Symptoms")
        df_smp.to_sql('symptom', con = psql_conn, if_exists = 'append', index = False)
        
        df_diag = pd.read_excel(DATADIR + '/data/vaccine-distribution-data.xlsx', sheet_name = "Diagnosis")
        df_diag.rename(columns = {'patient':'ssno'}, inplace = True)
        df_diag.to_sql('diagnosis', con = psql_conn, if_exists = 'append', index = False)
        psql_conn.execute("DELETE FROM diagnosis WHERE date = '2021-02-29' OR date = '44237';")
        psql_conn.execute("UPDATE diagnosis SET date = TO_DATE(date,'YYYY-MM-DD')")

        
        # Some queries

        # Staff members working in a vaccination on May 10, 2021
        print(pd.read_sql_query("SELECT ssno, name, phone, role, status, vaccstation FROM employee JOIN vaccinationshift ON employee.ssno = vaccinationshift.worker JOIN vaccination ON vaccinationshift.station = vaccination.station WHERE vaccinationshift.weekday ='{}' AND vaccination.date = '2021-05-10';".format(pd.read_sql_query("SELECT to_char(date '2021-05-10', 'Day');", psql_conn)['to_char'].iloc[0].strip()), psql_conn))

        # Doctors available on Wednesdays in Helsinki
        q2 = pd.read_sql_query("SELECT DISTINCT(employee.name), vaccstation.address FROM employee JOIN vaccinationshift ON employee.ssno = vaccinationshift.worker JOIN vaccstation ON employee.vaccstation = vaccstation.name WHERE vaccinationshift.weekday = 'Monday';", psql_conn)
        print(q2[q2['address'].str.endswith('HELSINKI')]['name'])

        # Vaccination batches
        print(pd.read_sql_query("SELECT G.batchid, batch.station, transportlog.arrival FROM transportlog JOIN batch ON transportlog.batchid = batch.batchid, (SELECT DISTINCT ON (batchid) batchid, logid FROM transportlog ORDER BY batchid DESC, logID DESC) AS G WHERE G.logid = transportlog.logid UNION SELECT batchid, station, NULL as arrival FROM batch WHERE batchid NOT IN (SELECT batchid FROM transportlog) ORDER BY batchid;", psql_conn))
        print(pd.read_sql_query("SELECT Q3.batchid, vaccstation.phone FROM vaccstation, (SELECT G.batchid, batch.station, transportlog.arrival FROM transportlog JOIN batch ON transportlog.batchid = batch.batchid, (SELECT DISTINCT ON (batchid) batchid, logid FROM transportlog ORDER BY batchid DESC, logID DESC) AS G WHERE G.logid = transportlog.logid) AS Q3 WHERE Q3.station = vaccstation.name AND Q3.arrival != Q3.station", psql_conn))

        # Patients with critical symptoms diagnosed later than May 10, 2021
        print(pd.read_sql_query("SELECT G.ssno, G.symptom, G.date, G.station, vaccination.batchid, batch.type FROM batch JOIN vaccination ON batch.batchid = vaccination.batchid JOIN (SELECT patient.ssno, diagnosis.symptom, vaccinated.station, vaccinated.date FROM patient JOIN diagnosis ON patient.ssno = diagnosis.ssno JOIN symptom ON diagnosis.symptom = symptom.name JOIN vaccinated ON patient.ssno = vaccinated.ssno WHERE symptom.criticality = 1) AS G ON G.date = vaccination.date AND G.station = vaccination.station;", psql_conn))
        
        # Vaccination status of patients
        pd.read_sql_query('''CREATE VIEW patientstatus AS SELECT patient.ssNo,patient.name,DOB,gender,'0' AS vaccinationStatus FROM patient WHERE patient.ssNo NOT IN (SELECT ssNo FROm vaccinated)
                             UNION
                             SELECT A1.ssNo,patient.name,DOB,gender,vaccinationStatus FROM patient, (SELECT ssNo,COUNT(vaccinated.ssNo)-1 AS vaccinationStatus FROM vaccinated GROUP BY ssNo) AS A1 WHERE A1.ssNo = patient.ssNo''', psql_conn)
        print(pd.read_sql_query("SELECT * FROM patientstatus", psql_conn))

        # Vaccines stored in each hospital
        print(pd.read_sql_query("SELECT batch.station, batch.type,SUM(batch.amount) FROM batch  GROUP BY batch.station,batch.type ORDER BY batch.station", psql_conn))
        print(pd.read_sql_query("SELECT batch.station, SUM(batch.amount) AS Total_number FROM batch  GROUP BY batch.station ORDER BY batch.station", psql_conn))

        # Average frequency of different symptoms diagnosed
        print(pd.read_sql_query("SELECT A.type, C.symptom, COUNT(C.symptom) FROM (SELECT vaccination.date,vaccination.station,batch.type FROM vaccination, batch WHERE batch.batchID=vaccination.batchID) AS A, vaccinated AS B, diagnosis AS C WHERE C.ssNo=B.ssNo AND A.date = B.date And A.station = B.station AND C.date::date>B.date::date GROUP BY A.type, C.symptom", psql_conn))

    
        # Further data analysis

        # Dataframe for patients and symptoms
        q1 = """SELECT patient.ssNo, patient.gender, patient.DOB, diagnosis.symptom, diagnosis.date 
                FROM patient, diagnosis 
                WHERE diagnosis.ssNo = patient.ssNo"""
        data1 = pd.read_sql_query(q1, psql_conn)
        data1.to_sql('patientsymptoms', con = psql_conn, if_exists = 'replace', index = True)
        print(pd.read_sql_query('SELECT * FROM PatientSymptoms', psql_conn))

        # Dataframe for patients and vaccines
        q2 = """SELECT A.ssNo AS patientssNO, A.date AS date1, A.type AS vaccinetype1, B.date AS date2, B.type AS vaccinetype2
                FROM (select vaccinated.ssNO, vaccinated.date, batch.type from vaccinated, vaccination, batch WHERE batch.batchID = vaccination.batchID AND vaccinated.date = vaccination.date AND vaccinated.station = vaccination.station) AS A
                LEFT JOIN (select vaccinated.ssNO, vaccinated.date, batch.type from vaccinated, vaccination, batch WHERE batch.batchID = vaccination.batchID AND vaccinated.date = vaccination.date AND vaccinated.station = vaccination.station) AS B ON (A.ssNo = B.ssNo AND A.date != B.date)
                WHERE A.date < B.date OR B.date is null"""
        data2 = pd.read_sql_query(q2,psql_conn)
        data2.to_sql('patientvaccineinfo', psql_conn, if_exists = 'replace', index = True)
        print(pd.read_sql_query('SELECT * FROM PatientVaccineInfo', psql_conn))

        # Most common symptoms for males and females
        data3_male = data1[data1['gender'] == 'M']
        data3_female = data1[data1['gender'] == 'F']
        print(data3_male)
        print(data3_female)

        df3_answer1_male = data3_male.groupby(['symptom'])['symptom'].count().reset_index(name = 'Count').sort_values(['Count'], ascending = False)
        df3_answer1_female = data3_female.groupby(['symptom'])['symptom'].count().reset_index(name = 'Count').sort_values(['Count'], ascending = False)

        print("Male Common Symptoms: ")
        print(df3_answer1_male[0:3])
        print("Female Common Symptoms: ")
        print(df3_answer1_female[0:3])
                         
        # Dataframe with age group
        data0_9 = pd.read_sql_query("SELECT * FROM (SELECT ssno, name, dob, date_part('year', age('2022-05-28', dob))::int AS age FROM patient) AS A1 WHERE age < 10",psql_conn).assign(ageGroup = '0-9')
        data10_19 = pd.read_sql_query("SELECT * FROM (SELECT ssno, name, dob, date_part('year', age('2022-05-28', dob))::int AS age FROM patient) AS A1 WHERE age < 20 AND age > 9", psql_conn).assign(ageGroup = '10-19')
        data20_39 = pd.read_sql_query("SELECT * FROM (SELECT ssno, name, dob, date_part('year', age('2022-05-28', dob))::int AS age FROM patient) AS A1 WHERE age < 40 AND age > 19", psql_conn).assign(ageGroup = '20-39')
        data40_59 = pd.read_sql_query("SELECT * FROM (SELECT ssno, name, dob, date_part('year', age('2022-05-28', dob))::int AS age FROM patient) AS A1 WHERE age < 60 AND age > 39", psql_conn).assign(ageGroup = '40-59')
        data60 = pd.read_sql_query("SELECT * FROM (SELECT ssno, name, dob, date_part('year', age('2022-05-28', dob))::int AS age FROM patient) AS A1 WHERE age > 59", psql_conn).assign(ageGroup = '60+')
        dataframeQ_4 = data0_9.append([data10_19, data20_39, data40_59, data60])
        print(dataframeQ_4)

        # Dataframe with age group and vaccination status
        vc_1or2 = pd.read_sql_query("""SELECT A.ssNo, name, dob, age, status FROM 
                                       (SELECT ssno, name, dob, date_part('year', age('2022-05-28', dob))::int AS age FROM patient) AS A, 
                                       (SELECT patient.ssNo, COUNT(*) AS status FROM patient, vaccinated 
                                       WHERE patient.ssNo = vaccinated.ssNo GROUP BY patient.ssNo) AS B  
                                       WHERE A.ssNo = B.ssNo""", psql_conn)
        no_vc = pd.read_sql_query("""SELECT  DISTINCT A.ssNo, name, dob, age FROM 
                                     (SELECT ssno, name, dob, date_part('year', age('2022-05-28', dob))::int AS age FROM patient) AS A 
                                     WHERE A.ssNo NOT IN 
                                     (SELECT ssNo FROM (SELECT patient.ssNo, COUNT(*) AS status FROM patient, vaccinated 
                                     WHERE patient.ssNo = vaccinated.ssNo GROUP BY patient.ssNo) AS B)""", psql_conn).assign(status = '0')
        dataframeQ_5 = pd.merge(dataframeQ_4, vc_1or2.append(no_vc))
        print(dataframeQ_5)

        # Vaccine doses
        row0_9_0 = dataframeQ_5.loc[(dataframeQ_5['status'] == '0') & (dataframeQ_5['ageGroup'] == '0-9')]
        row0_9_1 = dataframeQ_5.loc[(dataframeQ_5['status'] == 1) & (dataframeQ_5['ageGroup'] == '0-9')]
        row0_9_2 = dataframeQ_5.loc[(dataframeQ_5['status'] == 2) & (dataframeQ_5['ageGroup'] == '0-9')]
        row10_19_0 = dataframeQ_5.loc[(dataframeQ_5['status'] == '0') & (dataframeQ_5['ageGroup'] == '10-19')]
        row10_19_1 = dataframeQ_5.loc[(dataframeQ_5['status'] == 1) & (dataframeQ_5['ageGroup'] == '10-19')]
        row10_19_2 = dataframeQ_5.loc[(dataframeQ_5['status'] == 2) & (dataframeQ_5['ageGroup'] == '10-19')]
        row20_39_0 = dataframeQ_5.loc[(dataframeQ_5['status'] == '0') & (dataframeQ_5['ageGroup'] == '20-39')]
        row20_39_1 = dataframeQ_5.loc[(dataframeQ_5['status'] == 1) & (dataframeQ_5['ageGroup'] == '20-39')]
        row20_39_2 = dataframeQ_5.loc[(dataframeQ_5['status'] == 2) & (dataframeQ_5['ageGroup'] == '20-39')]
        row40_59_0 = dataframeQ_5.loc[(dataframeQ_5['status'] == '0') & (dataframeQ_5['ageGroup'] == '40-59')]
        row40_59_1 = dataframeQ_5.loc[(dataframeQ_5['status'] == 1) & (dataframeQ_5['ageGroup'] == '40-59')]
        row40_59_2 = dataframeQ_5.loc[(dataframeQ_5['status'] == 2) & (dataframeQ_5['ageGroup'] == '40-59')]
        percentageO_9_0 = round((len(row0_9_0) / len(data0_9)) * 100, 2)
        percentageO_9_1 = round((len(row0_9_1) / len(data0_9)) * 100, 2)
        percentageO_9_2 = round((len(row0_9_2) / len(data0_9)) * 100, 2)
        percentage1O_19_0 = round((len(row10_19_0) / len(data10_19)) * 100, 2)
        percentage1O_19_1 = round((len(row10_19_1) / len(data10_19)) * 100, 2)
        percentage1O_19_2 = round((len(row10_19_2) / len(data10_19)) * 100, 2)
        percentage2O_39_0 = round((len(row20_39_0) / len(data20_39)) * 100, 2)
        percentage2O_39_1 = round((len(row20_39_1) / len(data20_39)) * 100, 2)
        percentage2O_39_2 = round((len(row20_39_2) / len(data20_39)) * 100, 2)
        percentage4O_59_0 = round((len(row40_59_0) / len(data40_59)) * 100, 2)
        percentage4O_59_1 = round((len(row40_59_1) / len(data40_59)) * 100, 2)
        percentage4O_59_2 = round((len(row40_59_2) / len(data40_59)) * 100, 2)
        data = [[percentageO_9_0, percentage1O_19_0, percentage2O_39_0, percentage4O_59_0, 0], [percentageO_9_1, percentage1O_19_1, percentage2O_39_1, percentage4O_59_1, 0],[percentageO_9_2, percentage1O_19_2, percentage2O_39_2, percentage4O_59_2, 0]]
        dataframeQ_6 = pd.DataFrame(data, columns = ['0-9', '10-19', '20-39', '40-59', '60+'])
        print(dataframeQ_6)
   
        # Dataframe for symptoms with relative frequency
        df_1 = pd.read_sql_query("""SELECT A.type, C.symptom AS name, COUNT(C.symptom) FROM 
                                    (SELECT vaccination.date, vaccination.station, batch.type FROM vaccination, batch 
                                    WHERE batch.batchID = vaccination.batchID) AS A, vaccinated AS B, diagnosis AS C 
                                    WHERE C.ssNo = B.ssNo AND A.date = B.date And A.station = B.station AND C.date::date > B.date::date 
                                    GROUP BY A.type, C.symptom""", psql_conn)
        symp = pd.read_sql_query('SELECT * FROM symptom', psql_conn)
        df_2 = pd.read_sql_query("""SELECT type, COUNT(*) FROM vaccinated 
                                    JOIN vaccination ON vaccinated.date = vaccination.date AND vaccinated.station = vaccination.station 
                                    JOIN batch ON vaccination.batchID = batch.batchID GROUP BY type""", psql_conn)
        v01 = df_1[df_1['type'] == 'V01']
        v01['count'] = v01['count'] / df_2.iat[0, 1]
        v01.rename(columns = {'count':'V011'}, inplace = True)
        v01.drop(['type'], axis = 1, inplace = True)
        v01 = pd.merge(symp, v01, how = 'left')
        v02 = df_1[df_1['type'] == 'V02']
        v02['count'] = v02['count'] / df_2.iat[1, 1]
        v02.rename(columns = {'count':'V022'}, inplace = True)
        v02.drop(['type'], axis = 1, inplace = True)
        v02 = pd.merge(symp, v02, how = 'left')
        v03 = df_1[df_1['type'] == 'V03']
        v03['count'] = v03['count'] / df_2.iat[2, 1]
        v03.rename(columns = {'count':'V033'}, inplace = True)
        v03.drop(['type'], axis = 1, inplace = True)
        v03 = pd.merge(symp, v03, how = 'left')
        res = pd.merge(pd.merge(v01, v02), v03)
        res['V011'] = res['V011'].fillna(0)
        res['V022'] = res['V022'].fillna(0)
        res['V033'] = res['V033'].fillna(0)
        multi_labels = ['-', 'rare', 'common', 'very common']
        multi_cut_bins = [-0.1, 0, 0.05, 0.1, res['V011'].max()]
        categorized = pd.cut(res['V011'], bins = multi_cut_bins, labels = multi_labels, include_lowest = True)
        res.insert(3, 'V01', categorized)
        multi_cut_bins = [-0.1, 0, 0.05, 0.1, res['V022'].max()]
        categorized = pd.cut(res['V022'], bins = multi_cut_bins, labels = multi_labels, include_lowest = True)
        res.insert(5, 'V02', categorized)
        multi_cut_bins = [-0.1, 0, 0.05, 0.1, res['V033'].max()]
        categorized = pd.cut(res['V033'], bins = multi_cut_bins, labels = multi_labels, include_lowest = True)
        res.insert(7, 'V03', categorized)
        res.drop(['V011', 'V022', 'V033'], axis = 1, inplace = True)
        print(res)

        # Estimating the amount of vaccines that should be reserved for each vaccination
        q0 = pd.read_sql_query("SELECT * FROM Vaccination", psql_conn)
        q1 = pd.read_sql_query("SELECT (count(vaccinated.ssno)/batch.amount::FLOAT) AS Reserved FROM Vaccination JOIN Batch ON Vaccination.batchID = batch.batchID JOIN Vaccinated ON Vaccinated.date = Vaccination.date AND Vaccinated.station = Vaccination.station GROUP BY Vaccination.date, Vaccination.station, Batch.amount", psql_conn)
        q3 = q1
        q3 = q3-q1.mean(axis = 0)
        q1 = (q1 + (q3 * q3) / len(q3.index)) * 100
        q2 = pd.concat([q0,q1], axis = 1, join = 'inner')
        print(q2)

        # Total number of vaccinated patients with respect to date
        res = pd.read_sql_query('SELECT date, COUNT(*) FROM vaccinated GROUP BY date ORDER BY date', psql_conn)
        res['total'] = res['count'].cumsum()
        res = res.drop('count',1)
        res.set_index('date',inplace= True)
        res['total'].plot()
        plt.xlabel('Date')
        plt.ylabel('Number of patients vaccinated')
        plt.show()
        print(res)
        
        # Patients and staff members that nurse with ssNo ”19740919-7140” may have met in the past 10 days
        pat = pd.read_sql_query("""SELECT patient.ssNo, name FROM patient 
                                   JOIN vaccinated ON patient.ssNo = vaccinated.ssNo 
                                   WHERE date >= '2021-05-05' AND date <= '2021-05-15' AND station IN 
                                   (SELECT station FROM vaccinationshift WHERE worker = '19740919-7140')""", psql_conn)
        sta = pd.read_sql_query("""SELECT DISTINCT employee.ssNo, name FROM employee 
                                   JOIN vaccinationshift ON employee.ssNo = vaccinationshift.worker
                                   WHERE station IN
                                   (SELECT station FROM vaccinationshift WHERE worker = '19740919-7140') AND weekday IN
                                   (SELECT weekday FROM vaccinationshift WHERE worker = '19740919-7140')""", psql_conn)
        res = pd.concat([pat, sta])
        print(res)


    except Exception as e:
            print ("FAILED due to:" + str(e))
    finally:
        if (connection):
            psql_conn.close()
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

main()