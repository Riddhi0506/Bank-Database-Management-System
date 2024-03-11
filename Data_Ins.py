from logging import exception
import pandas as pd
import cx_Oracle
import csv


def dataIns(A,conn):
    if(A == 1):
        # Establishing Connection
        # cx_Oracle.init_oracle_client(lib_dir = "/Users/rajatsingh/opt/anaconda3/lib/python3.9/site-packages/sqlalchemy/dialects/oracle/instantclient_19_8")
        # conn = cx_Oracle.connect('rxb8700/Themintoo1996@acaddbprod.uta.edu:1523/pcse1p.data.uta.edu')
        # print(conn.version)

        curr = conn.cursor()


        curr.execute('DROP TABLE CUST_ACCOUNT')
        curr.execute('DROP TABLE CUST_LOAN')
        curr.execute('DROP TABLE CHECKINGS')
        curr.execute('DROP TABLE SAVINGS')
        curr.execute('DROP TABLE LOAN_PAYMENT')
        curr.execute('DROP TABLE CUSTOMER')
        curr.execute('DROP TABLE LOAN')
        curr.execute('DROP TABLE ACCOUNT')
        curr.execute('DROP TABLE DEPENDENT')
        curr.execute('DROP TABLE EMPLOYEE')
        curr.execute('DROP TABLE BRANCH')

        print("All tables are deleted")

        curr = conn.cursor()


        # ------------------Branch Table creation----------------------
        curr.execute('''
        CREATE TABLE BRANCH (BRANCH_NAME VARCHAR(30), 
        ASSETS VARCHAR(30), 
        CITY VARCHAR(20), 
        PRIMARY KEY(BRANCH_NAME)
        )
        ''')
        
        print("Branch table created successfully")
        with open("Branch.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                BRANCH_NAME = i[0]
                ASSETS = i[1]
                CITY = i[2]
                # try:
                sql = '''INSERT INTO BRANCH (BRANCH_NAME,ASSETS,CITY) VALUES (:a,:b,:c)'''
                curr.execute(sql,(BRANCH_NAME,ASSETS,CITY))
                # except exception as err:
                #     print("Error while inserting,err")
                # else:
                #     print("Successfully inserted")

        conn.commit()

        curr = conn.cursor()



        #---------------- Employee table creation---------------
        curr.execute('''
        CREATE TABLE EMPLOYEE ( 
        E_SSN VARCHAR(12), 
        E_TELNO INT, 
        E_NAME VARCHAR(20), 
        E_STDATE VARCHAR(20), 
        E_LENGTH INT, 
        MGR_SSN VARCHAR(12), 
        BRANCH_NAME VARCHAR(30),
        E_TYPE VARCHAR(30),
        PRIMARY KEY(E_SSN), 
        FOREIGN KEY(BRANCH_NAME) REFERENCES BRANCH(BRANCH_NAME)
        )
        ''')

        print("Employee table created successfully")

        with open("Employee.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                E_SSN = i[0]
                E_TELNO = i[1]
                E_NAME = i[2]
                E_STDATE = i[3]
                E_LENGTH = i[4]
                MGR_SSN = i[5]
                BRANCH_NAME = i[6]
                E_TYPE = i[7]

                sql = '''INSERT INTO EMPLOYEE (E_SSN,E_TELNO,E_NAME,E_STDATE,E_LENGTH,MGR_SSN,BRANCH_NAME,E_TYPE) VALUES (:a,:b,:c,:d,:e,:f,:g,:h)'''
                curr.execute(sql,(E_SSN,E_TELNO,E_NAME,E_STDATE,E_LENGTH,MGR_SSN,BRANCH_NAME,E_TYPE))

        conn.commit()

        curr = conn.cursor()

        # ------------------Dependent table creation---------------------
        curr.execute('''
        CREATE TABLE DEPENDENT ( 
        D_NAME VARCHAR(20), 
        E_SSN VARCHAR(12), 
        PRIMARY KEY(D_NAME), 
        FOREIGN KEY (E_SSN) REFERENCES EMPLOYEE(E_SSN)
        )
        ''')

        print("Dependent table created successfully")

        with open("Dependent.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                D_NAME = i[0]
                E_SSN = i[1]

                sql = '''INSERT INTO DEPENDENT (D_NAME,E_SSN) VALUES (:a,:b)'''
                curr.execute(sql,(D_NAME,E_SSN))

        conn.commit()

        curr = conn.cursor()

        # ----------------Account Table creation----------
        curr.execute('''
        CREATE TABLE ACCOUNT(
        ACC_NO INT, 
        ACC_BAL FLOAT, 
        TRANS_DATE VARCHAR(10), 
        ACCOUNT_TYPE VARCHAR(20), 
        BRANCH_NAME VARCHAR(30), 
        PRIMARY KEY(ACC_NO), 
        FOREIGN KEY (BRANCH_NAME) REFERENCES BRANCH(BRANCH_NAME)
        )
        ''')
        print("Account table is created successfully")

        with open("Account.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                ACC_NO = i[0]
                ACC_BAL = i[1]
                TRANS_DATE = i[2]
                ACCOUNT_TYPE = i[3]
                BRANCH_NAME = i[4]

                sql = '''INSERT INTO ACCOUNT (ACC_NO,ACC_BAL,TRANS_DATE,ACCOUNT_TYPE,BRANCH_NAME) VALUES (:a,:b,:c,:d,:e)'''
                curr.execute(sql,(ACC_NO,ACC_BAL,TRANS_DATE,ACCOUNT_TYPE,BRANCH_NAME))

        conn.commit()

        curr = conn.cursor()

        # -----------------Loan Table creation---------------
        curr.execute('''
        CREATE TABLE LOAN( 
        LOAN_NO INT, 
        LOAN_AMT FLOAT, 
        BRANCH_NAME VARCHAR(20), 
        PRIMARY KEY (LOAN_NO), 
        FOREIGN KEY (BRANCH_NAME) REFERENCES BRANCH(BRANCH_NAME)
        )
        ''')
        print("Loan Table created successfully")

        with open("Loan.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                LOAN_NO = i[0]
                LOAN_AMT = i[1]
                BRANCH_NAME = i[2]

                sql = '''INSERT INTO LOAN (LOAN_NO,LOAN_AMT,BRANCH_NAME) VALUES (:a,:b,:c)'''
                curr.execute(sql,(LOAN_NO,LOAN_AMT,BRANCH_NAME))


        conn.commit()

        curr = conn.cursor()

        #----------------------Customer Table creation---------------
        curr.execute('''
        CREATE TABLE CUSTOMER ( 
        C_SSN VARCHAR(12), 
        C_NAME VARCHAR(20), 
        C_STREET VARCHAR(20), 
        C_CITY VARCHAR(20), 
        E_SSN VARCHAR(12),
        PRIMARY KEY (C_SSN),
        FOREIGN KEY (E_SSN) REFERENCES EMPLOYEE(E_SSN)
        )
        ''')

        print("Customer table created successfully")

        with open("Customer.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                C_SSN = i[0]
                C_NAME = i[1]
                C_STREET = i[2]
                C_CITY = i[3]
                E_SSN = i[4]

                sql = '''INSERT INTO CUSTOMER (C_SSN,C_NAME,C_STREET,C_CITY,E_SSN) VALUES (:a,:b,:c,:d,:e)'''
                curr.execute(sql,(C_SSN,C_NAME,C_STREET,C_CITY,E_SSN))


        conn.commit()

        curr = conn.cursor()

        # ----------------Loan Payment Table creation--------------
        curr.execute('''
        CREATE TABLE LOAN_PAYMENT( 
        PAYMENT_NO INT, 
        AMOUNT FLOAT, 
        PAYMENT_DATE VARCHAR(10), 
        LOAN_NO INT, 
        LOAN_PAY_NO VARCHAR(6), 
        PRIMARY KEY(PAYMENT_NO), 
        FOREIGN KEY (LOAN_NO) REFERENCES LOAN(LOAN_NO)
        )
        ''')
        print("Loan_Payment table created successfully")
        with open("LOAN_PAYMENT_NO.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                PAYMENT_NO = i[0]
                AMOUNT = i[1]
                PAYMENT_DATE = i[2]
                LOAN_NO = i[3]
                LOAN_PAY_NO = i[4]

                sql = '''INSERT INTO LOAN_PAYMENT (PAYMENT_NO,AMOUNT,PAYMENT_DATE,LOAN_NO,LOAN_PAY_NO) VALUES (:a,:b,:c,:d,:e)'''
                curr.execute(sql,(PAYMENT_NO,AMOUNT,PAYMENT_DATE,LOAN_NO,LOAN_PAY_NO))

        conn.commit()

        curr = conn.cursor()

        # ---------------------Savings Table creation--------------
        curr.execute('''
        CREATE TABLE SAVINGS( 
        SV_DEPO FLOAT, 
        SV_WITH FLOAT, 
        INT_RATE FLOAT, 
        ACC_NO INT, 
        FOREIGN KEY (ACC_NO) REFERENCES ACCOUNT(ACC_NO)
        )
        ''')

        print("Savings table created successfully")

        with open("Savings.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                SV_DEPO = i[0]
                SV_WITH = i[1]
                INT_RATE = i[2]
                ACC_NO = i[3]

                sql = '''INSERT INTO SAVINGS (SV_DEPO,SV_WITH,INT_RATE,ACC_NO) VALUES (:a,:b,:c,:d)'''
                curr.execute(sql,(SV_DEPO,SV_WITH,INT_RATE,ACC_NO))

        conn.commit()

        curr = conn.cursor()

        # ------------------Checkings table creation-----------------
        curr.execute('''
        CREATE TABLE CHECKINGS( 
        CK_DEPO FLOAT, 
        CK_WITH FLOAT, 
        OVERDRAFTS FLOAT, 
        ACC_NO INT, 
        FOREIGN KEY (ACC_NO) REFERENCES ACCOUNT(ACC_NO)
        )
        ''')
        print("Checkings table created successfully")
        with open("Checkings.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                CK_DEPO = i[0]
                CK_WITH = i[1]
                OVERDRAFTS = i[2]
                ACC_NO = i[3]

                sql = '''INSERT INTO CHECKINGS (CK_DEPO,CK_WITH,OVERDRAFTS,ACC_NO) VALUES (:a,:b,:c,:d)'''
                curr.execute(sql,(CK_DEPO,CK_WITH,OVERDRAFTS,ACC_NO))
        conn.commit()

        curr = conn.cursor()


        # ------------------CUST_LOAN table creation---------------
        curr.execute('''
        CREATE TABLE CUST_LOAN( 
        C_SSN VARCHAR(12), 
        LOAN_NO INT,
        FOREIGN KEY (C_SSN) REFERENCES CUSTOMER(C_SSN),
        FOREIGN KEY (LOAN_NO) REFERENCES LOAN(LOAN_NO)
        )
        ''')

        print("Cust_Loan table created successfully")

        with open("cust_loan.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                C_SSN = i[0]
                LOAN_NO = i[1]

                sql = '''INSERT INTO CUST_LOAN (C_SSN,LOAN_NO) VALUES (:a,:b)'''
                curr.execute(sql,(C_SSN,LOAN_NO))
        conn.commit()

        curr = conn.cursor()



        # --------------------CUST_ACCOUNT table creation---------------
        curr.execute('''
        CREATE TABLE CUST_ACCOUNT( 
        C_SSN VARCHAR(12), 
        ACC_NO INT,
        FOREIGN KEY (C_SSN) REFERENCES CUSTOMER(C_SSN),
        FOREIGN KEY (ACC_NO) REFERENCES ACCOUNT(ACC_NO)
        )
        ''')
        print("Cust_Account table created successfully")
        with open("cust_account.csv","r") as csv_file:
            csv_reader = csv.reader(csv_file,delimiter = ",")
            for i in csv_reader:
                C_SSN = i[0]
                ACC_NO = i[1]

                sql = '''INSERT INTO CUST_ACCOUNT(C_SSN,ACC_NO) VALUES (:a,:b)'''
                curr.execute(sql,(C_SSN,ACC_NO))
        conn.commit()

        curr = conn.cursor()



        print("All tables are created and data is loaded successfully")
        conn.commit()

        # curr = conn.cursor()
        # curr.execute('''
        # SELECT * FROM CUST_ACCOUNT
        # '''
        # )
        # conn.commit()
        # for row in curr:
        #     print(row)

