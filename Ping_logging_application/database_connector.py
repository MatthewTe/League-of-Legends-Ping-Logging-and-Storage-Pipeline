# Importing data managment packages:
import pandas as pd
from ping_subprocess_logger import ping_data

# Importing database managment packages:
import MySQLdb


class db_connector(object):
    """
    This object contains the methods necessary for connecting to an external
    MySQL db and read data stored in the ping_data object to said db.

    Parameters
    ----------
    data_model : pandas dataframe
        This is the dataframe that is built according to the ping_logging data
        model. It is stored within the model as a pandas dataframe and will be
        pushed to the MySQL db as such.

    db_parms : dict
        This is a dictionary containing all the connection information for the
        online database service that is being used to read and write data. The
        structure of the db_parms dictionary is:

        {'host', 'user', 'passwd', 'db_name'}

    tbl_name : str
        This string will serve as the name for the MySQL table that all the data
        will be written to. If this table does not exist in the db it will be
        created.

    """
    def __init__(self, data_model, db_parms, tbl_name):

        # Declaring main instance variables:
        self.db_parms = db_parms
        self.data_model = data_model
        self.tbl_name = tbl_name


        # Connecting to database:
        self.db = MySQLdb.connect(host = db_parms['host'],
                                    user = db_parms['user'],
                                    passwd = db_parms['passwd'],
                                    db = db_parms['db_name'])


        # Creating cursor as instance variable:
        self.cur = self.db.cursor()


        # Checking to see if the table exists and creating it if not:
        createtbl_quiery = "CREATE TABLE IF NOT EXISTS {} (\
Time TEXT,\
Min INT,\
Max INT,\
Avg INT,\
Loss INT,\
PRIMARY KEY (Time(45)))".format(self.tbl_name)

        # Executing SQL query:
        self.cur.execute(createtbl_quiery)

    def push(self):
        """Method pushes the current data to the previously specified MySQLdb
        table. It pulls the current whole db table as a dataframe, compares it
        to the current data to ensure it is unique and then pushes the dataframe
        to the db.

        This is mostly to protect against repeated entries of the same dataframe
        """

        # Extracting and building a dataframe from all current data in the table:
        main_db_dataframe = pd.read_sql('SELECT * FROM {}'.format(self.tbl_name),
         con = self.db)

        # Merging the main SQL database to the current input data to compare:
        interim_df = pd.concat([main_db_dataframe, self.data_model],
         ignore_index=True)


        # Selecting the unique elements from interim_df
        # (should be all of self.data_model):
        main_unique_df = interim_df.drop_duplicates()


        # Adding all rows of main_unique_df to SQL table:
        for index, row in main_unique_df.iterrows():

            print(row)

            # Building SQL insert query:
            add_row_query = "INSERT INTO %s (Time, Min, Max, Avg, Loss)\
VALUES ('{}', '{}', '{}', '{}', '{}')" % (self.tbl_name)

            # Formatting query string:
            add_row_query = add_row_query.format(row['Time'],
             row['Min'], row['Max'], row['Avg'], row['% Loss'])


            # Executing SQL query:
            self.cur.execute(add_row_query)

            self.db.commit()

            self.db.close()


# Test
# TODO: Fix Erroring with MySQLdb._exceptions.IntegrityError:
