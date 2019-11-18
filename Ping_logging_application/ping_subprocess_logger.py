# Importing subprocess related packages:
import subprocess
from subprocess import check_output
import time

# Importing data managment packages:
import pandas as pd
from datetime import datetime

class ping_data(object):
    """
    This is the main object of the ping logging application and serves as the
    unifying object for all the ping logging methods. The application takes
    advantage mainly the subprocesses package to make use of the existing cmd
    function 'ping' to track and log the ping given an ip address

    Parameters
    ----------
    ip_address : str
        The ip address with which its ping will be tested

    runtime : int
        An integer that dictates how long the logging application will run
        and continue to build the dataframe in minuites.

    """
    def __init__(self, ip_address, runtime):

        # Declaring instance variables:
        self.ip_address = ip_address
        self.runtime = runtime

        # Building the data model as an instance dataframe:
        self.data = build_subprocess_dataframe()

    def build_subprocess_dataframe(self):
        """This method performs the subprocess to test the ping towards a
        specified IP address, extracts the necessary data from the subprocess
        and creates a pandas series with said extracted data. The series is
        then appened to a main dataframe. This process is repeated a specific
        number of times according to the input numbers of rows

        Returns
        -------
        df_main : pandas dataframe
            The main dataframe containing the logged data
        """

        # Creating empty main dataframe:
        df_main = pd.DataFrame(columns=['Time', 'Min', 'Max', 'Avg', '% Loss'])

        # Declaring the time variable that will be used to end the while loop:
        t_end = time.time() + 60 * self.runtime

        # Looping the process of logging and recording ping according to method
        # input:
        while time.time() < t_end:

            # Calling the subprocess and storing the result as a variable:
            subprocess_str = check_output('ping {}'.format(self.ip_address)
            ).decode('utf-8')
            #print(subprocess_str)


            # Slicing strings to extact variables due to predictability instead of
            # using RegEx:
            min_ping = int(subprocess_str[418:420])
            max_ping = int(subprocess_str[434:436])
            avg_ping = int(subprocess_str[450:452])
            loss_percentage = int(subprocess_str[345:346])
            #print(min_ping, max_ping, avg_ping, loss_percentage)

            # Getting current time:
            curr_time = datetime.now()
            #print(time)


            # Creating a pandas series with collected data:
            row = pd.Series([curr_time, min_ping, max_ping, avg_ping, loss_percentage],
            index = ['Time', 'Min', 'Max', 'Avg', '% Loss'])

            # Appending series to dataframe as row:
            df_main = df_main.append(row, ignore_index=True)
            #print(df_main)

        # Setting the index of the dataframe to the Time column:
        df_main.set_index('Time')

        return df_main

    def dataframe_to_csv(self, directory):
        '''This method exports the dataframe that is built via the
        build_subprocess_dataframe method to a specifed file directory

        Parameters
        ----------
        directory : str
            The string that dictates the path where the the .csv file will be
            written to. The directory string is written with standard path
            formatting path//path//path.
        '''

        # Exporting the dataframe to a .csv file:
        self.data.to_csv(directory)
 



# Testing:
#test = ping_data('104.160.131.3')
#print(test.build_subprocess_dataframe(3))
