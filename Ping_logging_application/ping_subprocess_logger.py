# Importing subprocess related packages:
import subprocess
from subprocess import check_output

# Importing data managment packages:
import pandas as pd
from datetime import datetime

class ping_data(object):
    """
    # TODO: Add class docstring
    """
    # def __init__(self, arg):

    def build_subprocess_dataframe(rows):
        """This method performs the subprocess to test the ping towards a
        specified IP address, extracts the necessary data from the subprocess
        and creates a pandas series with said extracted data. The series is
        then appened to a main dataframe. This process is repeated a specific
        number of times according to the input numbers of rows

        Parameters
        ----------
        row : int
            The number of rows the main dataframe will contain as it dictates
            how many times the logging process will be performed

        Returns
        -------
        df_main : pandas dataframe
            The main dataframe containing the logged data
        """

        # Creating empty main dataframe:
        df_main = pd.DataFrame(columns=['Time', 'Min', 'Max', 'Avg', '% Loss'])

        # Looping the process of logging and recording ping according to method
        # input:
        for i in range(rows):

            # Calling the subprocess and storing the result as a variable:
            subprocess_str = check_output('ping 104.160.131.3').decode('utf-8')
            #print(subprocess_str)


            # Slicing strings to extact variables due to predictability instead of
            # using RegEx:
            min_ping = int(subprocess_str[418:420])
            max_ping = int(subprocess_str[434:436])
            avg_ping = int(subprocess_str[450:452])
            loss_percentage = int(subprocess_str[345:346])
            #print(min_ping, max_ping, avg_ping, loss_percentage)

            # Getting current time:
            time = datetime.now()
            #print(time)


            # Creating a pandas series with collected data:
            row = pd.Series([time, min_ping, max_ping, avg_ping, loss_percentage],
            index = ['Time', 'Min', 'Max', 'Avg', '% Loss'])

            # Appending series to dataframe as row:
            df_main = df_main.append(row, ignore_index=True)
            #print(df_main)

        # Setting the index of the dataframe to the Time column:
        df_main.set_index('Time')


# Testing:
#ping_data.build_subprocess_dataframe(10)
