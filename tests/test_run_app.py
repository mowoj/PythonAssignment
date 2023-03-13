""" 
Use the following package for Spark tests - https://github.com/MrPowers/chispa
run test with command: python3 -m pytest --import-mode=append tests/
"""

from app.config import DEFAULTVAL
from app.run_app import parse_input_args, upper_values, lower_values


class TestRunApp:
    """_summary_
    """

    def test_parse_input_args_relying_on_default_values(self):
        """
        Testing the function that is responsible for downloading initial
        arguments such as file paths or filtering conditions.

        Data and data Types of default values comparison
        """
        # parameters from function
        args = parse_input_args(["-ds1", "/src/testest", "-v", "test value"])

        # default values
        assert args.dataset2 == DEFAULTVAL["financialData"]
        assert args.column == DEFAULTVAL["colFilter"]

        # data type
        assert isinstance(args.dataset2[0], str)
        assert isinstance(args.column[0], str)
        
    def test_upper_values_data_change(self,test_data_list,test_data_upper_list):
        """
        Testing the function that is responsible for changing string values to uppercase
        """    
        data = upper_values(test_data_list)
        assert data == test_data_upper_list
    
    def test_lower_values_data_change(self,test_data_list,test_data_lower_list):
        """
        Testing the function that is responsible for changing string values to lowercase
        """    
        data = lower_values(test_data_list)
        assert data == test_data_lower_list

