""" 
Use the following package for Spark tests - https://github.com/MrPowers/chispa
run test with command: python3 -m pytest --import-mode=append tests/
"""
from chispa import assert_df_equality

from app.data import AppData 

class TestData:
   """
   Tests for functions from AppData class from data.py file
   """
   def test_filter_data_isin_filter_on_col2(self, joined_datasets, source_data_filtered_by_col2 ):
       """
       Testing the function that is responsible for filtering data in dataset
       """
       result_df, column, values = source_data_filtered_by_col2
       df = AppData.filter_data_isin(joined_datasets,column, values) 
       
       assert_df_equality(df, result_df,ignore_row_order=True) 
       
   def test_change_col_name(self, joined_datasets, rename_datasets):
       """
       rename the column names according to the list
       """
       result_df, new_column = rename_datasets
       print(result_df)
       print(new_column.items())
       df = AppData.change_col_name(joined_datasets, new_column.items()) 
       print(df.show())
       assert_df_equality(df, result_df,
                          ignore_column_order=True, ignore_row_order=True) 
       
