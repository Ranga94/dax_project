import sys

def main(arguments):
    storage = Storage()
    result = storage.get_sql_data(sql_connection_string=arguments.connection_string,
                         sql_table_name=arguments.table_name,
                         sql_column_list=["EMAIL_USERNAME"])

    print(result)

#Hello world



if __name__=="__main__":
    import argparse
    #Instantiate argument parser
    parser = argparse.ArgumentParser()
    #Add arguments
    parser.add_argument('python_path')
    parser.add_argument('connection_string')
    parser.add_argument('table_name')
    #Parse arguments
    args = parser.parse_args()
    #We can use the arguments values now
    #Insert Python path
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    #Call main passing args object
    main(args)