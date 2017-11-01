def main(arguments):
    #Get value of parameters with . notation
    print("Printing value of parameter")
    print(arguments.foo)
    print(arguments.bar)

if __name__=="__main__":
    import argparse
    #Instantiate argument parser
    parser = argparse.ArgumentParser()
    #Add arguments
    parser.add_argument('foo')
    parser.add_argument('bar')
    #Parse arguments
    args = parser.parse_args()
    #Call main passing args object
    print(args.foo)
    #main(args)
