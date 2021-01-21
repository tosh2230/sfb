import sys
from .estimation import Estimation

def main():
    if len(sys.argv) == 2:
        estimation = Estimation()
        response = estimation.calc(sys.argv[1])
        print(response)
    else:
        print('Please set a argument: sql_file_path')