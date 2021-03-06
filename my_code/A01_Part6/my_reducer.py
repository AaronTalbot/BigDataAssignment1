#!/usr/bin/python
# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import sys
import codecs

def process_line(line):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the letter from the line
    letter = None

    # 1.2. We ouptut the num_words from the line
    num_words = None

    # 1.3. We ouptut the total_length_words from the line
    total_length_words = None

    # 2. We assign the variables
    content = line.strip().split("\t")
    aux_content = content[1].strip("()").split(" @ ")
    new = []
    for i in range(0,len(aux_content),4):
        new.append(aux_content[i:i+4])
    print(new)
    # composite_list = [aux_content[x:x+4] for x in range(len(aux_content),4)]
    # print(composite_list)
    # print(len(aux_content))
    # print(aux_content)
    return new

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    previous_line = []
    Array = []
    for line in my_input_stream:
        new_file = process_line(line)
        for parameters in new_file:
            if not previous_line:
                previous_line = parameters
            else:
                if previous_line[3] == parameters[2]:
                    previous_line = parameters
                else:
                    s = "By_Truck \t(" + str(previous_line[1]) + ", " + str(previous_line[3]) + \
                        ", " + str(parameters[0]) + ", " + str(parameters[2]) + ")\n"
                    Array.append(s)
                    previous_line = parameters

    for line in Array:
        my_output_stream.write(line)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We collect the input values
    file_name = "sort_1.txt"

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        file_name = sys.argv[1]

    # 2. Local or Hadoop
    local_False_hadoop_True = False

    # 3. We set the path to my_dataset and my_result
    my_input_stream = sys.stdin
    my_output_stream = sys.stdout

    if (local_False_hadoop_True == False):
        my_input_stream = "../../my_results/A01_Part6/2_my_sort_simulation/" + file_name
        my_output_stream = "../../my_results/A01_Part6/3_my_reduce_simulation/reduce_" + file_name[5:]

    # 4. my_reducer.py input parameters
    my_reducer_input_parameters = []

    # 5. We call to my_main
    my_map(my_input_stream, my_output_stream, my_reducer_input_parameters)
