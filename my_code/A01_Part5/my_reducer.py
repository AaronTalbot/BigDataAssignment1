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



    # 1.2. We ouptut the bike_id from the line
    bike_id = None

    # 1.3. We ouptut the total_time from the line
    total_time = None

    # 1.4. We output the total_trips from the line
    total_trips = None

    # 2. We assign the variables
    content = line.strip().split("\t")
    aux_content = content[1].split(", ")

    bike_id = aux_content[0][1:]
    total_time = int(aux_content[1])
    total_trips = int(aux_content[2][:-1])


    # 3. We assign res
    res = (bike_id, total_time, total_trips)

    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    time = {}
    trips = {}
    print(my_reducer_input_parameters)
    for line in my_input_stream:
        parameters = process_line(line)
        if parameters[0] not in time:
            time[parameters[0]] = parameters[1]
            trips[parameters[0]] = parameters[2]
        else:
            time[parameters[0]] += parameters[1]
            trips[parameters[0]] += parameters[2]

    value_list = list(time.values())
    New_List = value_list.copy()
    # print(New_List)
    Keys_list = list(time.keys())
    New_List.sort(reverse=True)

    for index,value in enumerate(New_List):
        if index == my_reducer_input_parameters[0]:
            break
        else:
            position = value_list.index(value)
            Key = Keys_list[position]
            trip_time = time[Key]
            total_trips = trips[Key]
            s = str(Key) + "\t(" + str(trip_time) + "," + str(total_trips) + ")\n"
            my_output_stream.write(s)


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
    top_n_bikes = 10

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        file_name = sys.argv[1]

    # 2. Local or Hadoop
    local_False_hadoop_True = False

    # 3. We set the path to my_dataset and my_result
    my_input_stream = sys.stdin
    my_output_stream = sys.stdout

    if (local_False_hadoop_True == False):
        my_input_stream = "../../my_results/A01_Part5/2_my_sort_simulation/" + file_name
        my_output_stream = "../../my_results/A01_Part5/3_my_reduce_simulation/reduce_" + file_name[5:]

    # 4. my_reducer.py input parameters
    my_reducer_input_parameters = []
    my_reducer_input_parameters.append( top_n_bikes )

    # 5. We call to my_main
    my_map(my_input_stream, my_output_stream, my_reducer_input_parameters)
