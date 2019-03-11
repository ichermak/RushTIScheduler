import datetime
import logging
import os
import shlex
import sys

def set_current_directory():
        abspath = os.path.abspath(__file__)
        directory = os.path.dirname(abspath)
        # set current directory
        os.chdir(directory)
        return directory

APP_NAME = "RushTIScheduler"
CURRENT_DIRECTORY = set_current_directory()
LOGFILE = os.path.join(CURRENT_DIRECTORY, APP_NAME + ".log")
# CONFIG = os.path.join(CURRENT_DIRECTORY, "config.ini")

MSG_RUSHTISCHEDULER_STARTS = "{app_name} starts. Parameters: {parameters}."
MSG_RUSHTISCHEDULER_TOO_FEW_ARGUMENTS = "{app_name} needs to be executed with 3 arguments."
MSG_RUSHTISCHEDULER_ARGUMENT1_INVALID = "Argument 1 (path to file) invalid. File needs to exist."
MSG_RUSHTISCHEDULER_ARGUMENT2_INVALID = "Argument 2 (max workers) invalid. Argument needs to be an integer number."
MSG_RUSHTISCHEDULER_ARGUMENT3_INVALID = "Argument 3 (repository for output file) invalid. Repository needs to exist."
MSG_RUSHTISCHEDULER_ENDS = "{app_name} ends. {fails} fails out of {executions} executions. Elapsed time: {time}"

logging.basicConfig(
        filename=LOGFILE,
        format='%(asctime)s - ' + APP_NAME + ' - %(levelname)s - %(message)s',
        level=logging.INFO)

def extract_info_from_line(line):
        """ Translate one line from txt file into arguments for execution

        :param: line: Arguments for execution. E.g. id="5" predecessors="2,3" instance="tm1srv01" process="Bedrock.Server.Wait" pWaitSec=5
        :return: attributes
        """
        attributes = {}
        temp = []
        for pair in shlex.split(line):
                attribute, value = pair.split("=")

                # if instance or process, needs to be case insensitive
                if attribute.lower() == 'process' or attribute.lower() == 'instance':
                        attributes[attribute.lower()] = value.strip('"').strip()
                
                # Convert string attribute value into list
                elif attribute.lower() == 'predecessors':
                        temp = value.strip('"').strip().split(',')
                        if temp[0] == '':
                                attributes[attribute] = []
                        else:
                                attributes[attribute] = temp

                # attributes (e.g. pWaitSec) are case sensitive in TM1 REST API !
                else:
                        attributes[attribute] = value.strip('"').strip()

        return attributes

def extract_info_from_file(file_path):
        """ Read a file that respect specification for RushTIScheduler and transform it into dictionary named tasks

        :param: file_path:
	:return: tasks
	"""
        tasks = {}
        task_attributes = {}
        
        with open(file_path) as input_file:
                lines = input_file.readlines()
                
                # Build tasks dictionnay
                for line in lines:
                        task_attributes = extract_info_from_line(line)
                        task_attributes["successors"] = []
                        tasks[task_attributes["id"]] = task_attributes
                
                # Deduct the successors attribut and add it to the task_attributes
                for task in tasks.values():
                        predecessors = task["predecessors"]
                        if len(predecessors) != 0:
                                for predecessor in predecessors:
                                        successor =  task["id"]
                                        task_attributes = tasks[predecessor]
                                        task_attributes["successors"].append(successor)

        return tasks

def deduce_levels_of_tasks(**tasks):
        """ ...

        :param: tasks:
	:return: levels
	"""
        levels = {}
        task_attributes = {}
        
        # level 0 contains all tasks without predecessors
        level = 0
        levels[level] = []
        for task in tasks.values():
                predecessors = task["predecessors"]
                if len(predecessors) == 0:
                        levels[level].append(task["id"])

        # Handek other levels
        level = 0
        for task in tasks: 
                level_tasks = levels[level]
                next_level_created = False
                for level_task in level_tasks:
                        task_attributes = tasks[level_task]
                        successors = task_attributes["successors"]

                        # Create next level if necessary and add successors to this new level
                        if len(successors) != 0:
                                if not(next_level_created):
                                        precedent_level = level
                                        level += 1
                                        levels[level] = []
                                        next_level_created = True 
                                for successor in successors:

                                        # test if task exists in current level
                                        if not(successor in levels[level]):
                                                levels[level].append(successor)
                                        
                                        # Delet successor in precedent level
                                        if successor in levels[precedent_level]:
                                                levels[precedent_level].remove(successor)

        return levels

def rearrange_tasks_in_levels(maximum_workers, **tasks):
        """ ...

        :param: tasks:
	:return: levels
	"""
        levels = deduce_levels_of_tasks(**tasks)
        levels_count = len(levels.items())

        for task_key in tasks.keys():
                index = 0
                while index < levels_count:
                        level_key = index 
                        level = levels[level_key]
                        if level_key + 1 < levels_count:
                                next_level = levels[level_key + 1]
                                if len(next_level) < maximum_workers:
                                        for task in level:
                                                successors = tasks[task]["successors"]
                                                next_level_contains_successor = False
                                                for successor in successors:
                                                        if next_level.count(successor) != 0:
                                                                next_level_contains_successor = True
                                                if not(next_level_contains_successor):
                                                        # move task from level to next_level
                                                        levels[level_key].remove(task)
                                                        levels[level_key + 1].append(task)

                        index += 1
        
        return levels

def output_to_rushti_task_file(input_file_path, maximum_workers, output_file_path):
        """ Transform a file that respect specification for RushTIScheduler into a scheduled file for RushTI

        :param: input_file_path:
	:param: maximum_workers:
	:param: output_file_path:
	:return: True
	"""
        tasks = {}
        levels ={}
        tasks = extract_info_from_file(input_file_path)
        # levels = deduce_levels_of_tasks(**tasks)
        levels = rearrange_tasks_in_levels(maximum_workers, **tasks)
        levels_count = len(levels.items())

        # Delete former file if exists
        if os.path.isfile(output_file_path): 
                os.remove(output_file_path)
        
        with open(output_file_path, "a") as output_file:
                index = 0 
                for level in levels.values():
                        index += 1
                        for task in level:
                                line =''
                                task_attributes = tasks[task]
                                for attribute_key, attribute_value in task_attributes.items():
                                        if attribute_key.lower() != 'id' and attribute_key.lower() != 'predecessors' and attribute_key.lower() != 'successors':
                                                line = line + attribute_key + '=' + '"' + attribute_value + '"' + ' ' 
                                
                                output_file.write(line + '\n')
                        if index < levels_count: 
                                output_file.write('wait\n')
        
        return levels_count

def file_path_exists(file_path):
        """ ...

        :param: file_path:
	:return: boolean
	"""
        exists = False
        folders = file_path.split('\\')
        path =''
        index = 0
        while index < (len(folders) - 1): 
                if path == '':
                        path = folders[index]
                else:
                        path = path + '/' + folders[index]
                index +=1
        if os.path.exists(path):       
                exists = True
        return exists

def translate_cmd_arguments(*args):
        """ Translation and Validity-checks for command line arguments.
        
        :param args: 
        :return: input_file_path, maximum_workers and output_file_path
        """
        # three few arguments
        if len(args) < 4:
                msg = MSG_RUSHTISCHEDULER_TOO_FEW_ARGUMENTS.format(app_name=APP_NAME)
                logging.error(msg)
                sys.exit(msg)

        # txt file doesnt exist
        if not os.path.isfile(args[1]):
                msg = MSG_RUSHTISCHEDULER_ARGUMENT1_INVALID
                logging.error(msg)
                sys.exit(msg)

        # max_workers is not a number
        if not args[2].isdigit():
                msg = MSG_RUSHTISCHEDULER_ARGUMENT2_INVALID
                logging.error(msg)
                sys.exit(msg)

        # txt repository for file doesnt exist
        if not file_path_exists(args[3]):
                msg = MSG_RUSHTISCHEDULER_ARGUMENT3_INVALID
                logging.error(msg)
                sys.exit(msg)

        return args[1], args[2], args[3] 
	
def exit_rushtischeduler(executions, successes, elapsed_time):
        """ Exit RushTI with exit code 0 or 1 depending on the TI execution outcomes

        :param executions: Number of executions
        :param successes: Number of executions that succeeded
        :param elapsed_time:
        :return:
        """
        fails = executions - successes
        message = MSG_RUSHTISCHEDULER_ENDS.format(
                app_name=APP_NAME,
                fails=fails,
                executions=executions,
                time=str(elapsed_time))
        if fails > 0:
                logging.error(message)
                sys.exit(message)
        else:
                logging.info(message)
                sys.exit(0)

# receives three arguments: 1) path-to-input-txt-file, 2) max-workers, 3) path-to-output-txt-file
if __name__ == "__main__":
        logging.info(MSG_RUSHTISCHEDULER_STARTS.format(
                app_name=APP_NAME,
                parameters=sys.argv))

        # start timer
        start = datetime.datetime.now()

        # read commandline arguments
        input_file_path, maximum_workers, output_file_path = translate_cmd_arguments(*sys.argv)

        # execution
        levels_count = output_to_rushti_task_file(input_file_path, int(maximum_workers), output_file_path)

        # timing
        duration = datetime.datetime.now() - start
        exit_rushtischeduler(executions=1, successes=1, elapsed_time=duration)