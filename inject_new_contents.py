import fileinput
import re
import os


def update_config(regex_pattern, new_string):
    filename = 'config.yml'
    with fileinput.FileInput(filename, inplace=True, backup='.bak') as file:
        for line in file:
            print(re.sub(regex_pattern, new_string, line), end='')

    return True


update_config('vCPU:\s\d+$', 'vCPU: 96')


def push_to_git(files, commit_message):
    if type(files) == list:
        os.system('git add ' + ' '.join(files))
    elif type(files) == str:
        os.system('git add ' + files)
    os.system('git commit -m "' + commit_message + '"')
    os.system('git push')
    return True


push_to_git(['./config.yml', './inject_new_contents.py'], 'testing code for committing & pushing to GIT')
