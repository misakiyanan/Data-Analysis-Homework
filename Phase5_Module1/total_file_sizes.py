# ref. https://thispointer.com/python-how-to-get-list-of-files-in-directory-and-sub-directories/

import os

def get_list_of_files(dirname):
    list_of_file = os.listdir(dirname)  # create a list of file and sub directories
    all_files = list()

    # iterate over all the entries
    for entry in list_of_file:
        # create full path
        full_path = os.path.join(dirname, entry)

        # if entry is a directory then get the list of files in this directory
        if os.path.isdir(full_path):
            all_files = all_files + get_list_of_files(full_path)
        else:
            all_files.append(full_path)

    return all_files


def cal_total_file_sizes(dirname):
    total_size = 0
    all_files = get_list_of_files(dirname)
    for f in all_files:
        total_size += os.path.getsize(f)
    return total_size


if __name__ == '__main__':
    my_path = '/Users/xieyanan/Desktop/2021sem1/QBUS6840/'
    for file in get_list_of_files(my_path):
        print(file)
    print(f"Total file size is {cal_total_file_sizes(my_path)} B.")

