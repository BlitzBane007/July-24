import os
import shutil

def delete_files_in_subfolders(path):
    if os.path.exists(path) and os.path.isdir(path):
        for root, dirs, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                except OSError as e:
                    print(f"Error: {e.strerror} - {e.filename}")
    else:
        print("The provided path does not exist or is not a directory.")

# Example usage:
# Replace 'your_directory_path' with the actual directory path.
directory = input("Enter directory: ")
delete_files_in_subfolders(directory)
