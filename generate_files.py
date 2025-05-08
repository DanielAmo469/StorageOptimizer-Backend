import os
import random
import string
import subprocess
from datetime import datetime, timedelta

# Target volumes and size limits
volume_targets = {
    r"\\192.168.16.14\data1": 1.7 * 1024 ** 3,  # 1.7 GB
    r"\\192.168.16.14\data2": 0.8 * 1024 ** 3   # 0.8 GB
}

file_extensions = ['.docx', '.png', '.txt', '.pdf', '.csv', '.json', '.jpg']

def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def set_file_timestamps(file_path, creation_time, access_time, modified_time):
    os.utime(file_path, (access_time.timestamp(), modified_time.timestamp()))

    creation_str = creation_time.strftime("%m/%d/%Y %H:%M:%S")
    powershell_command = f"""$(Get-Item '{file_path}').CreationTime=('{creation_str}')"""
    subprocess.call(["powershell", "-Command", powershell_command], shell=True)

def get_folder_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            try:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
            except:
                continue
    return total_size

def generate_files_and_folders(base_path, size_limit, num_folders=5, num_files=20, max_depth=3):
    def _generate(path, depth):
        nonlocal generated_size
        for _ in range(num_folders):
            folder_name = random_string()
            folder_path = os.path.join(path, folder_name)
            os.makedirs(folder_path, exist_ok=True)

            for _ in range(num_files):
                if generated_size >= size_limit:
                    return

                file_name = f"{random_string()}{random.choice(file_extensions)}"
                file_path = os.path.join(folder_path, file_name)
                content_size = random.randint(10 * 1024, 1024 * 1024)  # 10KB–1MB

                with open(file_path, 'wb') as f:
                    f.write(os.urandom(content_size))

                now = datetime.now()
                creation_time = now - timedelta(days=random.randint(30, 730))
                access_time = creation_time + timedelta(days=random.randint(0, 60))
                modified_time = access_time + timedelta(days=random.randint(0, 30))
                set_file_timestamps(file_path, creation_time, access_time, modified_time)

                generated_size += content_size

            if depth > 1:
                _generate(folder_path, depth - 1)

    generated_size = get_folder_size(base_path)
    _generate(base_path, max_depth)

def main():
    for base_path, size_limit in volume_targets.items():
        if not os.path.exists(base_path):
            print(f"Path does not exist: {base_path}. Skipping...")
            continue

        print(f"Generating files under: {base_path} (limit: {round(size_limit / (1024**2), 1)} MB)")
        generate_files_and_folders(base_path, size_limit, num_folders=10, num_files=50, max_depth=3)
        print(f"Finished generating files for {base_path}")

if __name__ == "__main__":
    main()
