import os
import random
import string
from datetime import datetime, timedelta

base_paths = [
    r"\\192.168.16.14\data1",
    r"\\192.168.16.14\data2"
]

file_extensions = ['.docx', '.png', '.txt', '.pdf', '.csv', '.json', '.jpg']

def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def simulate_file_usage(file_path, usage_count):
    now = datetime.now()
    for _ in range(usage_count):
        access_time = now - timedelta(days=random.randint(0, 365))
        os.utime(file_path, (access_time.timestamp(), access_time.timestamp()))

def generate_files_and_folders(base_path, num_folders=5, num_files=20, max_depth=3):
    for _ in range(num_folders):
        folder_name = random_string()
        folder_path = os.path.join(base_path, folder_name)
        os.makedirs(folder_path, exist_ok=True)

        for _ in range(num_files):
            file_name = f"{random_string()}{random.choice(file_extensions)}"
            file_path = os.path.join(folder_path, file_name)

            content_size = random.randint(1, 1024 * 1024) 
            with open(file_path, 'wb') as f:
                f.write(os.urandom(content_size))

            usage_count = random.randint(1, 10)
            simulate_file_usage(file_path, usage_count)

        if max_depth > 1:
            generate_files_and_folders(folder_path, num_folders // 2, num_files // 2, max_depth - 1)

def main():
    for base_path in base_paths:
        if not os.path.exists(base_path):
            print(f"Path does not exist: {base_path}. Skipping...")
            continue

        generate_files_and_folders(base_path, num_folders=10, num_files=50, max_depth=3)
        print(f"Files and folders generated in: {base_path}")

if __name__ == "__main__":
    main()
