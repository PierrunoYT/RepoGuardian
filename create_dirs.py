import os

# Directories to create
dirs = ['src', 'config', 'tests']

# Create directories
for dir_path in dirs:
    os.makedirs(dir_path, exist_ok=True)
    print(f"Created directory: {dir_path}")
