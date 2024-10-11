# pip install lorem-text
# python generate_dataset.py -l 1 -d 100 -f 1000 -n 50 -a -o ./100x1000_dataset
import os
import argparse
from lorem_text import lorem

# Function to generate random lorem ipsum text using the correct lorem-text API
def generate_lorem_ipsum():
    return lorem.paragraph()  # Corrected to lorem_text.lorem.paragraph()

# Recursive function to create directory structure and files
def create_structure(current_level, dir_prefix, dir_levels, num_dirs, num_files_leaf, num_files_nonleaf, add_files_in_nonleaf):
    for i in range(1, num_dirs + 1):
        new_dir = os.path.join(dir_prefix, f"dir_{current_level}_{i}")
        os.makedirs(new_dir, exist_ok=True)

        # Add files in non-leaf directories if requested
        if add_files_in_nonleaf and current_level < dir_levels:
            for j in range(1, num_files_nonleaf + 1):
                file_name = os.path.join(new_dir, f"file_{current_level}_{i}_{j}.txt")
                with open(file_name, 'w') as f:
                    f.write(generate_lorem_ipsum())

        # Recur into the next level if not the last level
        if current_level < dir_levels:
            create_structure(current_level + 1, new_dir, dir_levels, num_dirs, num_files_leaf, num_files_nonleaf, add_files_in_nonleaf)
        else:
            # If it's a leaf directory, add files
            for k in range(1, num_files_leaf + 1):
                file_name = os.path.join(new_dir, f"file_{current_level}_{i}_{k}.txt")
                with open(file_name, 'w') as f:
                    f.write(generate_lorem_ipsum())

# Main function to handle arguments and initiate the creation process
def main(args):
    create_structure(
        1, args.output_dir, args.levels, args.num_dirs, args.num_files_leaf,
        args.num_files_nonleaf, args.add_files_in_nonleaf
    )
    print("Dataset generation completed!")

if __name__ == "__main__":
    # Argument parser setup
    parser = argparse.ArgumentParser(description="Generate artificial datasets of directories and files.")

    parser.add_argument("-l", "--levels", type=int, required=True, help="Number of directory levels.")
    parser.add_argument("-d", "--num_dirs", type=int, required=True, help="Number of directories per level.")
    parser.add_argument("-f", "--num_files_leaf", type=int, required=True, help="Number of files in each leaf directory.")
    parser.add_argument("-n", "--num_files_nonleaf", type=int, default=1, help="Number of files in non-leaf directories.")
    parser.add_argument("-a", "--add_files_in_nonleaf", action="store_true", help="Add files to non-leaf directories.")
    parser.add_argument("-o", "--output_dir", type=str, default="dataset", help="Output directory for dataset (default: 'dataset').")

    args = parser.parse_args()

    main(args)
