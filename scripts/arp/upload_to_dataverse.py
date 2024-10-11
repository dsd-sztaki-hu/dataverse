# pip install dvuploader
# python upload_to_dataverse.py -d ./output_dir -u https://demo.dataverse.org/ -a YOUR_API_TOKEN -p doi:10.70122/XXX/XXXXX
import dvuploader as dv
import os
import argparse
import re

def natural_sort_key(s):
    """
    Key function for natural sorting.
    Splits the string into a list of integers and non-integer substrings to sort naturally.
    """
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

def gather_files(directory, batch_size=None):
    """
    Recursively gathers all files from a directory and subdirectories.
    Adds the relative path as the directory_label for each file.
    Sorts directories and files naturally (i.e., numerically).
    Optionally splits them into batches.
    """
    files = []
    for root, dirs, filenames in os.walk(directory):
        # Sort directories and filenames using natural sort
        dirs.sort(key=natural_sort_key)
        filenames.sort(key=natural_sort_key)

        for filename in filenames:
            filepath = os.path.join(root, filename)
            # Get the relative directory structure
            relative_dir = os.path.relpath(root, directory)
            directory_label = relative_dir if relative_dir != '.' else None

            # Add the file to the list for uploading with the directory_label
            files.append(dv.File(filepath=filepath, directory_label=directory_label))

    if batch_size:
        return (files[i:i+batch_size] for i in range(0, len(files), batch_size))
    else:
        return [files]

def upload_batch(files, dataverse_url, api_token, pid, n_parallel_uploads=2):
    """
    Uploads a batch of files to a Dataverse instance.
    Includes debug logging to show what files will be uploaded.
    """
    print(f"\nUploading {len(files)} files in this batch...")

    # Debug: Show file paths being uploaded along with their directory labels
    for file in files:
        print(f"Preparing to upload: {file.filepath} (directory_label: {file.directory_label})")

    # Create the DVUploader instance
    dvuploader = dv.DVUploader(files=files)

    # Perform the upload
    dvuploader.upload(
        api_token=api_token,
        dataverse_url=dataverse_url,
        persistent_id=pid,
        n_parallel_uploads=n_parallel_uploads
    )

    print("Batch upload completed.\n")

def main():
    parser = argparse.ArgumentParser(description="Upload a generated dataset to a Dataverse instance.")

    parser.add_argument('-d', '--directory', type=str, required=True, help='Path to the dataset directory.')
    parser.add_argument('-u', '--dataverse_url', type=str, required=True, help='Dataverse URL.')
    parser.add_argument('-a', '--api_token', type=str, required=True, help='API token for Dataverse.')
    parser.add_argument('-p', '--pid', type=str, required=True, help='Persistent identifier (PID) of the dataset in Dataverse.')
    parser.add_argument('-n', '--n_parallel_uploads', type=int, default=2, help='Number of parallel uploads (default: 2).')
    parser.add_argument('-b', '--batch_size', type=int, default=1000, help='Number of files to upload per batch (default: 1000).')

    args = parser.parse_args()

    batches = gather_files(args.directory, batch_size=args.batch_size)

    for batch in batches:
        upload_batch(batch, args.dataverse_url, args.api_token, args.pid, args.n_parallel_uploads)

if __name__ == "__main__":
    main()
