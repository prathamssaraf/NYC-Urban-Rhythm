#!/usr/bin/env python3
"""
File Collector Script

This script recursively searches through directories starting from the current directory,
finds all files (excluding specified binary/media formats), and collects their content into 
a single text file with file paths and separators for better readability.
"""

import os
import argparse
from datetime import datetime


def collect_code(root_dir='.', output_file='code_collection.txt', exclude_extensions=None, ignore_dirs=None):
    """
    Recursively collect all files into a single text file.
    
    Args:
        root_dir (str): Root directory to start the search from
        output_file (str): Name of the output file
        exclude_extensions (list): List of file extensions to exclude (e.g., ['.pyc', '.exe'])
        ignore_dirs (list): List of directory names to ignore (e.g., ['node_modules', '.git'])
    """
    if exclude_extensions is None:
        # Default extensions to exclude - typically binary or temporary files
        exclude_extensions = [
            '.pyc', '.pyo', '.pyd', '.exe', '.dll', '.so', '.o', '.obj',
            '.class', '.jar', '.war', '.ear', '.zip', '.tar', '.gz', '.7z',
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.ico', '.svg', '.mp3',
            '.mp4', '.avi', '.mov', '.flv', '.wmv', '.pdf', '.doc', '.docx',
            '.xls', '.xlsx', '.ppt', '.pptx'
        ]
    
    if ignore_dirs is None:
        # Default directories to ignore - add or remove based on your needs
        ignore_dirs = ['.git', 'node_modules', 'venv', '__pycache__', 'dist', 'build']
    
    # Convert exclude_extensions to lowercase for case-insensitive matching
    exclude_extensions = [ext.lower() for ext in exclude_extensions]
    
    # Get absolute path of root directory
    root_dir = os.path.abspath(root_dir)
    
    print(f"Starting code collection from {root_dir}")
    print(f"Excluding extensions: {', '.join(exclude_extensions)}")
    print(f"Ignoring directories: {', '.join(ignore_dirs)}")
    
    # Total counters
    total_files = 0
    total_lines = 0
    
    with open(output_file, 'w', encoding='utf-8') as outfile:
        # Write header
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        outfile.write(f"CODE COLLECTION GENERATED ON {timestamp}\n")
        outfile.write(f"Root directory: {root_dir}\n")
        outfile.write("=" * 80 + "\n\n")
        
        # Walk through directory tree
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Remove ignored directories from dirnames to prevent walking into them
            # Using slice assignment to modify dirnames in-place (required by os.walk)
            dirnames[:] = [d for d in dirnames if d not in ignore_dirs]
            
            for filename in filenames:
                # Check if the file does not have one of the excluded extensions
                file_ext = os.path.splitext(filename)[1].lower()
                if file_ext not in exclude_extensions:
                    file_path = os.path.join(dirpath, filename)
                    rel_path = os.path.relpath(file_path, root_dir)
                    
                    try:
                        with open(file_path, 'r', encoding='utf-8') as infile:
                            content = infile.read()
                            
                            # Count lines
                            line_count = content.count('\n') + (0 if content.endswith('\n') else 1)
                            total_lines += line_count
                            total_files += 1
                            
                            # Write file info and content to output file
                            outfile.write(f"FILE: {rel_path}\n")
                            outfile.write(f"LINES: {line_count}\n")
                            outfile.write("-" * 80 + "\n\n")
                            outfile.write(content)
                            outfile.write("\n\n")
                            outfile.write("=" * 80 + "\n\n")
                            
                            print(f"Added: {rel_path} ({line_count} lines)")
                    except Exception as e:
                        print(f"Error reading {rel_path}: {e}")
    
    # Write summary at the end of the file
    with open(output_file, 'a', encoding='utf-8') as outfile:
        outfile.write(f"\nSUMMARY\n")
        outfile.write(f"Total files processed: {total_files}\n")
        outfile.write(f"Total lines of code: {total_lines}\n")
    
    print(f"\nCollection complete!")
    print(f"Processed {total_files} files with {total_lines} lines of code")
    print(f"Output saved to: {os.path.abspath(output_file)}")


def main():
    """Parse command line arguments and run the script."""
    parser = argparse.ArgumentParser(description='Collect all files into a single text file.')
    parser.add_argument('-d', '--directory', default='.', help='Root directory to start from (default: current directory)')
    parser.add_argument('-o', '--output', default='file_collection.txt', help='Output file name (default: file_collection.txt)')
    parser.add_argument('-e', '--exclude', nargs='+', help='File extensions to exclude (e.g., .exe .jpg)')
    parser.add_argument('-i', '--ignore', nargs='+', help='Directories to ignore (e.g., node_modules .git)')
    
    args = parser.parse_args()
    
    # Convert exclude extensions from command line to proper format with leading dot
    exclude_extensions = None
    if args.exclude:
        exclude_extensions = [ext if ext.startswith('.') else f'.{ext}' for ext in args.exclude]
    
    collect_code(
        root_dir=args.directory,
        output_file=args.output,
        exclude_extensions=exclude_extensions,
        ignore_dirs=args.ignore
    )


if __name__ == "__main__":
    main()